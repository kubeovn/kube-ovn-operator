package ovnadmin

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

// ClusterStatus is one server's unixctl observation. Member IDs in the Servers
// section are abbreviated by OVS and must be resolved against pinned full IDs.
type ClusterStatus struct {
	Database, ClusterID, ServerID, Address string
	State, Role, Leader                    string
	Term, CommitIndex, AppliedIndex        int64
	Servers                                []ClusterServer
	Connections                            map[string]bool
}

type ClusterServer struct {
	Nickname, ShortID, Address string
	Self                       bool
}

var (
	statusID     = regexp.MustCompile(`^[0-9a-f]{4} \(([0-9a-f-]{36})\)$`)
	statusLog    = regexp.MustCompile(`^\[([0-9]+), ([0-9]+)\]$`)
	statusServer = regexp.MustCompile(`^([^\s()]+) \(([0-9a-f]{4}) at ([^\s()]+)\)(.*)$`)
)

func ParseClusterStatus(data []byte, database string) (ClusterStatus, error) {
	var result ClusterStatus
	if len(data) == 0 || len(data) > 64*1024 {
		return result, fmt.Errorf("invalid cluster status size")
	}
	fields := map[string]string{}
	servers := false
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "Servers:" {
			if servers {
				return result, fmt.Errorf("duplicate Servers section")
			}
			servers = true
			continue
		}
		if servers {
			match := statusServer.FindStringSubmatch(line)
			if match == nil {
				return result, fmt.Errorf("invalid member status")
			}
			tail := match[4]
			self := strings.Contains(tail, " (self)")
			// Other parenthesized annotations describe votes or an in-flight
			// membership transition. Neither is a stable mutation precondition.
			if strings.ContainsAny(strings.ReplaceAll(tail, " (self)", ""), "()") {
				return result, fmt.Errorf("member is not in a stable phase")
			}
			result.Servers = append(result.Servers, ClusterServer{Nickname: match[1], ShortID: match[2], Address: match[3], Self: self})
			continue
		}
		if strings.HasPrefix(line, "Adding server ") || strings.HasPrefix(line, "Removing server ") {
			return result, fmt.Errorf("membership change is already in progress")
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue // The first line is the local nickname.
		}
		if _, duplicate := fields[key]; duplicate {
			return result, fmt.Errorf("duplicate cluster status field")
		}
		fields[key] = strings.TrimSpace(value)
	}
	for _, key := range []string{"Name", "Cluster ID", "Server ID", "Address", "Status", "Role", "Term", "Leader", "Log", "Entries not yet committed", "Entries not yet applied", "Connections"} {
		if _, exists := fields[key]; !exists {
			return result, fmt.Errorf("missing cluster status field %s", key)
		}
	}
	if fields["Name"] != database || (database != "OVN_Northbound" && database != "OVN_Southbound") {
		return result, fmt.Errorf("unexpected database in cluster status")
	}
	result.Database, result.Address = database, fields["Address"]
	for key, destination := range map[string]*string{"Cluster ID": &result.ClusterID, "Server ID": &result.ServerID} {
		match := statusID.FindStringSubmatch(fields[key])
		if match == nil || !membership.ValidID(match[1]) || fields[key][:4] != match[1][:4] {
			return result, fmt.Errorf("invalid full %s", key)
		}
		*destination = match[1]
	}
	if err := membership.ValidateMembers([]membership.Member{{ServerID: result.ServerID, Address: result.Address}}); err != nil {
		return result, err
	}
	result.State, result.Role, result.Leader = fields["Status"], fields["Role"], fields["Leader"]
	var err error
	result.Term, err = nonnegative(fields["Term"])
	if err != nil || result.Term == 0 {
		return result, fmt.Errorf("invalid RAFT term")
	}
	log := statusLog.FindStringSubmatch(fields["Log"])
	if log == nil {
		return result, fmt.Errorf("invalid RAFT log range")
	}
	start, startErr := nonnegative(log[1])
	end, endErr := nonnegative(log[2])
	uncommitted, commitErr := nonnegative(fields["Entries not yet committed"])
	unapplied, appliedErr := nonnegative(fields["Entries not yet applied"])
	if startErr != nil || endErr != nil || commitErr != nil || appliedErr != nil || start < 1 || end < start || uncommitted >= end || unapplied >= end || unapplied < uncommitted || uncommitted > end-start || unapplied > end-start {
		return result, fmt.Errorf("inconsistent RAFT commit/application counters")
	}
	// raft_unixctl_status uses log_end - commit_index - 1 and
	// log_end - last_applied - 1. The log boundary alone is not commit evidence.
	result.CommitIndex, result.AppliedIndex = end-1-uncommitted, end-1-unapplied
	result.Connections = map[string]bool{}
	for token := range strings.FieldsSeq(fields["Connections"]) {
		if strings.HasPrefix(token, "(") && strings.HasSuffix(token, ")") {
			continue // A parenthesized connection is disconnected.
		}
		if !strings.HasPrefix(token, "->") && !strings.HasPrefix(token, "<-") {
			return result, fmt.Errorf("invalid RAFT connection")
		}
		result.Connections[token[2:]] = true
	}
	if len(result.Servers) == 0 {
		return result, fmt.Errorf("cluster has no member configuration")
	}
	nicknames, addresses := map[string]bool{}, map[string]bool{}
	selfCount := 0
	for _, server := range result.Servers {
		if nicknames[server.Nickname] || addresses[server.Address] {
			return result, fmt.Errorf("duplicate nickname or member address")
		}
		nicknames[server.Nickname], addresses[server.Address] = true, true
		if server.Self {
			selfCount++
			if server.Address != result.Address || server.ShortID != result.ServerID[:4] {
				return result, fmt.Errorf("local member identity differs from member configuration")
			}
		}
	}
	if selfCount != 1 {
		return result, fmt.Errorf("cluster status must identify exactly one local member")
	}
	return result, nil
}

func nonnegative(value string) (int64, error) {
	number, err := strconv.ParseInt(value, 10, 64)
	if err != nil || number < 0 {
		return 0, fmt.Errorf("invalid nonnegative integer")
	}
	return number, nil
}
