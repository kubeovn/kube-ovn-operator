package health

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// clientAddress reads the server's effective listener configuration. A RAFT
// address is never treated as a client address or assigned a guessed port.
func (r *membershipRunner) clientAddress(ctx context.Context, pod string, index int, raft string) (string, error) {
	data, err := r.readTopologyCommand(ctx, pod, []string{"ovs-appctl", "--timeout=5", "-t", topologySocket(index), "ovsdb-server/list-remotes"})
	if err != nil {
		return "", err
	}
	if len(data) == 0 || len(data) > 64<<10 {
		return "", errors.New("invalid client remote inventory size")
	}
	var remotes []string
	database, global := topologyDatabases[index], []string{"NB_Global", "SB_Global"}[index]
	for remote := range strings.FieldsSeq(string(data)) {
		if strings.HasPrefix(remote, "db:") {
			if remote != "db:"+database+","+global+",connections" {
				return "", errors.New("unsupported database-backed client remote")
			}
			command := []string{"ovsdb-client", "--timeout=5", "query", strings.Replace(topologySocket(index), ".ctl", ".sock", 1), fmt.Sprintf(`[%q,{"op":"select","table":%q,"where":[],"columns":["connections"]},{"op":"select","table":"Connection","where":[],"columns":["_uuid","target"]}]`, database, global)}
			command[3] = "unix:" + command[3]
			response, err := r.readTopologyCommand(ctx, pod, command)
			if err != nil {
				return "", err
			}
			targets, err := connectionTargets(response)
			if err != nil {
				return "", err
			}
			remotes = append(remotes, targets...)
		} else {
			remotes = append(remotes, remote)
		}
	}
	_, endpoint, _ := strings.Cut(raft, ":")
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", err
	}
	var candidates []string
	for _, remote := range remotes {
		address, err := passiveClient(remote, host)
		if err != nil {
			return "", err
		}
		if address != "" && !slices.Contains(candidates, address) {
			candidates = append(candidates, address)
		}
	}
	if len(candidates) != 1 {
		return "", errors.New("central member requires one unambiguous client listener on its observed node address")
	}
	return candidates[0], nil
}

func (r *membershipRunner) readTopologyCommand(ctx context.Context, pod string, command []string) ([]byte, error) {
	if err := r.validate(ctx, pod); err != nil {
		return nil, err
	}
	data, err := r.runner.Run(ctx, r.installation.Namespace, pod, "ovn-central", command)
	if err != nil {
		return nil, err
	}
	if err := r.validate(ctx, pod); err != nil {
		return nil, err
	}
	return data, nil
}

// connectionTargets follows only UUIDs referenced by the singleton global row.
// Unreferenced Connection rows must not invent an active listener.
func connectionTargets(data []byte) ([]string, error) {
	var result []struct {
		Error string `json:"error"`
		Rows  []struct {
			Connections jsontext.Value `json:"connections"`
			UUID        jsontext.Value `json:"_uuid"`
			Target      string         `json:"target"`
		} `json:"rows"`
	}
	if len(data) > 64<<10 || json.Unmarshal(data, &result) != nil || len(result) != 2 || result[0].Error != "" || result[1].Error != "" || len(result[0].Rows) != 1 {
		return nil, errors.New("invalid client connection query result")
	}
	raw := result[0].Rows[0].Connections
	var tagged []jsontext.Value
	if json.Unmarshal(raw, &tagged) != nil || len(tagged) != 2 {
		return nil, errors.New("invalid client connection reference set")
	}
	var tag string
	if json.Unmarshal(tagged[0], &tag) != nil {
		return nil, errors.New("invalid client connection set tag")
	}
	var refs []jsontext.Value
	switch tag {
	case "uuid":
		refs = []jsontext.Value{raw}
	case "set":
		if json.Unmarshal(tagged[1], &refs) != nil {
			return nil, errors.New("invalid client connection set")
		}
	default:
		return nil, errors.New("invalid client connection reference")
	}
	targets := map[string]string{}
	for _, row := range result[1].Rows {
		id, err := optionalUUID(row.UUID)
		if err != nil || id == "" || row.Target == "" || targets[id] != "" {
			return nil, errors.New("invalid or duplicate client connection row")
		}
		targets[id] = row.Target
	}
	var remotes []string
	seen := map[string]bool{}
	for _, ref := range refs {
		id, err := optionalUUID(ref)
		if err != nil || id == "" || targets[id] == "" || seen[id] {
			return nil, errors.New("unresolved client connection reference")
		}
		seen[id] = true
		remotes = append(remotes, targets[id])
	}
	return remotes, nil
}

func passiveClient(remote, host string) (string, error) {
	protocol, rest, _ := strings.Cut(remote, ":")
	if protocol != "ptcp" && protocol != "pssl" {
		return "", nil
	}
	port, bind, _ := strings.Cut(rest, ":")
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 || strconv.FormatUint(number, 10) != port {
		return "", errors.New("invalid passive client port")
	}
	nodeIP, err := netip.ParseAddr(host)
	if err != nil {
		return "", err
	}
	if bind != "" {
		bind = strings.TrimSuffix(strings.TrimPrefix(bind, "["), "]")
		ip, err := netip.ParseAddr(bind)
		if err != nil || ip.Is4In6() || ip.Zone() != "" {
			return "", errors.New("invalid passive client bind address")
		}
		if ip != nodeIP && (!ip.IsUnspecified() || ip.Is4() != nodeIP.Is4()) {
			return "", nil
		}
	}
	return strings.TrimPrefix(protocol, "p") + ":" + net.JoinHostPort(host, port), nil
}
