package controller

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// retainChartConnections replaces bootstrap-only NODE_IPS with the admitted
// native peer inventory. A rollout must stay on the retained storage nodes;
// rescheduling onto a retired node could reopen its removed RAFT database.
func retainChartConnections(object *unstructured.Unstructured, retained health.CentralTopology) error {
	containers, found, err := unstructured.NestedSlice(object.Object, "spec", "template", "spec", "containers")
	if err != nil || !found {
		return err
	}
	changed := false
	for _, value := range containers {
		container, ok := value.(map[string]any)
		if !ok || container["name"] != "ovn-central" {
			continue
		}
		env, ok := container["env"].([]any)
		if !ok {
			continue
		}
		for _, value := range env {
			entry, ok := value.(map[string]any)
			if !ok || entry["name"] != "NODE_IPS" {
				continue
			}
			enableSSL := false
			for _, candidate := range env {
				if option, ok := candidate.(map[string]any); ok && option["name"] == "ENABLE_SSL" {
					enableSSL = option["value"] == "true"
				}
			}
			if len(retained.Members) > 0 && enableSSL != strings.HasPrefix(retained.Members[0].Databases[0].ClientAddress, "ssl:") {
				return errors.New("retained central client protocol differs from Chart ENABLE_SSL")
			}
			addresses, err := retainedChartAddresses(retained)
			if err != nil {
				return err
			}
			independent := slices.ContainsFunc(retained.Members, func(member health.CentralIdentity) bool {
				return member.Workload.Kind != "Deployment" || member.Workload.Name != object.GetName()
			})
			if !independent && entry["value"] == strings.Join(addresses, ",") {
				continue
			}
			delete(entry, "valueFrom")
			entry["value"] = strings.Join(addresses, ",")
			changed = true
		}
	}
	if !changed {
		return nil
	}
	var nodes []string
	for _, member := range retained.Members {
		if member.Workload.Kind == "Deployment" && member.Workload.Name == object.GetName() {
			if member.Node.Name == "" {
				return errors.New("retained central storage node is missing")
			}
			nodes = append(nodes, member.Node.Name)
		}
	}
	slices.Sort(nodes)
	nodes = slices.Compact(nodes)
	if len(nodes) == 0 {
		return errors.New("retained central Chart nodes are missing")
	}
	path := []string{"spec", "template", "spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms"}
	terms, found, err := unstructured.NestedSlice(object.Object, path...)
	if err != nil {
		return err
	}
	if !found {
		for _, node := range nodes {
			terms = append(terms, map[string]any{"matchFields": []any{map[string]any{"key": "metadata.name", "operator": "In", "values": []any{node}}}})
		}
	}
	terms, err = retainedChartNodeTerms(terms, nodes)
	if err != nil {
		return err
	}
	if err := unstructured.SetNestedSlice(object.Object, terms, path...); err != nil {
		return err
	}
	if err := unstructured.SetNestedSlice(object.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		return err
	}
	return unstructured.SetNestedMap(object.Object, map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": int64(0), "maxUnavailable": int64(1)}}, "spec", "strategy")
}

func retainedChartAddresses(retained health.CentralTopology) ([]string, error) {
	var addresses []string
	protocol := ""
	for _, member := range retained.Members {
		address := ""
		for i, database := range member.Databases {
			transport, endpoint, found := strings.Cut(database.ClientAddress, ":")
			host, port, err := net.SplitHostPort(endpoint)
			ip, ipErr := netip.ParseAddr(host)
			if !found || err != nil || ipErr != nil || transport != "tcp" && transport != "ssl" || port != strconv.Itoa(6641+i) {
				return nil, fmt.Errorf("retained central client address %q cannot be represented by Chart NODE_IPS", database.ClientAddress)
			}
			if protocol != "" && protocol != transport {
				return nil, errors.New("retained central client protocols differ")
			}
			protocol = transport
			if address != "" && address != ip.String() {
				return nil, errors.New("chart NODE_IPS requires NB/SB listeners on the same node address")
			}
			address = ip.String()
		}
		addresses = append(addresses, address)
	}
	slices.Sort(addresses)
	addresses = slices.Compact(addresses)
	if len(addresses) != len(retained.Members) {
		return nil, errors.New("chart central peers require distinct retained node addresses")
	}
	return addresses, nil
}

// Field selectors support only one value. Preserve each original AND term,
// intersect its field restrictions with the retained nodes, and emit one OR
// term for each remaining node. Reapplying the transformation is idempotent.
func retainedChartNodeTerms(terms []any, nodes []string) ([]any, error) {
	var result []any
	for _, value := range terms {
		term, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("central node affinity term is invalid")
		}
		expressions, _ := term["matchExpressions"].([]any)
		originalFields, _ := term["matchFields"].([]any)
		if len(expressions) == 0 && len(originalFields) == 0 {
			continue
		}
		for _, node := range nodes {
			copy := runtime.DeepCopyJSON(term)
			fields, _ := copy["matchFields"].([]any)
			matches := true
			for _, value := range fields {
				field, ok := value.(map[string]any)
				if !ok {
					return nil, errors.New("central node field selector is invalid")
				}
				if field["key"] != "metadata.name" {
					continue
				}
				values, ok := field["values"].([]any)
				if !ok || len(values) != 1 {
					return nil, errors.New("central node field selector requires exactly one value")
				}
				switch field["operator"] {
				case "In":
					matches = matches && values[0] == node
				case "NotIn":
					matches = matches && values[0] != node
				default:
					return nil, errors.New("central node field selector requires In or NotIn")
				}
			}
			if !matches {
				continue
			}
			constraint := map[string]any{"key": "metadata.name", "operator": "In", "values": []any{node}}
			if !slices.ContainsFunc(fields, func(field any) bool { return reflect.DeepEqual(field, constraint) }) {
				copy["matchFields"] = append(fields, constraint)
			}
			if !slices.ContainsFunc(result, func(term any) bool { return reflect.DeepEqual(term, copy) }) {
				result = append(result, copy)
			}
		}
	}
	if len(result) == 0 {
		return nil, errors.New("retained central storage nodes conflict with existing node affinity")
	}
	return result, nil
}
