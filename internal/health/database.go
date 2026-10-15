// Package health collects runtime evidence and binds access to owned OVN processes.
package health

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"uuid"
)

// Database is the server's own _Server.Database row. Index is the log index
// exposed to clients, not the end of the RAFT log or its last uncommitted entry.
// See ovsdb/_server.xml in Open vSwitch for the column semantics.
type Database struct {
	Name      string
	Model     string
	Connected bool
	Leader    bool
	ClusterID string
	ServerID  string
	Index     int64
}

// DatabaseCommand reads only the local OVSDB server's metadata. Neither the
// socket nor the transaction is supplied by an installation or shell string.
func DatabaseCommand(database string) ([]string, error) {
	var socket string
	switch database {
	case "OVN_Northbound":
		socket = "unix:/var/run/ovn/ovnnb_db.sock"
	case "OVN_Southbound":
		socket = "unix:/var/run/ovn/ovnsb_db.sock"
	default:
		return nil, fmt.Errorf("unsupported OVN database %q", database)
	}
	transaction := fmt.Sprintf(`["_Server",{"op":"select","table":"Database","where":[["name","==",%q]],"columns":["name","model","connected","leader","cid","sid","index"]}]`, database)
	return []string{"ovsdb-client", "--timeout=5", "query", socket, transaction}, nil
}

func ParseDatabase(data []byte, name string) (Database, error) {
	var results []struct {
		Error string `json:"error"`
		Rows  []struct {
			Name      string         `json:"name"`
			Model     string         `json:"model"`
			Connected *bool          `json:"connected"`
			Leader    *bool          `json:"leader"`
			ClusterID jsontext.Value `json:"cid"`
			ServerID  jsontext.Value `json:"sid"`
			Index     jsontext.Value `json:"index"`
		} `json:"rows"`
	}
	// Do not include raw output or server-supplied error details in status.
	if err := json.Unmarshal(data, &results); err != nil {
		return Database{}, fmt.Errorf("invalid OVSDB query response")
	}
	if len(results) != 1 || results[0].Error != "" || len(results[0].Rows) != 1 {
		return Database{}, fmt.Errorf("OVSDB query must return exactly one database row")
	}
	row := results[0].Rows[0]
	if row.Name != name || row.Connected == nil || row.Leader == nil {
		return Database{}, fmt.Errorf("OVSDB row has an unexpected name or missing health fields")
	}
	db := Database{Name: row.Name, Model: row.Model, Connected: *row.Connected, Leader: *row.Leader}
	var err error
	if db.ClusterID, err = optionalUUID(row.ClusterID); err != nil {
		return Database{}, fmt.Errorf("invalid OVSDB cluster ID")
	}
	if db.ServerID, err = optionalUUID(row.ServerID); err != nil {
		return Database{}, fmt.Errorf("invalid OVSDB server ID")
	}
	index, present, err := optionalAtom(row.Index)
	if err != nil {
		return Database{}, fmt.Errorf("invalid OVSDB applied index")
	}
	if present {
		if err := json.Unmarshal(index, &db.Index); err != nil || db.Index < 0 {
			return Database{}, fmt.Errorf("invalid OVSDB applied index")
		}
	}
	switch db.Model {
	case "clustered":
		if db.ClusterID == "" || db.ServerID == "" {
			return Database{}, fmt.Errorf("clustered database identity is not available")
		}
	case "standalone":
		if db.ClusterID != "" || db.ServerID != "" || present {
			return Database{}, fmt.Errorf("standalone database unexpectedly has RAFT metadata")
		}
	default:
		return Database{}, fmt.Errorf("unsupported OVSDB storage model")
	}
	return db, nil
}

// OVSDB represents optional values either as an atom or a set of zero/one
// atoms. UUID atoms are themselves tagged arrays and must not be unwrapped.
func optionalAtom(value jsontext.Value) (jsontext.Value, bool, error) {
	if len(value) == 0 {
		return nil, false, fmt.Errorf("missing optional column")
	}
	var tagged []jsontext.Value
	if err := json.Unmarshal(value, &tagged); err == nil && len(tagged) == 2 {
		var tag string
		if err := json.Unmarshal(tagged[0], &tag); err == nil && tag == "set" {
			var values []jsontext.Value
			if err := json.Unmarshal(tagged[1], &values); err != nil || values == nil || len(values) > 1 {
				return nil, false, fmt.Errorf("invalid optional set")
			}
			if len(values) == 0 {
				return nil, false, nil
			}
			return values[0], true, nil
		}
	}
	return value, true, nil
}

func optionalUUID(value jsontext.Value) (string, error) {
	atom, present, err := optionalAtom(value)
	if err != nil || !present {
		return "", err
	}
	var tagged []string
	if err := json.Unmarshal(atom, &tagged); err != nil || len(tagged) != 2 || tagged[0] != "uuid" {
		return "", fmt.Errorf("invalid UUID atom")
	}
	id, err := uuid.Parse(tagged[1])
	if err != nil || id == (uuid.UUID{}) {
		return "", fmt.Errorf("invalid UUID")
	}
	return id.String(), nil
}
