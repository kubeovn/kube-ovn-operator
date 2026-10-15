package backup

import (
	"bufio"
	"context"
	"crypto/sha1" // OVSDB's on-disk framing requires SHA-1; references use SHA-256.
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

type fileInfo struct {
	SchemaVersion string
	Size          int64
	SHA256        string
}

type schema struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Tables  map[string]struct {
		Columns map[string]jsontext.Value `json:"columns"`
	} `json:"tables"`
}

// inspect validates the exact two-record native backup format, schema/table/row
// shape, JSON, lengths and checksums without buffering the data transaction.
// It is not an OVSDB type-system implementation. Semantic validity comes from a
// successful native backup of the pinned process; native restore is tested too.
func inspect(ctx context.Context, input io.Reader, database string, maxBytes int64) (fileInfo, error) {
	contentHash := sha256.New()
	counted := &countingReader{ctx: ctx, input: io.LimitReader(input, maxBytes+1)}
	reader := bufio.NewReaderSize(io.TeeReader(counted, contentHash), 4096)
	var definition schema
	for record := range 2 {
		header, err := reader.ReadSlice('\n')
		if err != nil || len(header) > 128 {
			return fileInfo{}, errors.New("missing or oversized OVSDB record header")
		}
		parts := strings.Split(strings.TrimSuffix(string(header), "\n"), " ")
		if len(parts) != 4 || parts[0] != "OVSDB" || parts[1] != "JSON" {
			return fileInfo{}, errors.New("backup is not a standalone OVSDB snapshot")
		}
		length, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || length < 3 || length > maxBytes || strconv.FormatInt(length, 10) != parts[2] || !validDigest(parts[3], sha1.Size) {
			return fileInfo{}, errors.New("invalid OVSDB record size or checksum")
		}
		if record == 0 && length > 4<<20 {
			return fileInfo{}, errors.New("backup schema exceeds 4 MiB")
		}
		body := &io.LimitedReader{R: reader, N: length}
		hash := sha1.New()
		last := &countingReader{ctx: ctx, input: io.TeeReader(body, hash)}
		decoder := jsontext.NewDecoder(&jsonLimitReader{input: last})
		if record == 0 {
			if err := json.UnmarshalDecode(decoder, &definition); err != nil {
				return fileInfo{}, errors.New("invalid backup schema JSON")
			}
			if definition.Name != database || definition.Version == "" || len(definition.Tables) == 0 {
				return fileInfo{}, errors.New("backup schema does not match the requested database")
			}
		} else if err := inspectData(decoder, definition); err != nil {
			return fileInfo{}, err
		}
		if _, err := decoder.ReadToken(); !errors.Is(err, io.EOF) {
			return fileInfo{}, errors.New("extra or malformed JSON in OVSDB record")
		}
		if body.N != 0 || last.last != '\n' || hex.EncodeToString(hash.Sum(nil)) != parts[3] {
			return fileInfo{}, errors.New("truncated or corrupt OVSDB record")
		}
	}
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		return fileInfo{}, errors.New("trailing content after native backup")
	}
	if err := ctx.Err(); err != nil {
		return fileInfo{}, err
	}
	if counted.count > maxBytes {
		return fileInfo{}, errors.New("backup exceeds configured size limit")
	}
	return fileInfo{SchemaVersion: definition.Version, Size: counted.count, SHA256: hex.EncodeToString(contentHash.Sum(nil))}, nil
}

func inspectData(d *jsontext.Decoder, definition schema) error {
	if err := expect(d, '{'); err != nil {
		return err
	}
	for d.PeekKind() != '}' {
		name, err := d.ReadToken()
		if err != nil || name.Kind() != '"' {
			return errors.New("invalid backup table name")
		}
		switch name.String() {
		case "_date":
			value, err := d.ReadToken()
			if err != nil || value.Kind() != '0' {
				return errors.New("invalid backup timestamp")
			}
			date, err := value.Int()
			if err != nil || date <= 0 {
				return errors.New("invalid backup timestamp")
			}
		case "_comment":
			if err := expect(d, '"'); err != nil {
				return err
			}
		case "_is_diff":
			value, err := d.ReadToken()
			if err != nil || (value.Kind() != 't' && value.Kind() != 'f') {
				return errors.New("invalid backup diff marker")
			}
		default:
			table, ok := definition.Tables[name.String()]
			if !ok {
				return errors.New("backup contains a table absent from its schema")
			}
			if err := inspectTable(d, table.Columns); err != nil {
				return err
			}
		}
	}
	return expect(d, '}')
}

func inspectTable(d *jsontext.Decoder, columns map[string]jsontext.Value) error {
	if err := expect(d, '{'); err != nil {
		return err
	}
	for rows := 0; d.PeekKind() != '}'; rows++ {
		if rows >= 250000 {
			return errors.New("backup table exceeds 250000 rows")
		}
		id, err := d.ReadToken()
		if err != nil || id.Kind() != '"' || !membership.ValidID(id.String()) {
			return errors.New("backup row has an invalid UUID")
		}
		if err := expect(d, '{'); err != nil {
			return err
		}
		for d.PeekKind() != '}' {
			column, err := d.ReadToken()
			if err != nil || column.Kind() != '"' {
				return errors.New("invalid backup column")
			}
			if _, ok := columns[column.String()]; !ok {
				return errors.New("backup contains a column absent from its schema")
			}
			if err := inspectValue(d); err != nil {
				return err
			}
		}
		if err := expect(d, '}'); err != nil {
			return err
		}
	}
	return expect(d, '}')
}

// OVSDB values are scalars or arrays (sets, maps and UUIDs), never objects.
// Rejecting objects also bounds duplicate-name tracking to schema/table/rows.
func inspectValue(d *jsontext.Decoder) error {
	depth := d.StackDepth()
	for {
		token, err := d.ReadToken()
		if err != nil || token.Kind() == '{' || token.Kind() == '}' || token.Kind() == 'n' || d.StackDepth()-depth > 8 {
			return errors.New("invalid OVSDB value in backup row")
		}
		if d.StackDepth() == depth {
			return nil
		}
	}
}

// Bound individual JSON tokens before the decoder can allocate an unbounded
// string. Schema and overall byte limits are independent of this token limit.
type jsonLimitReader struct {
	input   io.Reader
	quoted  bool
	escaped bool
	span    int
}

func (r *jsonLimitReader) Read(data []byte) (int, error) {
	n, err := r.input.Read(data[:min(len(data), 32<<10)])
	for _, b := range data[:n] {
		if r.quoted {
			r.span++
			if r.escaped {
				r.escaped = false
			} else if b == '\\' {
				r.escaped = true
			} else if b == '"' {
				r.quoted, r.span = false, 0
			}
		} else {
			switch b {
			case '"':
				r.quoted, r.span = true, 0
			case '{', '}', '[', ']', ':', ',', ' ', '\n', '\r', '\t':
				r.span = 0
			default:
				r.span++
			}
		}
		if r.span > 1<<20 {
			return 0, errors.New("backup JSON token exceeds 1 MiB")
		}
	}
	return n, err
}

func expect(d *jsontext.Decoder, kind jsontext.Kind) error {
	token, err := d.ReadToken()
	if err != nil || token.Kind() != kind {
		return fmt.Errorf("invalid backup JSON structure; expected %q", kind)
	}
	return nil
}

func validDigest(value string, bytes int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes && hex.EncodeToString(decoded) == value
}

type countingReader struct {
	ctx   context.Context
	input io.Reader
	count int64
	last  byte
}

func (r *countingReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.input.Read(data)
	r.count += int64(n)
	if n > 0 {
		r.last = data[n-1]
	}
	return n, err
}
