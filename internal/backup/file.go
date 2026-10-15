package backup

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"uuid"
)

// FileStore uses an already provisioned, operator-owned persistent directory.
// It neither provisions storage nor deletes published backups. The caller must
// provide a filesystem supporting atomic rename and file/directory fsync.
type FileStore struct{ root *os.Root }

func OpenFileStore(directory string) (*FileStore, error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	return &FileStore{root: root}, nil
}

func (s *FileStore) Close() error { return s.root.Close() }

func (s *FileStore) Publish(ctx context.Context, produce func(io.Writer) (Record, error)) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	temporary := ".pending-" + uuid.New().String()
	if err := s.root.Mkdir(temporary, 0700); err != nil {
		return "", err
	}
	defer func() { _ = s.root.RemoveAll(temporary) }()
	data, err := s.root.OpenFile(temporary+"/snapshot.db", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	record, captureErr := produce(data)
	err = errors.Join(captureErr, data.Sync(), data.Close())
	if err != nil {
		return "", err
	}
	manifest, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	if len(manifest) > 64<<10 {
		return "", errors.New("backup manifest exceeds 64 KiB")
	}
	metadata, err := s.root.OpenFile(temporary+"/record.json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	_, writeErr := metadata.Write(manifest)
	if err := errors.Join(writeErr, metadata.Sync(), metadata.Close(), s.syncDirectory(temporary), ctx.Err()); err != nil {
		return "", err
	}
	name := fmt.Sprintf("%x", sha256.Sum256(manifest))
	// The destination is a nonempty directory if it already exists, so rename
	// cannot replace a published record, even if an identical upload is retried.
	if err := s.root.Rename(temporary, name); err != nil {
		return "", err
	}
	if err := errors.Join(s.syncDirectory("."), ctx.Err()); err != nil {
		return "", err
	}
	return "sha256:" + name, nil
}

func (s *FileStore) syncDirectory(name string) error {
	dir, err := s.root.Open(name)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func (s *FileStore) Open(ctx context.Context, reference string) (Record, io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, nil, err
	}
	name, ok := strings.CutPrefix(reference, "sha256:")
	if !ok || !validDigest(name, sha256.Size) {
		return Record{}, nil, errors.New("invalid backup reference")
	}
	manifest, err := s.openRegular(name + "/record.json")
	if err != nil {
		return Record{}, nil, err
	}
	content, readErr := io.ReadAll(io.LimitReader(manifest, (64<<10)+1))
	if err := errors.Join(readErr, manifest.Close(), ctx.Err()); err != nil {
		return Record{}, nil, err
	}
	if len(content) > 64<<10 || fmt.Sprintf("%x", sha256.Sum256(content)) != name {
		return Record{}, nil, errors.New("backup manifest does not match its reference")
	}
	var record Record
	if err := json.Unmarshal(content, &record, json.RejectUnknownMembers(true)); err != nil {
		// Do not include database contents or untrusted metadata in errors.
		return Record{}, nil, errors.New("invalid backup manifest")
	}
	data, err := s.openRegular(name + "/snapshot.db")
	if err != nil {
		return Record{}, nil, err
	}
	return record, data, nil
}

func (s *FileStore) openRegular(name string) (*os.File, error) {
	info, err := s.root.Lstat(name)
	if err != nil || !info.Mode().IsRegular() {
		return nil, errors.New("backup object is not an accessible regular file")
	}
	file, err := s.root.Open(name)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, errors.New("backup object changed while opening")
	}
	return file, nil
}
