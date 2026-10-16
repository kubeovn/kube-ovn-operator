package backup

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"
)

// Credentials are read from an existing process credential source. The
// operator never creates, refreshes, logs or persists these values.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Endpoint        string
}

func CredentialsFromEnvironment() (Credentials, error) {
	credentials := Credentials{
		AccessKeyID:     strings.TrimSpace(os.Getenv("AWS_ACCESS_KEY_ID")),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		Region:          strings.TrimSpace(os.Getenv("AWS_REGION")),
		Endpoint:        strings.TrimSpace(os.Getenv("AWS_S3_ENDPOINT")),
	}
	if credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" {
		return Credentials{}, errors.New("S3 backup requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from an existing credential source")
	}
	if credentials.Region == "" {
		credentials.Region = strings.TrimSpace(os.Getenv("AWS_DEFAULT_REGION"))
	}
	if credentials.Region == "" {
		credentials.Region = "us-east-1"
	}
	return credentials, nil
}

// S3Store implements backup.Store with immutable object keys. A failed
// publication may leave an unreachable object, but no reference is returned
// until both the snapshot and manifest PUTs succeed. The store never deletes
// objects; retention is a separate lifecycle policy.
type S3Store struct {
	destination S3Destination
	credentials Credentials
	client      *http.Client
	clock       func() time.Time
	endpoint    *url.URL
	pathStyle   bool
}

func NewS3Store(rawDestination string, credentials Credentials, client *http.Client) (*S3Store, error) {
	destination, err := ParseS3Destination(rawDestination)
	if err != nil {
		return nil, err
	}
	credentials.AccessKeyID = strings.TrimSpace(credentials.AccessKeyID)
	credentials.Region = strings.TrimSpace(credentials.Region)
	credentials.Endpoint = strings.TrimSpace(credentials.Endpoint)
	if credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" {
		return nil, errors.New("S3 store requires an access key and secret key from an existing credential source")
	}
	if credentials.Region == "" {
		credentials.Region = "us-east-1"
	}
	pathStyle := credentials.Endpoint != "" || strings.Contains(destination.Bucket, ".")
	endpoint, err := s3Endpoint(destination.Bucket, credentials, pathStyle)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &S3Store{destination: destination, credentials: credentials, client: client, clock: time.Now, endpoint: endpoint, pathStyle: pathStyle}, nil
}

func s3Endpoint(bucket string, credentials Credentials, pathStyle bool) (*url.URL, error) {
	if strings.TrimSpace(credentials.Endpoint) != "" {
		endpoint, err := url.Parse(strings.TrimSpace(credentials.Endpoint))
		if err != nil || endpoint.Scheme != "http" && endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
			return nil, errors.New("AWS_S3_ENDPOINT must be an absolute HTTP(S) URL without credentials, query or fragment")
		}
		return endpoint, nil
	}
	if pathStyle {
		host := "s3." + credentials.Region + ".amazonaws.com"
		if credentials.Region == "us-east-1" {
			host = "s3.amazonaws.com"
		}
		return &url.URL{Scheme: "https", Host: host}, nil
	}
	host := bucket + ".s3." + credentials.Region + ".amazonaws.com"
	if credentials.Region == "us-east-1" {
		host = bucket + ".s3.amazonaws.com"
	}
	return &url.URL{Scheme: "https", Host: host}, nil
}

func (s *S3Store) Publish(ctx context.Context, produce func(io.Writer) (Record, error)) (string, error) {
	if s == nil || s.client == nil || s.endpoint == nil || produce == nil {
		return "", errors.New("S3 store is not configured")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	temporary, err := os.CreateTemp("", "kube-ovn-backup-")
	if err != nil {
		return "", fmt.Errorf("create temporary backup: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryName)
	}()
	if err := temporary.Chmod(0600); err != nil {
		return "", fmt.Errorf("protect temporary backup: %w", err)
	}
	record, produceErr := produce(temporary)
	if err := errors.Join(produceErr, temporary.Sync(), temporary.Close(), ctx.Err()); err != nil {
		return "", err
	}
	manifest, err := json.Marshal(record)
	if err != nil {
		return "", fmt.Errorf("marshal backup manifest: %w", err)
	}
	if len(manifest) > 64<<10 {
		return "", errors.New("backup manifest exceeds 64 KiB")
	}
	name := fmt.Sprintf("%x", sha256.Sum256(manifest))
	if err := s.putFile(ctx, temporaryName, s.objectKey(name, "snapshot.db"), "application/octet-stream"); err != nil {
		return "", err
	}
	if err := s.putBytes(ctx, s.objectKey(name, "record.json"), manifest, "application/json"); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return "sha256:" + name, nil
}

func (s *S3Store) Open(ctx context.Context, reference string) (Record, io.ReadCloser, error) {
	if s == nil || s.client == nil || s.endpoint == nil || s.clock == nil {
		return Record{}, nil, errors.New("S3 store is not configured")
	}
	name, ok := strings.CutPrefix(reference, "sha256:")
	if !ok || !validDigest(name, sha256.Size) {
		return Record{}, nil, errors.New("invalid backup reference")
	}
	manifest, err := s.getBytes(ctx, s.objectKey(name, "record.json"), 64<<10)
	if err != nil {
		return Record{}, nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(manifest)) != name {
		return Record{}, nil, errors.New("backup manifest does not match its reference")
	}
	var record Record
	if err := json.Unmarshal(manifest, &record, json.RejectUnknownMembers(true)); err != nil {
		return Record{}, nil, errors.New("invalid backup manifest")
	}
	data, err := s.get(ctx, s.objectKey(name, "snapshot.db"))
	if err != nil {
		return Record{}, nil, err
	}
	return record, data, nil
}

func (s *S3Store) objectKey(name, object string) string {
	key := name + "/" + object
	if s.destination.Prefix == "" {
		return key
	}
	return s.destination.Prefix + "/" + key
}

func (s *S3Store) objectURL(key string) *url.URL {
	endpoint := s.endpoint.Clone()
	// Custom endpoints and dotted AWS bucket names use path-style URLs. This
	// avoids wildcard DNS/TLS requirements and keeps compatible services
	// independent of bucket subdomain routing.
	if s.pathStyle {
		endpoint.Path = appendURLPath(endpoint.Path, s.destination.Bucket+"/"+key)
		endpoint.RawPath = ""
		return endpoint
	}
	endpoint.Path = appendURLPath(endpoint.Path, key)
	endpoint.RawPath = ""
	return endpoint
}

func appendURLPath(base, suffix string) string {
	base = strings.TrimRight(base, "/")
	suffix = strings.TrimLeft(suffix, "/")
	if base == "" {
		return "/" + suffix
	}
	if suffix == "" {
		return base
	}
	return base + "/" + suffix
}

func (s *S3Store) putFile(ctx context.Context, name, key, contentType string) error {
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return s.do(ctx, http.MethodPut, key, file, info.Size(), contentType, fmt.Sprintf("%x", hash.Sum(nil)))
}

func (s *S3Store) putBytes(ctx context.Context, key string, data []byte, contentType string) error {
	hash := sha256.Sum256(data)
	return s.do(ctx, http.MethodPut, key, bytes.NewReader(data), int64(len(data)), contentType, hex.EncodeToString(hash[:]))
}

func (s *S3Store) get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.request(ctx, http.MethodGet, key, nil, 0, "", "")
}

func (s *S3Store) getBytes(ctx context.Context, key string, max int64) ([]byte, error) {
	body, err := s.get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()
	data, err := io.ReadAll(io.LimitReader(body, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, errors.New("backup manifest exceeds 64 KiB")
	}
	return data, nil
}

func (s *S3Store) do(ctx context.Context, method, key string, body io.Reader, length int64, contentType, payloadHash string) error {
	response, err := s.request(ctx, method, key, body, length, contentType, payloadHash)
	if response != nil {
		_ = response.Close()
	}
	return err
}

func (s *S3Store) request(ctx context.Context, method, key string, body io.Reader, length int64, contentType, payloadHash string) (io.ReadCloser, error) {
	if s == nil || s.client == nil || s.endpoint == nil || s.clock == nil {
		return nil, errors.New("S3 store is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if payloadHash == "" {
		emptyHash := sha256.Sum256(nil)
		payloadHash = hex.EncodeToString(emptyHash[:])
	}
	now := s.clock().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	requestURL := s.objectURL(key)
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), body)
	if err != nil {
		return nil, err
	}
	if length >= 0 {
		request.ContentLength = length
	}
	request.Host = requestURL.Host
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	request.Header.Set("X-Amz-Date", amzDate)
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if s.credentials.SessionToken != "" {
		request.Header.Set("X-Amz-Security-Token", s.credentials.SessionToken)
	}
	canonicalHeaders, signedHeaders := canonicalHeaders(request)
	canonicalRequest := strings.Join([]string{method, requestURL.EscapedPath(), canonicalQuery(requestURL), canonicalHeaders, signedHeaders, payloadHash}, "\n")
	scope := date + "/" + s.credentials.Region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", amzDate, scope, hashString(canonicalRequest)}, "\n")
	signature := hex.EncodeToString(signingKey(s.credentials.SecretAccessKey, date, s.credentials.Region, "s3", stringToSign))
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.credentials.AccessKeyID+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
	response, err := s.client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_ = response.Body.Close()
		return nil, fmt.Errorf("S3 request %s %s failed with HTTP %d", method, key, response.StatusCode)
	}
	return response.Body, nil
}

func canonicalHeaders(request *http.Request) (string, string) {
	values := map[string]string{"host": request.Host, "x-amz-content-sha256": request.Header.Get("X-Amz-Content-Sha256"), "x-amz-date": request.Header.Get("X-Amz-Date")}
	if value := request.Header.Get("Content-Type"); value != "" {
		values["content-type"] = value
	}
	if value := request.Header.Get("X-Amz-Security-Token"); value != "" {
		values["x-amz-security-token"] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var headers strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&headers, "%s:%s\n", key, strings.Join(strings.Fields(values[key]), " "))
	}
	return headers.String(), strings.Join(keys, ";")
}

func canonicalQuery(requestURL *url.URL) string {
	return requestURL.Query().Encode()
}

func hashString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func signingKey(secret, date, region, service, value string) []byte {
	dateKey := hmacBytes([]byte("AWS4"+secret), date)
	regionKey := hmacBytes(dateKey, region)
	serviceKey := hmacBytes(regionKey, service)
	key := hmacBytes(serviceKey, "aws4_request")
	return hmacBytes(key, value)
}

func hmacBytes(key []byte, value string) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write([]byte(value))
	return hash.Sum(nil)
}
