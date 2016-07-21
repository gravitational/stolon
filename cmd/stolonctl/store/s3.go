package store

import (
	"net/url"
	"path"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	minio "github.com/minio/minio-go"
)

type S3Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

type S3Location struct {
	Host   string
	Bucket string
	Path   string
}

func newS3Location(path string) (*S3Location, error) {
	if !strings.HasPrefix(path, "s3://") {
		return nil, trace.Errorf("path has no s3 protocol specifier")
	}

	loc := &S3Location{}
	url, err := url.Parse(path)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	loc.Host = url.Host

	splitPath := strings.Split(strings.TrimPrefix(url.Path, "/"), "/")
	loc.Bucket = splitPath[0]

	if len(splitPath) > 1 {
		loc.Path = strings.TrimSuffix(strings.Join(splitPath[1:], "/"), "/")
	}

	if loc.Bucket == "" {
		return nil, trace.Errorf("no s3 bucket supplied")
	}

	log.Infof("host: %s, bucket: %s, path: %s", loc.Host, loc.Bucket, loc.Path)

	return loc, nil
}

func UploadToS3(cred S3Credentials, src string, dest string) error {
	loc, err := newS3Location(dest)
	if err != nil {
		return trace.Wrap(err)
	}

	client, err := minio.New(loc.Host, cred.AccessKeyID, cred.SecretAccessKey, true)
	if err != nil {
		return trace.Wrap(err)
	}

	_, filename := path.Split(src)
	n, err := client.FPutObject(loc.Bucket, path.Join(loc.Path, filename), src, "application/gzip")
	if err != nil {
		return trace.Wrap(err)
	}

	log.Infof("Successfully uploaded %s of size %d", filename, n)

	return nil
}

func DownloadFromS3(cred S3Credentials, src string, dest string) (string, error) {
	loc, err := newS3Location(src)
	if err != nil {
		return "", trace.Wrap(err)
	}

	client, err := minio.New(loc.Host, cred.AccessKeyID, cred.SecretAccessKey, true)
	if err != nil {
		return "", trace.Wrap(err)
	}

	dest = path.Join(dest, path.Base(src))
	err = client.CopyObject(loc.Bucket, src, dest, minio.NewCopyConditions())
	if err != nil {
		return "", trace.Wrap(err)
	}

	log.Infof("Successfully downloaded %s", dest)

	return dest, nil
}
