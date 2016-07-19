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
	loc := &S3Location{}
	url, err := url.Parse(path)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	loc.Host = url.Host

	splitPath := strings.Split(strings.TrimPrefix(url.Path, "/"), "/")
	loc.Bucket = splitPath[0]

	if len(splitPath) > 1 {
		loc.Path = strings.Join(splitPath[1:], "")
	}

	log.Infof("Backup host: %v, bucket: %v, path: %v", loc.Host, loc.Bucket, loc.Path)

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
