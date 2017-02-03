// Copyright 2016 Gravitational, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

func newS3Location(rawURL string) (*S3Location, error) {
	if !strings.HasPrefix(rawURL, "s3://") {
		return nil, trace.Errorf("path has no s3 protocol specifier")
	}

	loc := &S3Location{}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, trace.Wrap(err)
	}
	loc.Host = parsedURL.Host

	splitPath := strings.Split(strings.TrimPrefix(parsedURL.Path, "/"), "/")
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

func UploadToS3(cred S3Credentials, src, dest string) (string, error) {
	log.Infof("Uploading %s to %s", src, dest)
	loc, err := newS3Location(dest)
	if err != nil {
		return "", trace.Wrap(err)
	}

	client, err := minio.NewV2(loc.Host, cred.AccessKeyID, cred.SecretAccessKey, true)
	if err != nil {
		return "", trace.Wrap(err)
	}

	found, err := client.BucketExists(loc.Bucket)
	if err != nil {
		return "", trace.Wrap(err)
	}

	if !found {
		err = client.MakeBucket(loc.Bucket, "G1")
		if err != nil {
			return "", trace.Wrap(err)
		}
	}

	_, filename := path.Split(src)
	dest = path.Join(loc.Path, filename)
	uploadedSize, err := client.FPutObject(loc.Bucket, dest, src, "application/gzip")
	if err != nil {
		return "", trace.Wrap(err)
	}
	log.Infof("Successfully uploaded '%s' of size %d", dest, uploadedSize)

	return dest, nil
}

func DownloadFromS3(cred S3Credentials, src string, dest string) (string, error) {
	loc, err := newS3Location(src)
	if err != nil {
		return "", trace.Wrap(err)
	}

	client, err := minio.NewV2(loc.Host, cred.AccessKeyID, cred.SecretAccessKey, true)
	if err != nil {
		return "", trace.Wrap(err)
	}

	dest = path.Join(dest, path.Base(src))
	if err := client.FGetObject(loc.Bucket, loc.Path, dest); err != nil {
		return "", trace.Wrap(err)
	}

	log.Infof("Successfully downloaded %s to %s", src, dest)

	return dest, nil
}
