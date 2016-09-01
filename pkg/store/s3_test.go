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
	"testing"
)

func TestParseBadS3Locations(t *testing.T) {
	loc := "s3://foo"
	_, err := newS3Location(loc)
	if err == nil {
		t.Fatal("expected error parsing s3 location", loc)
	}

	loc = "s://foo/bucket/path"
	_, err = newS3Location(loc)
	if err == nil {
		t.Fatal("expected error parsing s3 location", loc)
	}

	loc = "/foo/bucket/path"
	_, err = newS3Location(loc)
	if err == nil {
		t.Fatal("expected error parsing s3 location", loc)
	}
}

func TestParseSimpleS3Locations(t *testing.T) {
	loc := "s3://foo/bar/"
	s3, err := newS3Location(loc)
	if err != nil {
		t.Fatal("expected success parsing s3 location", loc)
	}
	if s3.Host != "foo" {
		t.Fatal("expected 'foo' for host, got", s3.Host)
	}
	if s3.Bucket != "bar" {
		t.Fatal("expected 'bar' for bucket, got", s3.Bucket)
	}
	if s3.Path != "" {
		t.Fatal("expected '' for path, got", s3.Path)
	}

	loc = "s3://foo/bar/path"
	s3, err = newS3Location(loc)
	if err != nil {
		t.Fatal("expected success parsing s3 location", loc)
	}
	if s3.Host != "foo" {
		t.Fatal("expected 'foo' for host, got", s3.Host)
	}
	if s3.Bucket != "bar" {
		t.Fatal("expected 'bar' for bucket, got", s3.Bucket)
	}
	if s3.Path != "path" {
		t.Fatal("expected 'path' for path, got", s3.Path)
	}

	loc = "s3://foo/bar/long/path"
	s3, err = newS3Location(loc)
	if err != nil {
		t.Fatal("expected success parsing s3 location", loc)
	}
	if s3.Host != "foo" {
		t.Fatal("expected 'foo' for host, got", s3.Host)
	}
	if s3.Bucket != "bar" {
		t.Fatal("expected 'bar' for bucket, got", s3.Bucket)
	}
	if s3.Path != "long/path" {
		t.Fatal("expected 'long/path' for path, got", s3.Path)
	}

	loc = "s3://foo/bar/long/path/"
	s3, err = newS3Location(loc)
	if err != nil {
		t.Fatal("expected success parsing s3 location", loc)
	}
	if s3.Host != "foo" {
		t.Fatal("expected 'foo' for host, got", s3.Host)
	}
	if s3.Bucket != "bar" {
		t.Fatal("expected 'bar' for bucket, got", s3.Bucket)
	}
	if s3.Path != "long/path" {
		t.Fatal("expected 'long/path' for path, got", s3.Path)
	}
}
