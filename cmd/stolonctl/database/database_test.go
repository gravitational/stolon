package database

import (
	"testing"
)

func TestParseBadS3Locations(t *testing.T) {
	loc := "s3://foo"
	_, err := parseS3Location(loc)
	if err == nil {
		t.Fatal("expected error parsing s3 location", loc)
	}

	loc = "s://foo/bucket/path"
	_, err = parseS3Location(loc)
	if err == nil {
		t.Fatal("expected error parsing s3 location", loc)
	}

	loc = "/foo/bucket/path"
	_, err = parseS3Location(loc)
	if err == nil {
		t.Fatal("expected error parsing s3 location", loc)
	}
}

func TestParseSimpleS3Locations(t *testing.T) {
	loc := "s3://foo/bar/"
	s3, err := parseS3Location(loc)
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
	s3, err = parseS3Location(loc)
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
	s3, err = parseS3Location(loc)
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
	s3, err = parseS3Location(loc)
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
