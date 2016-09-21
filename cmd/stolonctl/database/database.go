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

package database

import (
	"github.com/gravitational/stolon/pkg/postgresql"
	"github.com/gravitational/stolon/pkg/store"
	"github.com/gravitational/trace"
)

func Backup(conn postgresql.ConnSettings, s3Cred store.S3Credentials, dbName string, dest string) error {
	if _, err := postgresql.Backup(conn, s3Cred, dbName, dest); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func Restore(conn postgresql.ConnSettings, s3Cred store.S3Credentials, src string) error {
	if err := postgresql.Restore(conn, s3Cred, src); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func Create(conn postgresql.ConnSettings, name string) error {
	if err := postgresql.Create(conn, name); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func Delete(conn postgresql.ConnSettings, name string) error {
	if err := postgresql.Delete(conn, name); err != nil {
		return trace.Wrap(err)
	}
	return nil
}

func Run(conn postgresql.ConnSettings, filename string) error {
	if err := postgresql.Run(conn, filename); err != nil {
		return trace.Wrap(err)
	}
	return nil
}
