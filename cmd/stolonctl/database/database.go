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
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"

	"github.com/gravitational/stolon/cmd/stolonctl/store"
)

type ConnSettings struct {
	Host     string
	Port     string
	Username string
}

func Backup(conn ConnSettings, s3Cred store.S3Credentials, dbName string, dest string) error {
	fileName := fmt.Sprintf(`%v_%v.sql.gz`, dbName, time.Now().Unix())

	if !strings.HasPrefix(dest, "s3://") {
		result := path.Join(dest, fileName)
		return backupToFile(conn, dbName, result)
	}

	tempDir, err := ioutil.TempDir("", "stolonctl")
	if err != nil {
		return trace.Wrap(err)
	}
	defer os.RemoveAll(tempDir)

	result := path.Join(tempDir, fileName)
	err = backupToFile(conn, dbName, result)
	if err != nil {
		return trace.Wrap(err)
	}
	err = store.UploadToS3(s3Cred, result, dest)
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func backupToFile(conn ConnSettings, dbName, dest string) error {
	log.Infof("Backup database %s to %s", dbName, dest)

	cmd := pgDumpCommand(
		"--host", conn.Host,
		"--port", conn.Port,
		"--username", conn.Username,
		"--file", dest,
		"--compress", "6",
		"--format", "custom",
		"--no-password", dbName)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		log.Infof("cmd output: %s", string(out))
	}
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func pgDumpCommand(args ...string) *exec.Cmd {
	return exec.Command("pg_dump", args...)
}

func Restore(conn ConnSettings, s3Cred store.S3Credentials, src string) error {
	if !strings.HasPrefix(src, "s3://") {
		return restoreFromFile(conn, src)
	}

	tempDir, err := ioutil.TempDir("", "stolonctl")
	if err != nil {
		return trace.Wrap(err)
	}
	defer os.RemoveAll(tempDir)

	result, err := store.DownloadFromS3(s3Cred, src, tempDir)
	if err != nil {
		return trace.Wrap(err)
	}
	return restoreFromFile(conn, result)
}

func restoreFromFile(conn ConnSettings, src string) error {
	log.Infof("Restore from %s", src)

	cmd := pgRestoreCommand(
		"--host", conn.Host,
		"--port", conn.Port,
		"--username", conn.Username,
		"--no-password", src)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		log.Infof("cmd output: %s", string(out))
	}
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func pgRestoreCommand(args ...string) *exec.Cmd {
	return exec.Command("pg_restore", args...)
}

func Create(conn ConnSettings, name string) error {
	log.Infof("Creating %s", name)

	err := psqlExecCommand(conn, fmt.Sprintf("CREATE DATABASE %s;", name))
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func Delete(conn ConnSettings, name string) error {
	log.Infof("Deleting %s", name)

	err := psqlExecCommand(conn, fmt.Sprintf("DROP DATABASE %s;", name))
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func Run(conn ConnSettings, filename string) error {
	log.Infof("Running file %s", filename)

	cmd := basePsqlCommand(conn, "--file", filename)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		log.Infof("cmd output: %s", string(out))
	}
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func psqlExecCommand(conn ConnSettings, exp string) error {
	cmd := basePsqlCommand(conn, "--command", exp)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		log.Infof("cmd output: %s", string(out))
	}
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func basePsqlCommand(conn ConnSettings, args ...string) *exec.Cmd {
	connArgs := []string{
		"--host", conn.Host,
		"--port", conn.Port,
		"--username", conn.Username,
		"--no-password", "postgres",
	}
	connArgs = append(connArgs, args...)
	return psqlCommand(connArgs...)
}

func psqlCommand(args ...string) *exec.Cmd {
	return exec.Command("psql", args...)
}
