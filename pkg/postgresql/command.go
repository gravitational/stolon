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

package postgresql

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

	"github.com/gravitational/stolon/pkg/store"
)

type pgBinary string

const (
	PSQLBin      pgBinary = "psql"
	PgDumpBin    pgBinary = "pg_dump"
	PgRestoreBin pgBinary = "pg_restore"
)

type ConnSettings struct {
	Host     string
	Port     string
	Username string
}

func newPGCommand(name pgBinary, conn ConnSettings, args ...string) *exec.Cmd {
	connArgs := []string{
		"--host", conn.Host,
		"--port", conn.Port,
		"--username", conn.Username,
		"--no-password",
	}
	connArgs = append(connArgs, args...)
	return exec.Command(string(name), connArgs...)
}

func backupToFile(conn ConnSettings, name, dest string) error {
	log.Infof("Backup database %s to %s", name, dest)

	cmd := newPGCommand(
		PgDumpBin, conn,
		"--compress", "6",
		"--format", "custom",
		"--file", dest,
		name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return trace.Wrap(err, fmt.Sprintf("cmd output: %s", string(out)))
	}

	return nil
}

func restoreFromS3(conn ConnSettings, s3Cred store.S3Credentials, dbName, src string) error {
	tempDir, err := ioutil.TempDir("", "stolonctl")
	if err != nil {
		return trace.Wrap(err)
	}
	defer os.RemoveAll(tempDir)

	result, err := store.DownloadFromS3(s3Cred, src, tempDir)
	if err != nil {
		return trace.Wrap(err)
	}

	if err = restoreFromFile(conn, dbName, result); err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func restoreFromFile(conn ConnSettings, name, src string) error {
	log.Infof("Restore from %s", src)

	cmd := newPGCommand(
		PgRestoreBin, conn,
		"--dbname", name,
		"--clean",
		"--if-exists",
		"--single-transaction",
		src)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return trace.Wrap(err, fmt.Sprintf("cmd output: %s", string(out)))
	}

	return nil
}

func Backup(conn ConnSettings, s3Cred store.S3Credentials, dbName, folder string) (string, error) {
	file := fmt.Sprintf(`%v_%v.sql.gz`, dbName, time.Now().Format("2006-01-02T15:04:05"))

	// Local backup
	if !strings.HasPrefix(folder, "s3://") {
		dest := path.Join(folder, file)
		if err := backupToFile(conn, dbName, dest); err != nil {
			return "", trace.Wrap(err)
		}

		return dest, nil
	}

	// Local backup to temp dir and upload to S3
	tempDir, err := ioutil.TempDir("", "stolon")
	if err != nil {
		return "", trace.Wrap(err)
	}
	defer os.RemoveAll(tempDir)

	tempFile := path.Join(tempDir, file)
	if err = backupToFile(conn, dbName, tempFile); err != nil {
		return "", trace.Wrap(err)
	}

	result, err := store.UploadToS3(s3Cred, tempFile, folder)
	if err != nil {
		return "", trace.Wrap(err)
	}

	return result, nil
}

func Restore(conn ConnSettings, s3Cred store.S3Credentials, dbName, src string) error {
	if strings.HasPrefix(src, "s3://") {
		if err := restoreFromS3(conn, s3Cred, dbName, src); err != nil {
			return trace.Wrap(err)
		}
		return nil
	}

	if err := restoreFromFile(conn, dbName, src); err != nil {
		trace.Wrap(err)
	}

	return nil
}

func Create(conn ConnSettings, name string) error {
	log.Infof("Creating %s", name)

	cmd := newPGCommand(
		PSQLBin, conn,
		"--command", fmt.Sprintf("CREATE DATABASE %s;", name),
		"postgres")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return trace.Wrap(err, fmt.Sprintf("cmd output: %s", string(out)))
	}

	return nil
}

func Delete(conn ConnSettings, name string) error {
	log.Infof("Deleting %s", name)

	cmd := newPGCommand(
		PSQLBin, conn,
		"--command", fmt.Sprintf("DROP DATABASE %s;", name),
		"postgres")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return trace.Wrap(err, fmt.Sprintf("cmd output: %s", string(out)))
	}

	return nil
}

func Run(conn ConnSettings, filename string) error {
	log.Infof("Running file %s", filename)

	cmd := newPGCommand(
		PSQLBin, conn,
		"--file", filename,
		"postgres")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return trace.Wrap(err, fmt.Sprintf("cmd output: %s", string(out)))
	}

	return nil
}
