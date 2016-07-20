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

func Backup(conn ConnSettings, s3Cred store.S3Credentials, dbName string, destPath string) error {
	fileName := fmt.Sprintf(`%v_%v.sql.gz`, dbName, time.Now().Unix())
	if strings.HasPrefix(destPath, "s3://") {
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

		err = store.UploadToS3(s3Cred, result, destPath)
		if err != nil {
			return trace.Wrap(err)
		}

		return nil
	} else {
		result := path.Join(destPath, fileName)
		return backupToFile(conn, dbName, result)
	}
}

func backupToFile(conn ConnSettings, dbName string, filePath string) error {
	log.Infof("Backup database %v to %v", dbName, filePath)

	cmd := pgDumpCommand(
		"--host", conn.Host,
		"--port", conn.Port,
		"--username", conn.Username,
		"--file", filePath,
		"--compress", "6",
		"--format", "custom",
		dbName)
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

func Restore(conn ConnSettings, s3Cred store.S3Credentials, srcPath string) error {
	return restoreFromFile(conn, srcPath)
}

func restoreFromFile(conn ConnSettings, fileName string) error {
	log.Infof("Restore from %v", fileName)

	cmd := pgRestoreCommand(
		"--host", conn.Host,
		"--port", conn.Port,
		"--username", conn.Username,
		fileName)
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
