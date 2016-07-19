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

	"github.com/gravitational/stolon/cmd/stolonctl/store"
)

type ConnSettings struct {
	Host     string
	Port     string
	Username string
}

func Backup(connSettings ConnSettings, s3Cred store.S3Credentials, dbName string, backupPath string) error {
	fileName := fmt.Sprintf(`%v_%v.sql.gz`, dbName, time.Now().Unix())
	if strings.HasPrefix(backupPath, "s3://") {
		tempDir, err := ioutil.TempDir("", "stolonctl")
		if err != nil {
			return trace.Wrap(err)
		}

		defer os.RemoveAll(tempDir)

		result := path.Join(tempDir, fileName)
		err = backupToFile(connSettings, dbName, result)
		if err != nil {
			return trace.Wrap(err)
		}

		err = store.UploadToS3(s3Cred, result, backupPath)
		if err != nil {
			return trace.Wrap(err)
		}

		return nil
	} else {
		result := path.Join(backupPath, fileName)
		return backupToFile(connSettings, dbName, result)
	}
}

func backupToFile(connSettings ConnSettings, dbName string, fileName string) error {
	log.Infof("Backup database %v to %v", dbName, fileName)

	cmd := pgDumpCommand("--host", connSettings.Host, "--port", connSettings.Port,
		"--username", connSettings.Username, "--file", fileName, "--compress", "6",
		dbName)

	out, err := cmd.CombinedOutput()
	log.Infof("cmd output: %s", string(out))
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func pgDumpCommand(args ...string) *exec.Cmd {
	return exec.Command("pg_dump", args...)
}
