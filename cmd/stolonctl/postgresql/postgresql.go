package postgresql

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
	"github.com/minio/minio-go"
)

type ConnSettings struct {
	Host     string
	Port     string
	Username string
}

type S3Settings struct {
	AccessKeyID     string
	SecretAccessKey string
}

func Backup(connSettings ConnSettings, s3Settings S3Settings, dbName string, backupPath string) error {
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

		err = uploadToS3(s3Settings, result, backupPath)
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

func uploadToS3(s3Settings S3Settings, sourceFilename string, destination string) error {
	ssl := false

	url, err := url.Parse(destination)
	if err != nil {
		return trace.Wrap(err)
	}

	client, err := minio.New(url.Host, s3Settings.AccessKeyID, s3Settings.SecretAccessKey, ssl)
	if err != nil {
		return trace.Wrap(err)
	}

	_, filename := path.Split(sourceFilename)

	n, err := client.FPutObject(url.Path, filename, sourceFilename, "application/gzip")
	if err != nil {
		return trace.Wrap(err)
	}
	log.Printf("Successfully uploaded %s of size %d\n", filename, n)
	return nil
}

func pgDumpCommand(args ...string) *exec.Cmd {
	return exec.Command("pg_dump", args...)
}
