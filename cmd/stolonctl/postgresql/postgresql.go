package postgresql

import (
	"fmt"
	"os/exec"
	"path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
)

type ConnSettings struct {
	Host     string
	Port     string
	Username string
}

func Backup(connSettings ConnSettings, dbName string, backupPath string) error {
	if strings.HasPrefix(backupPath, "s3://") {
		// backup to temp file
		// upload to s3
		return nil
	} else {
		result := path.Join(backupPath, fmt.Sprintf(`%v_%v.sql.gz`, dbName, time.Now().Unix()))
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
