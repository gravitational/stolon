package postgresql

import (
	"fmt"
	"os/exec"
	"path"
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
	result := path.Join(backupPath, fmt.Sprintf(`%v_%v.sql.gz`, dbName, time.Now().Unix()))
	log.Infof("Backup database %v to %v", dbName, result)

	cmd := pgDumpCommand("--host", connSettings.Host, "--port", connSettings.Port,
		"--username", connSettings.Username, "--file", result, "--compress", "6",
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
