package postgresql

import (
	"fmt"
	"os/exec"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/trace"
)

type ConnSettings struct {
	Host     string
	Port     string
	Username string
}

func Backup(c ConnSettings, DBName string) error {
	result := fmt.Sprintf(`%v_%v.sql.tar.gz`, DBName, time.Now().Unix())
	log.Infof("Backup database %v to %v", DBName, result)

	cmd := pgDumpCommand("--host", c.Host, "--port", c.Port, "--username", c.Username, DBName)
	out, err := cmd.CombinedOutput()
	log.Debugf("cmd output: %s", string(out))
	if err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func pgDumpCommand(args ...string) *exec.Cmd {
	return exec.Command("pg_dump", args...)
}
