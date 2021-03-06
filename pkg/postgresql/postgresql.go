// Copyright 2015 Sorint.lab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied
// See the License for the specific language governing permissions and
// limitations under the License.

package postgresql

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gravitational/stolon/common"
	"github.com/gravitational/trace"

	"github.com/coreos/pkg/capnslog"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
)

const (
	startTimeout       = 2 * time.Minute
	sleepBetweenChecks = 200 * time.Millisecond
)

var log = capnslog.NewPackageLogger("github.com/gravitational/pkg/postgresql", "postgresql")

type Manager struct {
	name            string
	pgBinPath       string
	dataDir         string
	confDir         string
	parameters      Parameters
	localConnString string
	replConnString  string
	suUsername      string
	suPassword      string
	replUsername    string
	replPassword    string
	requestTimeout  time.Duration
}

type Parameters map[string]string

func (s Parameters) Copy() Parameters {
	parameters := Parameters{}
	for k, v := range s {
		parameters[k] = v
	}
	return parameters
}

func (s Parameters) Set(k, v string) {
	s[k] = v
}

func (s Parameters) Get(k string) (string, bool) {
	v, ok := s[k]
	return v, ok
}

func (s Parameters) Equals(is Parameters) bool {
	return reflect.DeepEqual(s, is)
}

func NewManager(name string, pgBinPath string, dataDir string, confDir string, parameters Parameters, localConnString, replConnString, suUsername, suPassword, replUsername, replPassword string, requestTimeout time.Duration) *Manager {
	return &Manager{
		name:            name,
		pgBinPath:       pgBinPath,
		dataDir:         filepath.Join(dataDir, "postgres"),
		confDir:         confDir,
		parameters:      parameters,
		replConnString:  replConnString,
		localConnString: localConnString,
		suUsername:      suUsername,
		suPassword:      suPassword,
		replUsername:    replUsername,
		replPassword:    replPassword,
		requestTimeout:  requestTimeout,
	}
}

func (p *Manager) SetParameters(parameters Parameters) {
	p.parameters = parameters
}

func (p *Manager) GetParameters() Parameters {
	return p.parameters
}

func (p *Manager) Init() error {
	name := filepath.Join(p.pgBinPath, "initdb")
	out, err := exec.Command(name, "-D", p.dataDir, "-U", p.suUsername).CombinedOutput()
	if err != nil {
		err = fmt.Errorf("error: %v, output: %s", err, out)
		goto out
	}
	// Move current (initdb generated) postgresql.conf to postgresql-base.conf
	if err = os.Rename(filepath.Join(p.dataDir, "postgresql.conf"), filepath.Join(p.dataDir, "postgresql-base.conf")); err != nil {
		err = fmt.Errorf("error moving postgresql.conf file to postgresql-base.conf: %v", err)
		goto out
	}
	// Create default confDir
	if err = os.Mkdir(filepath.Join(p.dataDir, "conf.d"), 0700); err != nil {
		err = fmt.Errorf("error creating conf.d inside dataDir: %v", err)
		goto out
	}
	if err = p.WriteConf(); err != nil {
		err = fmt.Errorf("error writing postgresql.conf file: %v", err)
		goto out
	}

	log.Infof("Setting required accesses to pg_hba.conf")
	if err = p.writePgHba(); err != nil {
		err = fmt.Errorf("error setting requires accesses to pg_hba.conf: %v", err)
		goto out
	}

	if err = p.Start(); err != nil {
		err = fmt.Errorf("error starting instance: %v", err)
		goto out
	}

	log.Infof("Setting roles")
	if err = p.SetupRoles(); err != nil {
		err = fmt.Errorf("error setting roles: %v", err)
		goto out
	}

	log.Info("Creating function for computing replication lag")
	if err = p.CreateReplicationLagFunction(); err != nil {
		err = fmt.Errorf("error creating replication lag function: %v", err)
		goto out
	}

	if err = p.Stop(false); err != nil {
		err = fmt.Errorf("error stopping instance: %v", err)
		goto out
	}

	// On every error remove the dataDir, so we don't end with an half initialized database
out:
	if err != nil {
		os.RemoveAll(p.dataDir)
		return err
	}
	return nil
}

// Start starts the PostgreSQL server
func (p *Manager) Start() error {
	log.Info("Starting database")
	if err := p.WriteConf(); err != nil {
		return trace.Wrap(err, "error writing conf file")
	}
	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "start", "-w", "-t",
		strconv.FormatInt(int64(startTimeout/time.Second), 10), "-D", p.dataDir)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return trace.Wrap(err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return trace.Wrap(err)
	}

	log.Infof("Executing command: %s", cmd.String())
	if err = cmd.Start(); err != nil {
		return trace.Wrap(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go logOutput(stdoutPipe, &wg, "stdout")
	go logOutput(stderrPipe, &wg, "stderr")
	if err = cmd.Wait(); err != nil {
		return trace.Wrap(err)
	}
	wg.Wait()

	return nil
}

func (p *Manager) Stop(fast bool) error {
	log.Infof("Stopping database")
	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "stop", "-w", "-D", p.dataDir, "-o", "-c unix_socket_directories=/tmp")
	if fast {
		cmd.Args = append(cmd.Args, "-m", "fast")
	}
	log.Infof("Executing command: %s", cmd.String())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error: %v, output: %s", err, string(out))
	}
	return nil
}

// IsReady checks if the PostgreSQL server is accepting connections
func (p *Manager) IsReady() (ready bool, err error) {
	start := time.Now()
	for time.Since(start) < startTimeout {
		if err := p.ping(); err == nil {
			return true, nil
		}
		time.Sleep(sleepBetweenChecks)
	}
	return false, trace.LimitExceeded("timeout waiting for PostgreSQL to become ready")
}

func (p *Manager) IsStarted() (bool, error) {
	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "status", "-w", "-D", p.dataDir, "-o", "-c unix_socket_directories=/tmp")
	_, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			status := cmd.ProcessState.Sys().(syscall.WaitStatus).ExitStatus()
			if status == 3 {
				return false, nil
			}
		}
		return false, fmt.Errorf("cannot get instance state: %v", err)
	}
	return true, nil
}

func (p *Manager) Reload() error {
	log.Infof("Reloading database configuration")
	if err := p.WriteConf(); err != nil {
		return fmt.Errorf("error writing conf file: %v", err)
	}
	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "reload", "-D", p.dataDir, "-o", "-c unix_socket_directories=/tmp")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("error: %v, output: %s", err, string(out))
	}
	return nil
}

func (p *Manager) Restart(fast bool) error {
	log.Infof("Restarting database")
	if err := p.Stop(fast); err != nil {
		return err
	}
	if err := p.Start(); err != nil {
		return err
	}
	return nil
}

func (p *Manager) Promote() error {
	log.Infof("Promoting database")
	name := filepath.Join(p.pgBinPath, "pg_ctl")
	cmd := exec.Command(name, "promote", "-w", "-D", p.dataDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("error: %v, output: %s", err, string(out))
	}
	return nil
}

func (p *Manager) SetupRoles() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeout)
	defer cancel()

	if p.suUsername == p.replUsername {
		log.Infof("Adding replication role to superuser")
		if err := AlterRole(ctx, p.localConnString, []string{"replication"}, p.suUsername, p.suPassword); err != nil {
			return fmt.Errorf("error adding replication role to superuser: %v", err)
		}
		log.Debugf("replication role added to superuser")
	} else {
		// Configure superuser role password
		if p.suPassword != "" {
			log.Infof("Defining superuser password")
			if err := SetPassword(ctx, p.localConnString, p.suUsername, p.suPassword); err != nil {
				return fmt.Errorf("error setting superuser password: %v", err)
			}
			log.Debugf("superuser password defined")
		}
		roles := []string{"login", "replication"}
		log.Infof("Creating replication role")
		if err := CreateRole(ctx, p.localConnString, roles, p.replUsername, p.replPassword); err != nil {
			return fmt.Errorf("error creating replication role: %v", err)
		}
		log.Debugf("replication role %s created", p.replUsername)
	}
	return nil
}

func (p *Manager) CreateReplicationLagFunction() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeout)
	defer cancel()
	return ReplicationLagFunction(ctx, p.localConnString)
}

func (p *Manager) GetReplicationSlots() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeout)
	defer cancel()
	return GetReplicationSlots(ctx, p.localConnString)
}

func (p *Manager) CreateReplicationSlot(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeout)
	defer cancel()
	return CreateReplicationSlot(ctx, p.localConnString, name)
}

func (p *Manager) DropReplicationSlot(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeout)
	defer cancel()
	return DropReplicationSlot(ctx, p.localConnString, name)
}

func (p *Manager) IsInitialized() (bool, error) {
	// TODO improve checks
	dir, err := os.Open(p.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	n, err := dir.Readdirnames(1)
	if err != nil && err != io.EOF {
		return false, err
	}
	if len(n) > 0 {
		return true, nil
	}
	return false, nil
}

func (p *Manager) GetRole() (common.Role, error) {
	curConnParams, err := p.GetPrimaryConninfo()
	if err != nil {
		return 0, fmt.Errorf("error retrieving primary conn info: %v", err)
	}
	if curConnParams == nil {
		return common.MasterRole, nil
	}
	return common.StandbyRole, nil
}

func (p *Manager) GetPrimaryConninfo() (ConnParams, error) {
	regex, err := regexp.Compile(`\s*primary_conninfo\s*=\s*'(.*)'$`)
	if err != nil {
		return nil, err
	}
	fh, err := os.Open(filepath.Join(p.dataDir, "recovery.conf"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		m := regex.FindStringSubmatch(scanner.Text())
		if len(m) == 2 {
			return ParseConnString(m[1])
		}
	}
	return nil, nil
}

func (p *Manager) HasConnString() (bool, error) {
	regex, err := regexp.Compile(`primary_conninfo`)
	if err != nil {
		return false, err
	}
	fh, err := os.Open(filepath.Join(p.dataDir, "recovery.conf"))
	if os.IsNotExist(err) {
		return false, nil
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		if regex.MatchString(scanner.Text()) {
			return true, nil
		}
	}
	return false, nil
}

func (p *Manager) WriteConf() error {
	f, err := ioutil.TempFile(p.dataDir, "postgresql.conf")
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString("include 'postgresql-base.conf'\n")
	if p.confDir != "" {
		f.WriteString(fmt.Sprintf("include_dir '%s'\n", p.confDir))
	} else {
		f.WriteString("include_dir 'conf.d'\n")
	}
	for k, v := range p.parameters {
		// Single quotes needs to be doubled
		ev := strings.Replace(v, `'`, `''`, -1)
		_, err = f.WriteString(fmt.Sprintf("%s = '%s'\n", k, ev))
		if err != nil {
			os.Remove(f.Name())
			return err
		}
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(p.dataDir, "postgresql.conf")); err != nil {
		os.Remove(f.Name())
		return err
	}

	return nil
}

func (p *Manager) WriteRecoveryConf(followedConnParams ConnParams) error {
	f, err := ioutil.TempFile(p.dataDir, "recovery.conf")
	if err != nil {
		return err
	}
	defer f.Close()

	f.WriteString("standby_mode = 'on'\n")
	f.WriteString(fmt.Sprintf("primary_slot_name = '%s'\n", p.name))
	f.WriteString("recovery_target_timeline = 'latest'\n")

	if followedConnParams != nil {
		f.WriteString(fmt.Sprintf("primary_conninfo = '%s'", followedConnParams.ConnString()))
	}
	if err = f.Sync(); err != nil {
		return err
	}

	if err = os.Rename(f.Name(), filepath.Join(p.dataDir, "recovery.conf")); err != nil {
		os.Remove(f.Name())
		return err
	}
	return nil
}

func (p *Manager) writePgHba() error {
	contents := fmt.Sprintf(`local all all  trust
host all all 127.0.0.1/32 trust
host all all ::1/128 trust
hostssl all all 0.0.0.0/0 md5
hostssl all all ::0/0 md5
hostssl replication %[1]s 0.0.0.0/0 md5
hostssl replication %[1]s ::0/0 md5`, p.replUsername)

	return common.WriteFileAtomic(filepath.Join(p.dataDir, "pg_hba.conf"), []byte(contents), 0600)
}

func (p *Manager) SyncFromFollowedPGRewind(followedConnParams ConnParams, password string) error {
	// ioutil.Tempfile already creates files with 0600 permissions
	pgpass, err := ioutil.TempFile("", "pgpass")
	if err != nil {
		return err
	}
	defer os.Remove(pgpass.Name())
	defer pgpass.Close()

	host := followedConnParams.Get("host")
	port := followedConnParams.Get("port")
	user := followedConnParams.Get("user")
	pgpass.WriteString(fmt.Sprintf("%s:%s:*:%s:%s\n", host, port, user, password))

	// Disable syncronous replication. pg_rewind needs to create a
	// temporary table on the master but if synchronous replication is
	// enabled and there're no active standbys it will hang.
	followedConnParams.Set("options", "-c synchronous_commit=off")
	followedConnString := followedConnParams.ConnString()

	log.Infof("Running pg_rewind")
	name := filepath.Join(p.pgBinPath, "pg_rewind")
	cmd := exec.Command(name, "--debug", "-D", p.dataDir, "--source-server="+followedConnString)
	cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSFILE=%s", pgpass.Name()))
	log.Debugf("execing cmd: %s", cmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("error: %v, output: %s", err, string(out))
	}
	return nil
}

func (p *Manager) SyncFromFollowed(followedConnParams ConnParams) error {
	// ioutil.Tempfile already creates files with 0600 permissions
	pgpass, err := ioutil.TempFile("", "pgpass")
	if err != nil {
		return err
	}
	defer os.Remove(pgpass.Name())
	defer pgpass.Close()

	host := followedConnParams.Get("host")
	port := followedConnParams.Get("port")
	user := followedConnParams.Get("user")
	password := followedConnParams.Get("password")
	pgpass.WriteString(fmt.Sprintf("%s:%s:*:%s:%s\n", host, port, user, password))

	log.Infof("Running pg_basebackup")
	name := filepath.Join(p.pgBinPath, "pg_basebackup")
	cmd := exec.Command(name, "-R", "-D", p.dataDir, "--host="+host, "--port="+port, "-U", user)
	cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSFILE=%s", pgpass.Name()))
	log.Debugf("execing cmd: %s", cmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("error: %v, output: %s", err, string(out))
	}
	return nil
}

func (p *Manager) RemoveAll() error {
	initialized, err := p.IsInitialized()
	if err != nil {
		return fmt.Errorf("failed to retrieve instance state: %v", err)
	}
	started := false
	if initialized {
		var err error
		started, err = p.IsStarted()
		if err != nil {
			return fmt.Errorf("failed to retrieve instance state: %v", err)
		}
	}
	if started {
		return fmt.Errorf("cannot remove postregsql database. Instance is active")
	}
	return os.RemoveAll(p.dataDir)
}

// IsStreaming returns error if the PostgreSQL is not streaming WALs from master
func (p *Manager) IsStreaming() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeout)
	defer cancel()

	return isStreaming(ctx, p.localConnString)
}

// ping checks availability of a PostgreSQL instance
func (p *Manager) ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.requestTimeout)
	defer cancel()
	return CheckDBStatus(ctx, p.localConnString)
}

func logOutput(r io.Reader, wg *sync.WaitGroup, pipeName string) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		log.Infof("pg_ctl command %v: %s", pipeName, scanner.Text())
	}
	wg.Done()
}
