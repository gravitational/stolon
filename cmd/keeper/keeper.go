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

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gravitational/stolon/common"
	"github.com/gravitational/stolon/pkg/cluster"
	"github.com/gravitational/stolon/pkg/flagutil"
	"github.com/gravitational/stolon/pkg/kubernetes"
	"github.com/gravitational/stolon/pkg/postgresql"
	pg "github.com/gravitational/stolon/pkg/postgresql"
	"github.com/gravitational/stolon/pkg/store"
	"github.com/gravitational/stolon/pkg/util"
	"github.com/gravitational/trace"

	"github.com/coreos/pkg/capnslog"
	"github.com/coreos/rkt/pkg/lock"
	"github.com/davecgh/go-spew/spew"
	uuid "github.com/satori/go.uuid"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

var log = capnslog.NewPackageLogger("github.com/gravitational/stolon/cmd", "keeper")

func init() {
	capnslog.SetFormatter(capnslog.NewPrettyFormatter(os.Stderr, true))
	capnslog.SetGlobalLogLevel(capnslog.DEBUG)
}

var cmdKeeper = &cobra.Command{
	Use: "stolon-keeper",
	Run: keeper,
}

type config struct {
	id                      string
	storeBackend            string
	storeEndpoints          string
	storeCertFile           string
	storeKeyFile            string
	storeCACertFile         string
	dataDir                 string
	clusterName             string
	listenAddress           string
	port                    string
	debug                   bool
	pgListenAddress         string
	pgPort                  string
	pgBinPath               string
	pgConfDir               string
	pgReplUsername          string
	pgReplPassword          string
	pgReplPasswordFile      string
	pgSUUsername            string
	pgSUPassword            string
	pgSUPasswordFile        string
	pgSSLReplication        bool
	pgSSLCertFile           string
	pgSSLKeyFile            string
	pgSSLCAFile             string
	pgSSLCiphers            string
	pgInitialSUUsername     string
	pgInitialSUPasswordFile string
}

var cfg config

func init() {
	user, err := util.GetUser()
	if err != nil {
		log.Fatalf("%v", err)
	}

	cmdKeeper.PersistentFlags().StringVar(&cfg.id, "id", "", "keeper id (must be unique in the cluster and can contain only lower-case letters, numbers and the underscore character). If not provided a random id will be generated.")
	cmdKeeper.PersistentFlags().StringVar(&cfg.storeBackend, "store-backend", "", "store backend type (etcd or consul)")
	cmdKeeper.PersistentFlags().StringVar(&cfg.storeEndpoints, "store-endpoints", "", "a comma-delimited list of store endpoints (defaults: 127.0.0.1:2379 for etcd, 127.0.0.1:8500 for consul)")
	cmdKeeper.PersistentFlags().StringVar(&cfg.storeCertFile, "store-cert", "", "path to the client server TLS cert file")
	cmdKeeper.PersistentFlags().StringVar(&cfg.storeKeyFile, "store-key", "", "path to the client server TLS key file")
	cmdKeeper.PersistentFlags().StringVar(&cfg.storeCACertFile, "store-cacert", "", "path to the client server TLS trusted CA key file")
	cmdKeeper.PersistentFlags().StringVar(&cfg.dataDir, "data-dir", "", "data directory")
	cmdKeeper.PersistentFlags().StringVar(&cfg.clusterName, "cluster-name", "", "cluster name")
	cmdKeeper.PersistentFlags().StringVar(&cfg.listenAddress, "listen-address", "localhost", "keeper listening address")
	cmdKeeper.PersistentFlags().StringVar(&cfg.port, "port", "5431", "keeper listening port")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgListenAddress, "pg-listen-address", "localhost", "postgresql instance listening address")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgPort, "pg-port", "5432", "postgresql instance listening port")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgBinPath, "pg-bin-path", "", "absolute path to postgresql binaries. If empty they will be searched in the current PATH")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgConfDir, "pg-conf-dir", "", "absolute path to user provided postgres configuration. If empty a default dir under $dataDir/postgres/conf.d will be used")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgReplUsername, "pg-repl-username", "", "postgres replication user name. Required. It'll be created on db initialization. Requires --pg-repl-password or --pg-repl-passwordfile. Must be the same for all keepers.")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgReplPassword, "pg-repl-password", "", "postgres replication user password. Required. Only one of --pg-repl-password or --pg-repl-passwordfile is required. Must be the same for all keepers.")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgReplPasswordFile, "pg-repl-passwordfile", "", "postgres replication user password file. Only one of --pg-repl-password or --pg-repl-passwordfile is required. Must be the same for all keepers.")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgSUUsername, "pg-su-username", user, "postgres superuser user name. Used for keeper managed instance access and pg_rewind based synchronization. It'll be created on db initialization. Defaults to the name of the effective user running stolon-keeper. Must be the same for all keepers.")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgSUPassword, "pg-su-password", "", "postgres superuser password. Needed for pg_rewind based synchronization. If provided it'll be configured on db initialization. Only one of --pg-su-password or --pg-su-passwordfile is required. Must be the same for all keepers.")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgSUPasswordFile, "pg-su-passwordfile", "", "postgres superuser password file. Requires --pg-su-username. Must be the same for all keepers)")
	cmdKeeper.PersistentFlags().BoolVar(&cfg.pgSSLReplication, "pg-ssl-replication", false, "enable SSL replication")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgSSLCertFile, "pg-ssl-cert-file", "", "postgres SSL certificate file")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgSSLKeyFile, "pg-ssl-key-file", "", "postgres SSL private key")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgSSLCAFile, "pg-ssl-ca-file", "", "postgres SSL certificate authority file")
	cmdKeeper.PersistentFlags().StringVar(&cfg.pgSSLCiphers, "pg-ssl-ciphers", "", "postgres SSL allowed cipers list")
	cmdKeeper.PersistentFlags().BoolVar(&cfg.debug, "debug", false, "enable debug logging")
}

var defaultPGParameters = pg.Parameters{
	"unix_socket_directories": "/tmp",
	"wal_level":               "hot_standby",
	"wal_keep_segments":       "128",
	"hot_standby":             "on",
	"max_connections":         "500",
}

func readPasswordFromFile(filepath string) (string, error) {
	fi, err := os.Lstat(filepath)
	if err != nil {
		return "", fmt.Errorf("unable to read password from file %s: %v", filepath, err)
	}

	if fi.Mode() > 0600 {
		//TODO: enforce this by exiting with an error. Kubernetes makes this file too open today.
		log.Warningf("password file %s permissions %#o are too open. This file should only be readable to the user executing stolon! Continuing...", filepath, fi.Mode())
	}

	pwBytes, err := ioutil.ReadFile(filepath)
	if err != nil {
		return "", fmt.Errorf("unable to read password from file %s: %v", filepath, err)
	}
	return strings.TrimSpace(string(pwBytes)), nil
}

func (p *PostgresKeeper) getSslmode() string {
	if p.pgSSLReplication {
		return "require"
	}
	return "disable"
}

func (p *PostgresKeeper) getSUConnParams(keeperState *cluster.KeeperState) pg.ConnParams {
	cp := pg.ConnParams{
		"user":             p.pgSUUsername,
		"password":         p.pgSUPassword,
		"host":             keeperState.PGListenAddress,
		"port":             keeperState.PGPort,
		"application_name": p.id,
		"dbname":           "postgres",
		"sslmode":          p.getSslmode(),
	}
	return cp
}

func (p *PostgresKeeper) getReplConnParams(keeperState *cluster.KeeperState) pg.ConnParams {
	return pg.ConnParams{
		"user":             p.pgReplUsername,
		"password":         p.pgReplPassword,
		"host":             keeperState.PGListenAddress,
		"port":             keeperState.PGPort,
		"application_name": p.id,
		"sslmode":          p.getSslmode(),
	}
}

func (p *PostgresKeeper) getLocalConnParams() pg.ConnParams {
	return pg.ConnParams{
		"user": p.pgSUUsername,
		// Do not set password since we assume that pg_hba.conf trusts local users
		"host":    "localhost",
		"port":    p.pgPort,
		"dbname":  "postgres",
		"sslmode": p.getSslmode(),
	}
}

func (p *PostgresKeeper) getOurReplConnParams() pg.ConnParams {
	return pg.ConnParams{
		"user":     p.pgReplUsername,
		"password": p.pgReplPassword,
		"host":     p.pgListenAddress,
		"port":     p.pgPort,
		"sslmode":  p.getSslmode(),
	}
}

func (p *PostgresKeeper) createPGParameters(followersIDs []string) pg.Parameters {
	pgParameters := p.clusterConfig.PGParameters

	// Merge default PGParameters
	for k, v := range defaultPGParameters.Copy() {
		pgParameters[k] = v
	}

	pgParameters["listen_addresses"] = fmt.Sprintf("127.0.0.1,%s", p.pgListenAddress)
	pgParameters["port"] = p.pgPort
	pgParameters["max_replication_slots"] = strconv.FormatUint(uint64(p.clusterConfig.MaxStandbysPerSender), 10)
	// Add some more wal senders, since also the keeper will use them
	pgParameters["max_wal_senders"] = strconv.FormatUint(uint64(p.clusterConfig.MaxStandbysPerSender+2), 10)

	// required by pg_rewind
	// if database has data checksum enabled it's ignored
	if p.clusterConfig.UsePGRewind {
		pgParameters["wal_log_hints"] = "on"
	}

	// Setup synchronous replication
	if p.clusterConfig.SynchronousReplication {
		pgParameters["synchronous_standby_names"] = strings.Join(followersIDs, ",")
	} else {
		pgParameters["synchronous_standby_names"] = ""
	}

	if p.pgSSLReplication {
		pgParameters["ssl"] = "on"
	} else {
		pgParameters["ssl"] = "off"
	}

	if p.pgSSLCertFile != "" {
		pgParameters["ssl_cert_file"] = p.pgSSLCertFile
	}

	if p.pgSSLKeyFile != "" {
		pgParameters["ssl_key_file"] = p.pgSSLKeyFile
	}

	if p.pgSSLCertFile != "" {
		pgParameters["ssl_ca_file"] = p.pgSSLCAFile
	}

	if p.pgSSLCiphers != "" {
		pgParameters["ssl_ciphers"] = p.pgSSLCiphers
	}

	return pgParameters
}

type PostgresKeeper struct {
	cfg *config

	id                  string
	dataDir             string
	storeBackend        string
	storeEndpoints      string
	listenAddress       string
	port                string
	debug               bool
	pgListenAddress     string
	pgPort              string
	pgBinPath           string
	pgConfDir           string
	pgReplUsername      string
	pgReplPassword      string
	pgSUUsername        string
	pgSUPassword        string
	pgSSLReplication    bool
	pgSSLCertFile       string
	pgSSLKeyFile        string
	pgSSLCAFile         string
	pgSSLCiphers        string
	pgInitialSUUsername string

	e    *store.StoreManager
	pgm  *postgresql.Manager
	stop chan bool
	end  chan error

	clusterConfig *cluster.Config

	cvVersionMutex sync.Mutex
	cvVersion      int

	pgStateMutex    sync.Mutex
	getPGStateMutex sync.Mutex
	lastPGState     *cluster.PostgresState
}

func NewPostgresKeeper(id string, cfg *config, stop chan bool, end chan error) (*PostgresKeeper, error) {
	storePath := filepath.Join(common.StoreBasePath, cfg.clusterName)

	kvstore, err := store.NewStore(
		store.Backend(cfg.storeBackend),
		cfg.storeEndpoints,
		cfg.storeCertFile,
		cfg.storeKeyFile,
		cfg.storeCACertFile,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot create store: %v", err)
	}
	e := store.NewStoreManager(kvstore, storePath)

	p := &PostgresKeeper{
		cfg: cfg,

		id:             id,
		dataDir:        cfg.dataDir,
		storeBackend:   cfg.storeBackend,
		storeEndpoints: cfg.storeEndpoints,

		listenAddress:       cfg.listenAddress,
		port:                cfg.port,
		debug:               cfg.debug,
		pgListenAddress:     cfg.pgListenAddress,
		pgPort:              cfg.pgPort,
		pgBinPath:           cfg.pgBinPath,
		pgConfDir:           cfg.pgConfDir,
		pgReplUsername:      cfg.pgReplUsername,
		pgReplPassword:      cfg.pgReplPassword,
		pgSUUsername:        cfg.pgSUUsername,
		pgSUPassword:        cfg.pgSUPassword,
		pgInitialSUUsername: cfg.pgInitialSUUsername,

		pgSSLReplication: cfg.pgSSLReplication,
		pgSSLCertFile:    cfg.pgSSLCertFile,
		pgSSLKeyFile:     cfg.pgSSLKeyFile,
		pgSSLCAFile:      cfg.pgSSLCAFile,
		pgSSLCiphers:     cfg.pgSSLCiphers,

		e:    e,
		stop: stop,
		end:  end,
	}

	return p, nil
}

func (p *PostgresKeeper) usePGRewind() bool {
	return p.pgSUUsername != "" && p.pgSUPassword != "" && p.clusterConfig.UsePGRewind
}

func (p *PostgresKeeper) publish() error {
	discoveryInfo := &cluster.KeeperDiscoveryInfo{
		ListenAddress: p.listenAddress,
		Port:          p.port,
	}
	log.Debugf(spew.Sprintf("discoveryInfo: %#v", discoveryInfo))

	if err := p.e.SetKeeperDiscoveryInfo(p.id, discoveryInfo, p.clusterConfig.SleepInterval*2); err != nil {
		return err
	}
	return nil
}

func (p *PostgresKeeper) saveCVVersion(version int) error {
	p.cvVersionMutex.Lock()
	defer p.cvVersionMutex.Unlock()
	p.cvVersion = version
	return common.WriteFileAtomic(filepath.Join(p.dataDir, "cvversion"), []byte(strconv.Itoa(version)), 0600)
}

func (p *PostgresKeeper) loadCVVersion() error {
	p.cvVersionMutex.Lock()
	defer p.cvVersionMutex.Unlock()
	cvVersion, err := ioutil.ReadFile(filepath.Join(p.dataDir, "cvversion"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	p.cvVersion, err = strconv.Atoi(string(cvVersion))
	return err
}

func (p *PostgresKeeper) infoHandler(w http.ResponseWriter, req *http.Request) {
	p.cvVersionMutex.Lock()
	defer p.cvVersionMutex.Unlock()

	keeperInfo := cluster.KeeperInfo{
		ID:                 p.id,
		ClusterViewVersion: p.cvVersion,
		ListenAddress:      p.listenAddress,
		Port:               p.port,
		PGListenAddress:    p.pgListenAddress,
		PGPort:             p.pgPort,
	}

	if err := json.NewEncoder(w).Encode(&keeperInfo); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (p *PostgresKeeper) pgStateHandler(w http.ResponseWriter, req *http.Request) {
	pgState := p.getLastPGState()

	if pgState == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(pgState); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (p *PostgresKeeper) updatePGState(pctx context.Context) {
	p.pgStateMutex.Lock()
	defer p.pgStateMutex.Unlock()
	pgState, err := p.GetPGState(pctx)
	if err != nil {
		log.Errorf("error getting pgstate: %v", err)
		return
	}
	log.Debugf("keeperpgState: %v", pgState)
	p.lastPGState = pgState
}

func (p *PostgresKeeper) GetPGState(pctx context.Context) (*cluster.PostgresState, error) {
	p.getPGStateMutex.Lock()
	defer p.getPGStateMutex.Unlock()
	// Just get one pgstate at a time to avoid exausting available connections
	pgState := &cluster.PostgresState{}

	initialized, err := p.pgm.IsInitialized()
	if err != nil {
		return nil, err
	}
	if !initialized {
		pgState.Initialized = false
	} else {
		ctx, cancel := context.WithTimeout(pctx, p.clusterConfig.RequestTimeout)
		pgState, err = pg.GetPGState(ctx, p.getOurReplConnParams())
		defer cancel()
		if err != nil {
			return nil, trace.Wrap(err, "error getting pg state")
		}

		ctx, cancel = context.WithTimeout(pctx, p.clusterConfig.RequestTimeout)
		replicationLag, err := pg.GetReplicationLag(ctx, p.getLocalConnParams())
		defer cancel()
		if err != nil {
			return nil, trace.Wrap(err, "error getting replication lag")
		}
		pgState.ReplicationLag = replicationLag

		ctx, cancel = context.WithTimeout(pctx, p.clusterConfig.RequestTimeout)
		role, err := pg.GetRole(ctx, p.getLocalConnParams())
		defer cancel()
		if err != nil {
			return nil, trace.Wrap(err, "error getting role")
		}
		pgState.Role = role

		pgState.Initialized = true

		// if timeline <= 1 then no timeline history file exists.
		pgState.TimelinesHistory = cluster.PostgresTimeLinesHistory{}
		if pgState.TimelineID > 1 {
			ctx, cancel = context.WithTimeout(pctx, p.clusterConfig.RequestTimeout)
			tlsh, err := pg.GetTimelinesHistory(ctx, pgState.TimelineID, p.getOurReplConnParams())
			defer cancel()
			if err != nil {
				return nil, trace.Wrap(err, "error getting timeline history")
			}
			pgState.TimelinesHistory = tlsh
		}
	}

	return pgState, nil
}

func (p *PostgresKeeper) getLastPGState() *cluster.PostgresState {
	p.pgStateMutex.Lock()
	pgState := p.lastPGState.Copy()
	p.pgStateMutex.Unlock()
	return pgState
}

func (p *PostgresKeeper) Start() {
	endSMCh := make(chan struct{})
	endPgStatecheckerCh := make(chan struct{})
	endPublish := make(chan struct{})
	endApiCh := make(chan error)

	var err error
	var cd *cluster.ClusterData
	// TODO(sgotti) make the postgres manager stateless and instantiate a
	// new one at every check loop, this will avoid the need to loop here
	// to get the clusterconfig
	for {
		cd, _, err = p.e.GetClusterData()
		if err == nil {
			break
		}
		log.Errorf("error retrieving cluster data: %v", err)
		time.Sleep(cluster.DefaultSleepInterval)
	}

	var cv *cluster.ClusterView
	if cd == nil {
		cv = cluster.NewClusterView()
	} else {
		cv = cd.ClusterView
	}
	log.Debugf(spew.Sprintf("clusterView: %#v", cv))

	p.clusterConfig = cv.Config.ToConfig()
	log.Debugf(spew.Sprintf("clusterConfig: %#v", p.clusterConfig))

	if err := p.loadCVVersion(); err != nil {
		p.end <- fmt.Errorf("failed to load cluster version file: %v", err)
		return
	}

	// TODO(sgotti) reconfigure the various configurations options
	// (RequestTimeout) after a changed cluster config
	followersIDs := cv.GetFollowersIDs(p.id)
	pgParameters := p.createPGParameters(followersIDs)
	pgm := postgresql.NewManager(p.id, p.pgBinPath, p.dataDir, p.pgConfDir, pgParameters, p.getLocalConnParams().ConnString(), p.getOurReplConnParams().ConnString(), p.pgSUUsername, p.pgSUPassword, p.pgReplUsername, p.pgReplPassword, p.clusterConfig.RequestTimeout)
	p.pgm = pgm

	p.pgm.Stop(false)

	http.HandleFunc("/info", p.infoHandler)
	http.HandleFunc("/pgstate", p.pgStateHandler)
	go func() {
		endApiCh <- http.ListenAndServe(fmt.Sprintf("%s:%s", p.listenAddress, p.port), nil)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	smTimerCh := time.NewTimer(0).C
	updatePGStateTimerCh := time.NewTimer(0).C
	publishCh := time.NewTimer(0).C
	exitSignals := make(chan os.Signal, 1)
	signal.Notify(exitSignals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)

	for {
		select {
		case sig := <-exitSignals:
			log.Infof("Caught signal: %v. Stopping stolon keeper.", sig)
			cancel()
			p.pgm.Stop(false)
			close(p.end)
			return
		case <-p.stop:
			log.Info("Stopping stolon keeper.")
			cancel()
			p.pgm.Stop(false)
			close(p.end)
			return

		case <-smTimerCh:
			go func() {
				p.postgresKeeperSM(ctx)
				select {
				case endSMCh <- struct{}{}:
				case <-ctx.Done():
				}
			}()

		case <-endSMCh:
			smTimerCh = time.NewTimer(p.clusterConfig.SleepInterval).C

		case <-updatePGStateTimerCh:
			go func() {
				p.updatePGState(ctx)
				select {
				case endPgStatecheckerCh <- struct{}{}:
				case <-ctx.Done():
				}
			}()

		case <-endPgStatecheckerCh:
			updatePGStateTimerCh = time.NewTimer(p.clusterConfig.SleepInterval).C

		case <-publishCh:
			go func() {
				p.publish()
				select {
				case endPublish <- struct{}{}:
				case <-ctx.Done():
				}
			}()

		case <-endPublish:
			publishCh = time.NewTimer(p.clusterConfig.SleepInterval).C

		case err := <-endApiCh:
			close(p.stop)
			if err != nil {
				log.Fatal("ListenAndServe: ", err)
			}
		}
	}
}

func (p *PostgresKeeper) resync(followed *cluster.KeeperState, initialized, started bool) error {
	pgm := p.pgm
	if initialized && started {
		if err := pgm.Stop(false); err != nil {
			return fmt.Errorf("failed to stop pg instance: %v", err)
		}
	}
	replConnParams := p.getReplConnParams(followed)

	// TODO(sgotti) Actually we don't check if pg_rewind is installed or if
	// postgresql version is > 9.5 since someone can also use an externally
	// installed pg_rewind for postgres 9.4. If a pg_rewind executable
	// doesn't exists pgm.SyncFromFollowedPGRewind will return an error and
	// fallback to pg_basebackup
	if initialized && p.usePGRewind() {
		connParams := p.getSUConnParams(followed)
		log.Infof("syncing using pg_rewind from followed instance %q", followed.ID)
		if err := pgm.SyncFromFollowedPGRewind(connParams, p.pgSUPassword); err != nil {
			// log pg_rewind error and fallback to pg_basebackup
			log.Errorf("error syncing with pg_rewind: %v", err)
		} else {
			if err := pgm.WriteRecoveryConf(replConnParams); err != nil {
				return fmt.Errorf("err: %v", err)
			}
			return nil
		}
	}

	if err := pgm.RemoveAll(); err != nil {
		return fmt.Errorf("failed to remove the postgres data dir: %v", err)
	}
	log.Infof("syncing from followed instance %q with connection params: %v", followed.ID, replConnParams)
	if err := pgm.SyncFromFollowed(replConnParams); err != nil {
		return fmt.Errorf("error: %v", err)
	}
	log.Infof("sync from followed instance %q successfully finished", followed.ID)

	if err := pgm.WriteRecoveryConf(replConnParams); err != nil {
		return fmt.Errorf("err: %v", err)
	}
	return nil
}

func (p *PostgresKeeper) isDifferentTimelineBranch(fPGState *cluster.PostgresState, pgState *cluster.PostgresState) bool {
	if fPGState.SystemID != pgState.SystemID {
		log.Infof("followed instance system ID %d different than our system ID %d", fPGState.SystemID, pgState.SystemID)
		return true
	}

	if fPGState.TimelineID < pgState.TimelineID {
		log.Infof("followed instance timeline %d < than our timeline %d", fPGState.TimelineID, pgState.TimelineID)
		return true
	}
	if fPGState.TimelineID == pgState.TimelineID {
		return false
	}

	// fPGState.TimelineID > pgState.TimelineID
	tlh := fPGState.TimelinesHistory.GetTimelineHistory(pgState.TimelineID)
	if tlh != nil {
		if tlh.SwitchPoint < pgState.XLogPos {
			log.Infof("followed instance timeline %d forked at xlog pos %d before our current state (timeline %d at xlog pos %d)", fPGState.TimelineID, tlh.SwitchPoint, pgState.TimelineID, pgState.XLogPos)
			return true
		}
	}
	return false
}

func (p *PostgresKeeper) postgresKeeperSM(pctx context.Context) {
	e := p.e
	pgm := p.pgm

	cv, _, err := e.GetClusterView()
	if err != nil {
		log.Errorf("error retrieving cluster view: %v", err)
		return
	}
	log.Debugf(spew.Sprintf("clusterView: %#v", cv))

	if cv == nil {
		log.Infof("no clusterview available, waiting for it to appear")
		return
	}

	followersIDs := cv.GetFollowersIDs(p.id)

	// Update cluster config
	clusterConfig := cv.Config.ToConfig()
	log.Debugf(spew.Sprintf("clusterConfig: %#v", clusterConfig))
	// This shouldn't need a lock
	p.clusterConfig = clusterConfig

	prevPGParameters := pgm.GetParameters()
	// create postgres parameteres
	pgParameters := p.createPGParameters(followersIDs)
	// update pgm postgres parameters
	pgm.SetParameters(pgParameters)

	keepersState, _, err := e.GetKeepersState()
	if err != nil {
		log.Errorf("err: %v", err)
		return
	}
	if keepersState == nil {
		keepersState = cluster.KeepersState{}
	}
	log.Debugf(spew.Sprintf("keepersState: %#v", keepersState))

	keeper := keepersState[p.id]
	log.Debugf(spew.Sprintf("keeperState: %#v", keeper))

	initialized, err := pgm.IsInitialized()
	if err != nil {
		log.Errorf("failed to detect if instance is initialized: %v", err)
		return
	}

	if len(cv.KeepersRole) == 0 {
		if initialized {
			err = pgm.CreateReplicationLagFunction()
			if err != nil {
				log.Errorf("failed to create replication lag function: %v", err)
				return
			}
		} else {
			log.Infof("Initializing database")
			if err = pgm.Init(); err != nil {
				log.Errorf("failed to initialize postgres instance: %v", err)
				return
			}
			initialized = true
		}

	}

	started := false

	if initialized {
		started, err = pgm.IsStarted()
		if err != nil {
			log.Errorf("failed to retrieve instance status: %v", err)
		}
	}

	log.Debugf("initialized: %t", initialized)
	log.Debugf("started: %t", started)

	role, err := pgm.GetRole()
	if err != nil {
		log.Infof("error retrieving current pg role: %v", err)
		return
	}
	isMaster := false
	if role == common.MasterRole {
		log.Infof("current pg state: master")
		isMaster = true
	} else {
		log.Infof("current pg state: standby")
	}

	masterID := cv.Master
	log.Debugf("masterID: %q", masterID)

	keeperRole, ok := cv.KeepersRole[p.id]
	if !ok {
		// No information about our role
		log.Infof("our keeper requested role is not available")
		if initialized && !started {
			if err = pgm.Start(); err != nil {
				log.Errorf("failed to start postgres: %v", err)
				return
			} else {
				started = true
			}
		}
		return
	}
	if p.id == masterID {
		// We are the elected master
		log.Infof("our cluster requested state is master")
		if !initialized {
			log.Errorf("database is not initialized. This shouldn't happen!")
			return
		}
		if !started {
			if err = pgm.Start(); err != nil {
				log.Errorf("failed to start postgres: %v", err)
				return
			} else {
				started = true
			}
		}

		if role != common.MasterRole {
			log.Infof("promoting to master")
			if err = pgm.Promote(); err != nil {
				log.Errorf("err: %v", err)
				return
			}
		} else {
			log.Infof("already master")

			var replSlots []string
			replSlots, err = pgm.GetReplicationSlots()
			if err != nil {
				log.Errorf("err: %v", err)
				return
			}
			// Create replication slots
			for _, slotName := range replSlots {
				if !util.StringInSlice(followersIDs, slotName) {
					log.Infof("dropping replication slot for keeper %q not marked as follower", slotName)
					if err = pgm.DropReplicationSlot(slotName); err != nil {
						log.Errorf("err: %v", err)
					}
				}
			}

			for _, followerID := range followersIDs {
				if followerID == p.id {
					continue
				}
				if !util.StringInSlice(replSlots, followerID) {
					if err = pgm.CreateReplicationSlot(followerID); err != nil {
						log.Errorf("err: %v", err)
					}
				}
			}

		}
	} else if keeperRole.Follow != "" {
		// We are a standby
		log.Infof("our cluster requested state is standby following %q", keeperRole.Follow)
		followed, ok := keepersState[keeperRole.Follow]
		if !ok {
			log.Errorf("no keeper state data for %q", keeperRole.Follow)
			return
		}
		if isMaster {
			if initialized {
				// There can be the possibility that this
				// database is on the same branch of the
				// current followed instance.
				// So we try to put it in recovery and then
				// check if it's on the same branch or force a
				// resync
				replConnParams := p.getReplConnParams(followed)
				if err = pgm.WriteRecoveryConf(replConnParams); err != nil {
					log.Errorf("err: %v", err)
					return
				}
				if !started {
					if err = pgm.Start(); err != nil {
						log.Errorf("err: %v", err)
						return
					} else {
						started = true
					}
				} else {
					if err = pgm.Restart(false); err != nil {
						log.Errorf("err: %v", err)
						return
					}
				}

				// Check timeline history
				// We need to update our pgState to avoid dealing with
				// an old pgState not reflecting the real state
				var pgState *cluster.PostgresState
				pgState, err = p.GetPGState(pctx)
				if err != nil {
					log.Errorf("cannot get current pgstate: %v", err)
					return
				}
				fPGState := followed.PGState

				// Check if we are on another branch
				if p.isDifferentTimelineBranch(fPGState, pgState) {
					if err = p.resync(followed, initialized, started); err != nil {
						log.Errorf("failed to full resync from followed instance: %v", err)
						return
					}
					if err = pgm.Start(); err != nil {
						log.Errorf("err: %v", err)
						return
					} else {
						started = true
					}
				}

				if err = p.resyncIfNotReady(followed, initialized, started); err != nil {
					log.Error(err)
					return
				}
				started = true
			} else {
				if err = p.resync(followed, initialized, started); err != nil {
					log.Errorf("failed to full resync from followed instance: %v", err)
					return
				}
				if err = pgm.Start(); err != nil {
					log.Errorf("err: %v", err)
					return
				} else {
					started = true
				}
			}
		} else {
			log.Infof("already standby")
			if !initialized {
				log.Errorf("database is not initialized. This shouldn't happen!")
				return
			}
			if !started {
				if err = pgm.Start(); err != nil {
					log.Errorf("failed to start postgres: %v", err)
					return
				} else {
					started = true
				}
			}
			
			// Update our primary_conninfo if replConnString changed
			var curConnParams postgresql.ConnParams

			curConnParams, err = pgm.GetPrimaryConninfo()
			if err != nil {
				log.Errorf("err: %v", err)
				return
			}
			log.Debugf(spew.Sprintf("curConnParams: %v", curConnParams))

			newConnParams := p.getReplConnParams(followed)
			log.Debugf(spew.Sprintf("newConnParams: %v", newConnParams))

			if !curConnParams.Equals(newConnParams) {
				log.Infof("followed instance connection parameters changed. Reconfiguring...")
				log.Infof("following %s with connection parameters %v", keeperRole.Follow, newConnParams)
				if err = pgm.WriteRecoveryConf(newConnParams); err != nil {
					log.Errorf("err: %v", err)
					return
				}
				if err = pgm.Restart(false); err != nil {
					log.Errorf("err: %v", err)
					return
				}
			}


			var replSlots []string
			replSlots, err = pgm.GetReplicationSlots()
			if err != nil {
				log.Errorf("err: %v", err)
				return
			}
			// Delete replication slots on standby
			for _, slotName := range replSlots {
				log.Infof("deleting replication slot for standby keeper %q", slotName)
				if err = pgm.DropReplicationSlot(slotName); err != nil {
					log.Errorf("err: %v", err)
				}
			}

			// Check that we can sync with followed instance

			// Check timeline history
			// We need to update our pgState to avoid dealing with
			// an old pgState not reflecting the real state
			var pgState *cluster.PostgresState
			pgState, err = p.GetPGState(pctx)

			if err != nil {
				log.Errorf("cannot get current pgstate: %v", err)
				return
			}
			fPGState := followed.PGState
			if p.isDifferentTimelineBranch(fPGState, pgState) {
				if err = p.resync(followed, initialized, started); err != nil {
					log.Errorf("failed to full resync from followed instance: %v", err)
					return
				}
				if err = pgm.Start(); err != nil {
					log.Errorf("err: %v", err)
					return
				} else {
					started = true
				}
			}

			// TODO(sgotti) Check that the followed instance has all the needed WAL segments

			if err = p.resyncIfNotReady(followed, initialized, started); err != nil {
				log.Error(err)
				return
			}

			started = true
		}
	}

	// Log synchronous replication changes
	prevSyncStandbyNames := prevPGParameters["synchronous_standby_names"]
	syncStandbyNames := pgParameters["synchronous_standby_names"]
	if p.clusterConfig.SynchronousReplication {
		if prevSyncStandbyNames != syncStandbyNames {
			log.Infof("needed synchronous_standby_names changed from %q to %q", prevSyncStandbyNames, syncStandbyNames)
		}
	} else {
		if prevSyncStandbyNames != "" {
			log.Infof("sync replication disabled, removing current synchronous_standby_names %q", prevSyncStandbyNames)
		}
	}

	if !pgParameters.Equals(prevPGParameters) {
		log.Infof("postgres parameters changed, reloading postgres instance")
		pgm.SetParameters(pgParameters)
		if err := pgm.Reload(); err != nil {
			log.Errorf("failed to reload postgres instance: %v", err)
		}
	} else {
		// for tests
		log.Debugf("postgres parameters not changed")
	}
	if err := p.saveCVVersion(cv.Version); err != nil {
		log.Errorf("err: %v", err)
		return
	}
}

func (p *PostgresKeeper) resyncIfNotReady(followed *cluster.KeeperState, initialized, started bool) error {
	ready, err := p.pgm.IsReady()
	if err != nil {
		return trace.Wrap(err)
	}

	if !ready || p.pgm.IsStreaming() != nil {
		log.Info("Standby is not accepting connections. Forcing a full resync.")
		if err = p.resyncAndStart(followed, initialized, started); err != nil {
			return trace.Wrap(err)
		}
	}

	return nil
}

func (p *PostgresKeeper) resyncAndStart(followed *cluster.KeeperState, initialized, started bool) error {
	if err := p.resync(followed, initialized, started); err != nil {
		return trace.Wrap(err, "failed to full resync from followed instance")
	}
	if err := p.pgm.Start(); err != nil {
		return trace.Wrap(err, "error starting PostgreSQL instance")
	}

	return nil
}

func getIDFilePath(conf config) string {
	return filepath.Join(conf.dataDir, "pgid")
}

func getIDFromFile(conf config) (string, error) {
	id, err := ioutil.ReadFile(getIDFilePath(conf))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(id), nil
}

func saveIDToFile(conf config, id string) error {
	return common.WriteFileAtomic(getIDFilePath(conf), []byte(id), 0600)
}

func sigHandler(sigs chan os.Signal, stop chan bool) {
	s := <-sigs
	log.Debugf("got signal: %s", s)
	close(stop)
}

func main() {
	flagutil.SetFlagsFromEnv(cmdKeeper.PersistentFlags(), "STKEEPER")

	cmdKeeper.Execute()
}

func keeper(cmd *cobra.Command, args []string) {
	var err error
	capnslog.SetGlobalLogLevel(capnslog.INFO)
	if cfg.debug {
		capnslog.SetGlobalLogLevel(capnslog.DEBUG)
	}
	if cfg.dataDir == "" {
		log.Fatalf("data dir required")
	}

	if cfg.clusterName == "" {
		log.Fatalf("cluster name required")
	}
	if cfg.storeBackend == "" {
		log.Fatalf("store backend type required")
	}

	if err = os.MkdirAll(cfg.dataDir, 0700); err != nil {
		log.Fatalf("error: %v", err)
	}

	if cfg.pgConfDir != "" {
		if !filepath.IsAbs(cfg.pgConfDir) {
			log.Fatalf("pg-conf-dir must be an absolute path")
		}
		var fi os.FileInfo
		fi, err = os.Stat(cfg.pgConfDir)
		if err != nil {
			log.Fatalf("cannot stat pg-conf-dir: %v", err)
		}
		if !fi.IsDir() {
			log.Fatalf("pg-conf-dir is not a directory")
		}
	}

	if cfg.pgReplUsername == "" {
		log.Fatalf("--pg-repl-username is required")
	}

	if cfg.pgReplPassword == "" && cfg.pgReplPasswordFile == "" {
		log.Fatalf("one of --pg-repl-password or --pg-repl-passwordfile is required")
	}
	if cfg.pgReplPassword != "" && cfg.pgReplPasswordFile != "" {
		log.Fatalf("only one of --pg-repl-password or --pg-repl-passwordfile must be provided")
	}
	if cfg.pgSUPassword == "" && cfg.pgSUPasswordFile == "" {
		log.Fatalf("one of --pg-su-password or --pg-su-passwordfile is required")
	}
	if cfg.pgSUPassword != "" && cfg.pgSUPasswordFile != "" {
		log.Fatalf("only one of --pg-su-password or --pg-su-passwordfile must be provided")
	}

	if cfg.pgSUUsername == cfg.pgReplUsername {
		log.Warning("superuser name and replication user name are the same. Different users are suggested.")
		if cfg.pgSUPassword != cfg.pgReplPassword {
			log.Fatalf("provided superuser name and replication user name are the same but provided passwords are different")
		}
	}

	if cfg.pgReplPasswordFile != "" {
		cfg.pgReplPassword, err = readPasswordFromFile(cfg.pgReplPasswordFile)
		if err != nil {
			log.Fatalf("cannot read pg replication user password: %v", err)
		}
	}
	if cfg.pgSUPasswordFile != "" {
		cfg.pgSUPassword, err = readPasswordFromFile(cfg.pgSUPasswordFile)
		if err != nil {
			log.Fatalf("cannot read pg superuser password: %v", err)
		}
	}

	// Take an exclusive lock on dataDir
	_, err = lock.TryExclusiveLock(cfg.dataDir, lock.Dir)
	if err != nil {
		log.Fatalf("cannot take exclusive lock on data dir %q: %v", cfg.dataDir, err)
	}

	if cfg.id != "" {
		if !pg.IsValidReplSlotName(cfg.id) {
			log.Fatalf("keeper id %q not valid. It can contain only lower-case letters, numbers and the underscore character", cfg.id)
		}
	}
	id, err := getIDFromFile(cfg)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	if id != "" && cfg.id != "" && id != cfg.id {
		log.Fatalf("saved id: %q differs from configuration id: %q", id, cfg.id)
	}
	if id == "" {
		id = cfg.id
		if cfg.id == "" {
			// Generate a new id if conf.Name is empty
			if id == "" {
				u := uuid.NewV4()
				id = fmt.Sprintf("%x", u[:4])
			}
			log.Infof("generated id: %s", id)
		}
		if err = saveIDToFile(cfg, id); err != nil {
			log.Fatalf("error: %v", err)
		}
	}

	log.Infof("id: %s", id)

	if kubernetes.OnKubernetes() {
		log.Infof("running under kubernetes.")
	}

	stop := make(chan bool, 0)
	end := make(chan error, 0)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)
	go sigHandler(sigs, stop)

	p, err := NewPostgresKeeper(id, &cfg, stop, end)
	if err != nil {
		log.Fatalf("cannot create keeper: %v", err)
	}
	go p.Start()

	if err := <-end; err != nil {
		log.Error(err.Error())
	}
}
