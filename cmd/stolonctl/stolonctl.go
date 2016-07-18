package main

import (
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/gravitational/stolon/cmd/stolonctl/client"
	"github.com/gravitational/stolon/cmd/stolonctl/cluster"
	"github.com/gravitational/stolon/cmd/stolonctl/postgresql"
	"github.com/gravitational/stolon/pkg/util"
	"github.com/gravitational/trace"

	log "github.com/Sirupsen/logrus"
)

const (
	EnvStoreEndpoints = "STOLONCTL_STORE_ENDPOINTS"
	EnvStoreBackend   = "STOLONCTL_STORE_BACKEND"
	EnvStoreKey       = "STOLONCTL_STORE_KEY"
	EnvStoreCACert    = "STOLONCTL_STORE_CA_CERT"
	EnvStoreCert      = "STOLONCTL_STORE_CERT"
	EnvPGHost         = "STOLONCTL_PG_HOST"
	EnvPGPort         = "STOLONCTL_PG_PORT"
	EnvPGUsername     = "STOLONCTL_PG_USERNAME"
)

type application struct {
	*kingpin.Application
}

func new() *application {
	app := kingpin.New("stolonctl", "stolon command line client")

	var debug bool
	app.Flag("debug", "Enable verbose logging to stderr").
		Short('d').
		BoolVar(&debug)
	if debug {
		util.InitLoggerDebug()
	} else {
		util.InitLoggerCLI()
	}

	return &application{app}
}

func (app *application) run() error {
	// Cluster commands
	cmdCluster := app.Command("cluster", "operations on existing cluster")

	var cfg client.Config
	cmdCluster.Flag("store-endpoints", "a comma-delimited list of store endpoints (defaults: 127.0.0.1:2379 for etcd, 127.0.0.1:8500 for consul)").
		Envar(EnvStoreEndpoints).StringVar(&cfg.StoreEndpoints)
	cmdCluster.Flag("store-backend", "store backend type (etcd or consul)").
		Envar(EnvStoreBackend).StringVar(&cfg.StoreBackend)
	cmdCluster.Flag("store-cert", "path to the client server TLS cert file").
		Envar(EnvStoreCert).StringVar(&cfg.StoreCertFile)
	cmdCluster.Flag("store-key", "path to the client server TLS key file").
		Envar(EnvStoreKey).StringVar(&cfg.StoreKeyFile)
	cmdCluster.Flag("store-cacert", "path to the client server TLS trusted CA key file").
		Envar(EnvStoreCACert).StringVar(&cfg.StoreCACertFile)

	// print config
	cmdClusterConfig := cmdCluster.Command("config", "print configuration for cluster")
	cmdClusterConfigName := cmdClusterConfig.Arg("cluster-name", "cluster name").Required().String()
	// patch config
	cmdClusterPatch := cmdCluster.Command("patch", "patch configuration for cluster")
	cmdClusterPatchName := cmdClusterPatch.Arg("cluster-name", "cluster name").Required().String()
	cmdClusterPatchFile := cmdClusterPatch.Flag("file", "patch configuration for cluster").Short('f').String()
	// replace config
	cmdClusterReplace := cmdCluster.Command("replace", "replace configuration for cluster")
	cmdClusterReplaceName := cmdClusterReplace.Arg("cluster-name", "cluster name").Required().String()
	cmdClusterReplaceFile := cmdClusterReplace.Flag("file", "replace configuration for cluster").Short('f').String()
	// print status
	cmdClusterStatus := cmdCluster.Command("status", "print cluster status")
	cmdClusterStatusName := cmdClusterStatus.Arg("cluster-name", "cluster name").Required().String()
	cmdClusterStatusMasterOnly := cmdClusterStatus.Flag("master", "limit output to master only").Default("false").Bool()
	cmdClusterStatusOutputJson := cmdClusterStatus.Flag("json", "format output to json").Default("false").Bool()
	// list clusters
	cmdClusterList := cmdCluster.Command("list", "list clusters")

	// postgres commands
	cmdPG := app.Command("pg", "database operations")

	// backup
	cmdPGBackup := cmdPG.Command("backup", "backup database")
	cmdPGBackupName := cmdPGBackup.Arg("database-name", "database name").Required().String()

	var conn postgresql.ConnSettings
	cmdPGBackup.Flag("host", "database server host").Default("localhost").Envar(EnvPGHost).StringVar(&conn.Host)
	cmdPGBackup.Flag("port", "database server port").Default("5432").Envar(EnvPGPort).StringVar(&conn.Port)
	cmdPGBackup.Flag("username", "database user name").Default("postgres").Envar(EnvPGUsername).StringVar(&conn.Username)

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		return trace.Wrap(err)
	}

	switch cmd {
	case cmdPGBackup.FullCommand():
		return postgresql.Backup(conn, *cmdPGBackupName)
	}

	clt, err := client.New(cfg)
	if err != nil {
		return trace.Wrap(err)
	}
	switch cmd {
	case cmdClusterConfig.FullCommand():
		return cluster.PrintConfig(clt, *cmdClusterConfigName)
	case cmdClusterPatch.FullCommand():
		return cluster.PatchConfig(clt, *cmdClusterPatchName, *cmdClusterPatchFile, os.Args[len(os.Args)-1] == "-")
	case cmdClusterReplace.FullCommand():
		return cluster.ReplaceConfig(clt, *cmdClusterReplaceName, *cmdClusterReplaceFile, os.Args[len(os.Args)-1] == "-")
	case cmdClusterStatus.FullCommand():
		return cluster.Status(clt, *cmdClusterStatusName, *cmdClusterStatusMasterOnly, *cmdClusterStatusOutputJson)
	case cmdClusterList.FullCommand():
		return cluster.List(clt)
	}

	return nil
}

func main() {
	app := new()
	if err := app.run(); err != nil {
		log.Error(trace.DebugReport(err))
		os.Exit(1)
	}
}
