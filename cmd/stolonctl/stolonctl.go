package main

import (
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/gravitational/stolon/cmd/stolonctl/client"
	"github.com/gravitational/stolon/cmd/stolonctl/cluster"
	"github.com/gravitational/stolon/cmd/stolonctl/database"
	"github.com/gravitational/stolon/pkg/util"
	"github.com/gravitational/trace"

	log "github.com/Sirupsen/logrus"
)

const (
	EnvStoreEndpoints    = "STOLONCTL_STORE_ENDPOINTS"
	EnvStoreBackend      = "STOLONCTL_STORE_BACKEND"
	EnvStoreKey          = "STOLONCTL_STORE_KEY"
	EnvStoreCACert       = "STOLONCTL_STORE_CA_CERT"
	EnvStoreCert         = "STOLONCTL_STORE_CERT"
	EnvDatabaseHost      = "STOLONCTL_DB_HOST"
	EnvDatabasePort      = "STOLONCTL_DB_PORT"
	EnvDatabaseUsername  = "STOLONCTL_DB_USERNAME"
	EnvS3AccessKeyID     = "STOLONCTL_S3_ACCESS_KEY_ID"
	EnvS3SecretAccessKey = "STOLONCTL_S3_SECRET_ACCESS_KEY"
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
	cmdDatabase := app.Command("db", "database operations")

	// backup
	cmdDatabaseBackup := cmdDatabase.Command("backup", "backup database")
	cmdDatabaseDatabaseName := cmdDatabaseBackup.Arg("database-name", "database name").Required().String()
	cmdDatabaseBackupLocation := cmdDatabaseBackup.Arg("path", "path to store backup").Required().String()

	var conn database.ConnSettings
	var s3 database.S3Settings
	cmdDatabaseBackup.Flag("host", "database server host").Default("localhost").Envar(EnvDatabaseHost).StringVar(&conn.Host)
	cmdDatabaseBackup.Flag("port", "database server port").Default("5432").Envar(EnvDatabasePort).StringVar(&conn.Port)
	cmdDatabaseBackup.Flag("username", "database user name").Default("postgres").Envar(EnvDatabaseUsername).StringVar(&conn.Username)
	cmdDatabaseBackup.Flag("access-key", "S3 access key ID").Envar(EnvS3AccessKeyID).StringVar(&s3.AccessKeyID)
	cmdDatabaseBackup.Flag("secret-key", "S3 secret access key").Envar(EnvS3SecretAccessKey).StringVar(&s3.SecretAccessKey)

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		return trace.Wrap(err)
	}

	switch cmd {
	case cmdDatabaseBackup.FullCommand():
		return database.Backup(conn, s3, *cmdDatabaseDatabaseName, *cmdDatabaseBackupLocation)
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
