package main

import (
	"os"

	"github.com/alecthomas/kingpin"
	"github.com/gravitational/stolon/cmd/stolonctl/client"
	"github.com/gravitational/stolon/cmd/stolonctl/cluster"
	"github.com/gravitational/stolon/cmd/stolonctl/database"
	"github.com/gravitational/stolon/cmd/stolonctl/store"
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

	// database commands
	cmdDatabase := app.Command("db", "database operations")

	var dbConn database.ConnSettings
	cmdDatabase.Flag("host", "database server host").Default("localhost").Envar(EnvDatabaseHost).StringVar(&dbConn.Host)
	cmdDatabase.Flag("port", "database server port").Default("5432").Envar(EnvDatabasePort).StringVar(&dbConn.Port)
	cmdDatabase.Flag("username", "database user name").Default("postgres").Envar(EnvDatabaseUsername).StringVar(&dbConn.Username)

	var s3Cred store.S3Credentials
	cmdDatabase.Flag("access-key", "S3 access key ID").Envar(EnvS3AccessKeyID).StringVar(&s3Cred.AccessKeyID)
	cmdDatabase.Flag("secret-key", "S3 secret access key").Envar(EnvS3SecretAccessKey).StringVar(&s3Cred.SecretAccessKey)
	// create
	cmdDatabaseCreate := cmdDatabase.Command("create", "create the database")
	cmdDatabaseCreateName := cmdDatabaseCreate.Arg("name", "specifies the name of the database").Required().String()
	// delete
	cmdDatabaseDelete := cmdDatabase.Command("delete", "delete the database")
	cmdDatabaseDeleteName := cmdDatabaseDelete.Arg("name", "specifies the name of the database").Required().String()
	// run
	cmdDatabaseRun := cmdDatabase.Command("run", "run a script")
	cmdDatabaseRunFilename := cmdDatabaseRun.Arg("file", "specifies the filename of a script to run").Required().String()
	// backup
	cmdDatabaseBackup := cmdDatabase.Command("backup", "backup the database")
	cmdDatabaseBackupName := cmdDatabaseBackup.Arg("name", "specifies the name of the database").Required().String()
	cmdDatabaseBackupPath := cmdDatabaseBackup.Arg("path", "send output to the specified folder").Required().String()
	// restore
	cmdDatabaseRestore := cmdDatabase.Command("restore", "restore the database")
	cmdDatabaseRestoreFile := cmdDatabaseRestore.Arg("file", "file with SQL commands").Required().String()

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		return trace.Wrap(err)
	}

	switch cmd {
	case cmdDatabaseCreate.FullCommand():
		return database.Create(dbConn, *cmdDatabaseCreateName)
	case cmdDatabaseDelete.FullCommand():
		return database.Delete(dbConn, *cmdDatabaseDeleteName)
	case cmdDatabaseRun.FullCommand():
		return database.Run(dbConn, *cmdDatabaseRunFilename)
	case cmdDatabaseBackup.FullCommand():
		return database.Backup(dbConn, s3Cred, *cmdDatabaseBackupName, *cmdDatabaseBackupPath)
	case cmdDatabaseRestore.FullCommand():
		return database.Restore(dbConn, s3Cred, *cmdDatabaseRestoreFile)
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
