package main

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"strings"

	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/stolon/pkg/postgresql"
	"github.com/gravitational/stolon/pkg/store"
	"github.com/gravitational/trace"
	"github.com/kelseyhightower/envconfig"
)

var (
	ErrCantParseConfig = errors.New("Can't parse config")
)

type Config struct {
	LogLevel          string `envconfig:"STOLONRPC_LOG_LEVEL"`
	Port              string `envconfig:"STOLONRPC_PORT"`
	DatabaseHost      string `envconfig:"STOLONRPC_DB_HOST"`
	DatabasePort      string `envconfig:"STOLONRPC_DB_PORT"`
	DatabaseUsername  string `envconfig:"STOLONRPC_DB_USERNAME"`
	S3AccessKeyID     string `envconfig:"STOLONRPC_S3_ACCESS_KEY_ID"`
	S3SecretAccessKey string `envconfig:"STOLONRPC_S3_SECRET_ACCESS_KEY"`
}

func GetConfig() (*Config, error) {
	var config Config
	if err := envconfig.Process("", &config); err != nil {
		return nil, trace.Wrap(ErrCantParseConfig)
	}
	return &config, nil
}

func setupLogging(level string) error {
	lvl := strings.ToLower(level)

	if lvl == "debug" {
		trace.SetDebug(true)
	}

	sev, err := log.ParseLevel(lvl)
	if err != nil {
		return err
	}
	log.SetLevel(sev)
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	return nil
}

func main() {
	c, err := GetConfig()
	if err != nil {
		trace.Wrap(err)
	}

	if err = setupLogging(c.LogLevel); err != nil {
		trace.Wrap(err)
	}
	log.Infof("Start with config: %+v", c)

	op := new(DatabaseOperation)
	dbConn := postgresql.ConnSettings{
		Host:     c.DatabaseHost,
		Port:     c.DatabasePort,
		Username: c.DatabaseUsername,
	}
	op.dbConn = dbConn
	s3Cred := store.S3Credentials{
		AccessKeyID:     c.S3AccessKeyID,
		SecretAccessKey: c.S3SecretAccessKey,
	}
	op.s3Cred = s3Cred
	rpc.Register(op)
	rpc.HandleHTTP()

	errChan := make(chan error, 1)
	go func() {
		errChan <- http.ListenAndServe(net.JoinHostPort("", c.Port), nil)
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case err := <-errChan:
			if err != nil {
				log.Fatal(err)
			}
		case s := <-signalChan:
			log.Infof("Captured %s. Exiting...", s)
			os.Exit(0)
		}
	}
}
