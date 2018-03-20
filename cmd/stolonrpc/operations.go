package main

import (
	"net/http"

	"github.com/gravitational/stolon/pkg/postgresql"
	"github.com/gravitational/stolon/pkg/store"
	"github.com/gravitational/trace"
	log "github.com/sirupsen/logrus"
)

type DatabaseOperation struct {
	dbConn postgresql.ConnSettings
}

type Args struct {
	Name            string
	Path            string
	AccessKeyID     string
	SecretAccessKey string
}

func (a *Args) S3Credentials() store.S3Credentials {
	return store.S3Credentials{
		AccessKeyID:     a.AccessKeyID,
		SecretAccessKey: a.SecretAccessKey,
	}
}

type Reply string

func (o *DatabaseOperation) Create(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("RPC: create database '%s'", args.Name)
	if err := postgresql.Create(o.dbConn, args.Name); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = Reply("Ok")
	return nil
}

func (o *DatabaseOperation) Delete(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("RPC: delete database '%s'", args.Name)
	if err := postgresql.Delete(o.dbConn, args.Name); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = Reply(args.Name)
	return nil
}

func (o *DatabaseOperation) Backup(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("RPC: backup database '%s' to '%s'", args.Name, args.Path)
	result, err := postgresql.Backup(o.dbConn, args.S3Credentials(), args.Name, args.Path)
	if err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = Reply(result)
	return nil
}

func (o *DatabaseOperation) Restore(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("RPC: restore database from '%s'", args.Path)
	if err := postgresql.Restore(o.dbConn, args.S3Credentials(), args.Name, args.Path); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = Reply(args.Path)
	return nil
}
