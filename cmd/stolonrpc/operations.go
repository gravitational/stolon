package main

import (
	"fmt"
	"net/http"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/stolon/pkg/postgresql"
	"github.com/gravitational/stolon/pkg/store"
	"github.com/gravitational/trace"
)

type DatabaseOperation struct {
	dbConn postgresql.ConnSettings
	s3Cred store.S3Credentials
}

type Args struct {
	Name, Path string
}

type Reply struct {
	Message string
}

func (o *DatabaseOperation) Create(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("Execute: create database '%s'", args.Name)
	if err := postgresql.Create(o.dbConn, args.Name); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	reply.Message = fmt.Sprintf("Database '%s' was successfully created", args.Name)
	// log.Info(reply.Message)
	return nil
}

func (o *DatabaseOperation) Delete(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("Execute: delete database '%s'", args.Name)
	if err := postgresql.Delete(o.dbConn, args.Name); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	reply.Message = fmt.Sprintf("Database '%s' was successfully deleted", args.Name)
	log.Info(reply.Message)
	return nil
}

func (o *DatabaseOperation) Backup(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("Execute: backup database '%s' to '%s'", args.Name, args.Path)
	if err := postgresql.Backup(o.dbConn, o.s3Cred, args.Name, args.Path); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	reply.Message = fmt.Sprintf("Database '%s' was successfully backuped to '%s'", args.Name, args.Path)
	log.Info(reply.Message)
	return nil
}

func (o *DatabaseOperation) Restore(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("Execute: restore database from '%s'", args.Path)
	if err := postgresql.Restore(o.dbConn, o.s3Cred, args.Path); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	reply.Message = fmt.Sprintf("Database was successfully restored from '%s'", args.Path)
	log.Info(reply.Message)
	return nil
}
