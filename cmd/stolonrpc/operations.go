package main

import (
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
	result, err := postgresql.Backup(o.dbConn, o.s3Cred, args.Name, args.Path)
	if err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = Reply(result)
	return nil
}

func (o *DatabaseOperation) Restore(r *http.Request, args *Args, reply *Reply) error {
	log.Infof("RPC: restore database from '%s'", args.Path)
	if err := postgresql.Restore(o.dbConn, o.s3Cred, args.Path); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = Reply(args.Path)
	return nil
}
