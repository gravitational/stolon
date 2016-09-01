package main

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/stolon/pkg/postgresql"
	"github.com/gravitational/stolon/pkg/store"
	"github.com/gravitational/trace"
)

type DatabaseOperation struct {
	dbConn postgresql.ConnSettings
	s3Cred store.S3Credentials
}

type BackupArgs struct {
	Name, Path string
}

func (o *DatabaseOperation) Create(name string, reply *string) error {
	log.Infof("Execute: create database '%s'", name)
	if err := postgresql.Create(o.dbConn, name); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = fmt.Sprintf("Database '%s' was successfully created", name)
	log.Info(reply)
	return nil
}

func (o *DatabaseOperation) Delete(name string, reply *string) error {
	log.Infof("Execute: delete database '%s'", name)
	if err := postgresql.Delete(o.dbConn, name); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = fmt.Sprintf("Database '%s' was successfully deleted", name)
	log.Info(reply)
	return nil
}

func (o *DatabaseOperation) Backup(args *BackupArgs, reply *string) error {
	log.Infof("Execute: backup database '%s' to '%s'", args.Name, args.Path)
	if err := postgresql.Backup(o.dbConn, o.s3Cred, args.Name, args.Path); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = fmt.Sprintf("Database '%s' was successfully backuped to '%s'", args.Name, args.Path)
	log.Info(reply)
	return nil
}

func (o *DatabaseOperation) Restore(src string, reply *string) error {
	log.Infof("Execute: restore database from '%s'", src)
	if err := postgresql.Restore(o.dbConn, o.s3Cred, src); err != nil {
		log.Error(err)
		return trace.Wrap(err)
	}

	*reply = fmt.Sprintf("Database was successfully restored from '%s'", src)
	log.Info(reply)
	return nil
}
