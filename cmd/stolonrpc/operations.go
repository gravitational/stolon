package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/gravitational/stolon/cmd/stolonctl/database"
	"github.com/gravitational/trace"
)

type Operation struct {
	database.ConnSettings
}

func (o *Operation) Create(name string, reply *string) error {
	log.Infof("Execute: create %s", name)
	err := database.Create(o.ConnSettings, name)
	if err != nil {
		return trace.Wrap(err)
	}

	*reply = "Success"
	return nil
}
