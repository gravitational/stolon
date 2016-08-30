package main

import (
	"fmt"

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

	*reply = fmt.Sprintf("DB '%s' was successfully created", name)
	return nil
}

func (o *Operation) Delete(name string, reply *string) error {
	log.Infof("Execute: delete %s", name)
	err := database.Delete(o.ConnSettings, name)
	if err != nil {
		return trace.Wrap(err)
	}

	*reply = fmt.Sprintf("DB '%s' was successfully deleted", name)
	return nil
}
