/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	istorage "github.com/heeus/core-istorage"
	istructs "github.com/heeus/core-istructs"
)

// AppStorageProvider s.e.
type AppStorageProvider func(apps map[istructs.AppName]CassandraParams) istorage.IAppStorageProvider

// CassandraParams s.e.
type CassandraParams struct {
	// Comma separated list of hosts
	Hosts             string
	Port              int
	Keyspace          string
	ReplicationFactor int
	Username          string
	Pwd               string
}
