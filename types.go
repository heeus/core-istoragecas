/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	istorage "github.com/heeus/core-istorage"
	istructs "github.com/heeus/core-istructs"
)

// AppStorageProviderFuncType s.e.
type AppStorageProviderFuncType func(casPar CassandraParamsType, apps map[istructs.AppQName]AppCassandraParamsType) istorage.IAppStorageProvider

type CassandraParamsType struct {
	// Comma separated list of hosts
	Hosts    string
	Port     int
	Username string
	Pwd      string
}

type AppCassandraParamsType struct {
	Keyspace string
}
