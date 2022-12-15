/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	istorage "github.com/heeus/core/istorage"
	istructs "github.com/heeus/core/istructs"
)

// Provide s.e.
func Provide(casPar CassandraParamsType, apps map[istructs.AppQName]AppCassandraParamsType) (asp istorage.IAppStorageProvider, cleanup func()) {
	provider, err := newStorageProvider(casPar, apps)
	if err != nil {
		panic(err)
	}

	return provider, provider.release
}
