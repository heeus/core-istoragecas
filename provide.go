/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	"errors"

	istorage "github.com/untillpro/voedger/pkg/istorage"
)

func Provide(casPar CassandraParamsType) (asf istorage.IAppStorageFactory, err error) {
	if len(casPar.KeyspaceWithReplication) == 0 {
		return nil, errors.New("casPar.KeyspaceWithReplication can not be empty")
	}
	provider := newStorageProvider(casPar)
	return provider, nil
}
