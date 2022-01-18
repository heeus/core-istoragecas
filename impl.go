/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	istorage "github.com/heeus/core-istorage"
	istructs "github.com/heeus/core-istructs"
)

type appStorageProviderType struct {
	casPar  CassandraParamsType
	cluster *gocql.ClusterConfig
	cache   map[istructs.AppQName]istorage.IAppStorage
}

func newStorageProvider(casPar CassandraParamsType, apps map[istructs.AppQName]AppCassandraParamsType) (prov *appStorageProviderType, err error) {
	provider := appStorageProviderType{
		casPar: casPar,
		cache:  make(map[istructs.AppQName]istorage.IAppStorage),
	}

	provider.cluster = gocql.NewCluster(strings.Split(casPar.Hosts, ",")...)
	if casPar.Port > 0 {
		provider.cluster.Port = casPar.Port
	}
	provider.cluster.Consistency = gocql.Quorum
	provider.cluster.Timeout = ConnectionTimeout
	provider.cluster.Authenticator = gocql.PasswordAuthenticator{Username: casPar.Username, Password: casPar.Pwd}

	for appName, appPars := range apps {
		storage, err := newStorage(provider.cluster, appPars)
		if err != nil {
			return nil, fmt.Errorf("can't create application «%s» keyspace: %w", appName, err)
		}
		provider.cache[appName] = storage
	}

	return &provider, nil
}

func (p appStorageProviderType) AppStorage(appName istructs.AppQName) (storage istorage.IAppStorage, err error) {
	storage, ok := p.cache[appName]
	if !ok {
		return nil, istructs.ErrAppNotFound
	}
	return storage, nil
}

func (p appStorageProviderType) release() {
	for _, iStorage := range p.cache {
		storage := iStorage.(*appStorageType)
		storage.session.Close()
	}
}

type appStorageType struct {
	cluster *gocql.ClusterConfig
	appPar  AppCassandraParamsType
	lock    sync.Mutex
	session *gocql.Session
}

func newStorage(cluster *gocql.ClusterConfig, appPar AppCassandraParamsType) (storage istorage.IAppStorage, err error) {

	// prepare storage tables
	tables := []struct{ name, cql string }{
		{name: "values",
			cql: `(
		 		p_key 		blob,
		 		c_col			blob,
		 		value			blob,
		 		primary key 	((p_key), c_col)
			)`},
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("can't create session: %w", err)
	}

	for _, table := range tables {
		err = doWithAttempts(Attempts, time.Second, func() error {
			return session.Query(
				fmt.Sprintf(`create table if not exists %s.%s %s`, appPar.Keyspace, table.name, table.cql)).Consistency(gocql.Quorum).Exec()
		})
		if err != nil {
			return nil, fmt.Errorf("can't create table «%s»: %w", table.name, err)
		}
	}

	return &appStorageType{
		cluster: cluster,
		appPar:  appPar,
		session: session,
	}, nil
}

func doWithAttempts(attempts int, delay time.Duration, cmd func() error) (err error) {
	for i := 0; i < attempts; i++ {
		err = cmd()
		if err == nil {
			return nil // success
		}
		time.Sleep(delay)
	}
	return err
}

func (s *appStorageType) keyspace() string {
	return s.appPar.Keyspace
}

func safeCcols(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}

func (s *appStorageType) Put(pKey []byte, cCols []byte, value []byte) (err error) {
	return s.session.Query(fmt.Sprintf("insert into %s.values (p_key, c_col, value) values (?,?,?)", s.keyspace()),
		pKey,
		safeCcols(cCols),
		value).
		Consistency(gocql.Quorum).
		Exec()
}

func scanViewQuery(ctx context.Context, q *gocql.Query, cb istorage.ReadCallback) (err error) {
	q.Consistency(gocql.Quorum)
	scanner := q.Iter().Scanner()
	closeScanner := func(err error) error {
		if scanner.Err() != nil {
			if err != nil {
				err = fmt.Errorf("%s %w", err.Error(), scanner.Err())
			} else {
				err = scanner.Err()
			}
		}
		return err
	}
	for scanner.Next() {
		clustCols := make([]byte, 0)
		viewRecord := make([]byte, 0)
		err = scanner.Scan(&clustCols, &viewRecord)
		if err != nil {
			return closeScanner(err)
		}
		err = cb(clustCols, viewRecord)
		if err != nil {
			return closeScanner(err)
		}
		if ctx.Err() != nil {
			return nil // TCK contract
		}
	}
	return closeScanner(nil)
}

func (s *appStorageType) Read(ctx context.Context, pKey []byte, startCCols, finishCCols []byte, cb istorage.ReadCallback) (err error) {
	if (len(startCCols) > 0) && (len(finishCCols) > 0) && (bytes.Compare(startCCols, finishCCols) >= 0) {
		return nil // absurd range
	}

	qText := fmt.Sprintf("select c_col, value from %s.values where p_key=?", s.keyspace())

	var q *gocql.Query
	if len(startCCols) == 0 {
		if len(finishCCols) == 0 {
			// opened range
			q = s.session.Query(qText, pKey)
		} else {
			// left-opened range
			q = s.session.Query(qText+" and c_col<?", pKey, finishCCols)
		}
	} else if len(finishCCols) == 0 {
		// right-opened range
		q = s.session.Query(qText+" and c_col>=?", pKey, startCCols)
	} else {
		// closed range
		q = s.session.Query(qText+" and c_col>=? and c_col<?", pKey, startCCols, finishCCols)
	}

	return scanViewQuery(ctx, q, cb)
}

func (s *appStorageType) Get(pKey []byte, cCols []byte, data *[]byte) (ok bool, err error) {
	*data = (*data)[0:0]
	err = s.session.Query(fmt.Sprintf("select value from %s.values where p_key=? and c_col=?", s.keyspace()), pKey, safeCcols(cCols)).
		Consistency(gocql.Quorum).
		Scan(data)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
