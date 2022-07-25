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
	if casPar.NumRetries <= 0 {
		casPar.NumRetries = 3
	}
	retryPolicy := gocql.SimpleRetryPolicy{
		NumRetries: casPar.NumRetries,
	}
	provider.cluster.Consistency = gocql.Quorum
	provider.cluster.ConnectTimeout = InitialConnectionTimeout
	provider.cluster.Timeout = ConnectionTimeout
	provider.cluster.RetryPolicy = &retryPolicy
	provider.cluster.Authenticator = gocql.PasswordAuthenticator{Username: casPar.Username, Password: casPar.Pwd}
	provider.cluster.CQLVersion = casPar.cqlVersion()
	provider.cluster.ProtoVersion = casPar.ProtoVersion

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

func (s *appStorageType) PutBatch(items []istorage.BatchItem) (err error) {
	batch := s.session.NewBatch(gocql.LoggedBatch)
	batch.SetConsistency(gocql.Quorum)
	stmt := fmt.Sprintf("insert into %s.values (p_key, c_col, value) values (?,?,?)", s.keyspace())
	for _, item := range items {
		batch.Query(stmt, item.PKey, safeCcols(item.CCols), item.Value)
	}
	return s.session.ExecuteBatch(batch)
}

func scanViewQuery(ctx context.Context, q *gocql.Query, cb istorage.ReadCallback) (err error) {
	q.Consistency(gocql.Quorum)
	scanner := q.Iter().Scanner()
	sc := scannerCloser(scanner)
	for scanner.Next() {
		clustCols := make([]byte, 0)
		viewRecord := make([]byte, 0)
		err = scanner.Scan(&clustCols, &viewRecord)
		if err != nil {
			return sc(err)
		}
		err = cb(clustCols, viewRecord)
		if err != nil {
			return sc(err)
		}
		if ctx.Err() != nil {
			return nil // TCK contract
		}
	}
	return sc(nil)
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

func (s *appStorageType) GetBatch(pKey []byte, items []istorage.GetBatchItem) (err error) {
	ccToIdx := make(map[string][]int)
	values := make([]interface{}, 0, len(items)+1)
	values = append(values, pKey)

	stmt := strings.Builder{}
	stmt.WriteString("select c_col, value from ")
	stmt.WriteString(s.keyspace())
	stmt.WriteString(".values where p_key=? and ")
	stmt.WriteString("c_col in (")
	for i, item := range items {
		items[i].Ok = false
		values = append(values, item.CCols)
		ccToIdx[string(item.CCols)] = append(ccToIdx[string(item.CCols)], i)
		stmt.WriteRune('?')
		if i < len(items)-1 {
			stmt.WriteRune(',')
		}
	}
	stmt.WriteRune(')')

	scanner := s.session.Query(stmt.String(), values...).
		Consistency(gocql.Quorum).
		Iter().
		Scanner()
	sc := scannerCloser(scanner)

	for scanner.Next() {
		ccols := make([]byte, 0)
		value := make([]byte, 0)
		err = scanner.Scan(&ccols, &value)
		if err != nil {
			return sc(err)
		}
		ii, ok := ccToIdx[string(ccols)]
		if ok {
			for _, i := range ii {
				items[i].Ok = true
				*items[i].Data = append((*items[i].Data)[0:0], value...)
			}
		}
	}

	return sc(nil)
}

func scannerCloser(scanner gocql.Scanner) func(error) error {
	return func(err error) error {
		if scannerErr := scanner.Err(); scannerErr != nil {
			if err != nil {
				err = fmt.Errorf("%s %w", err.Error(), scannerErr)
			} else {
				err = scannerErr
			}
		}
		return err
	}
}
