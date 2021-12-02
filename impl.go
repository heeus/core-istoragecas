/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	cache   map[istructs.AppName]istorage.IAppStorage
}

func newStorageProvider(casPar CassandraParamsType, apps map[istructs.AppName]AppCassandraParamsType) (prov *appStorageProviderType, err error) {
	provider := appStorageProviderType{
		casPar: casPar,
		cache:  make(map[istructs.AppName]istorage.IAppStorage),
	}

	provider.cluster = gocql.NewCluster(strings.Split(casPar.Hosts, ",")...)
	if casPar.Port > 0 {
		provider.cluster.Port = casPar.Port
	}
	provider.cluster.Consistency = getConsistency(true)
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

func (p appStorageProviderType) AppStorage(appName istructs.AppName) (storage istorage.IAppStorage, err error) {
	storage, ok := p.cache[appName]
	if !ok {
		return nil, istructs.ErrAppNotFound
	}
	return storage, nil
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
		{name: "records",
			cql: `(
				wsid   		bigint,
				id_hi  		bigint,
				id_low 		smallint,
				data 		blob,
				primary key	((wsid, id_hi), id_low)
			)`},
		{name: "plog",
			cql: `(
		 		partition_id	smallint,
		 		offset_hi 		bigint,
		 		offset_low 		smallint,
		 		event 			blob,
		 		primary key		((partition_id, offset_hi), offset_low)
			)`},
		{name: "wlog",
			cql: `(
		 		wsid			bigint,
		 		offset_hi 		bigint,
		 		offset_low 		smallint,
		 		event 			blob,
		 		primary key 	((wsid, offset_hi), offset_low)
			)`},
		{name: "view_records",
			cql: `(
		 		wsid   			bigint,
		 		qname  			smallint,
		 		p_key 			blob,
		 		c_col			blob,
		 		value			blob,
		 		primary key 	((wsid, qname, p_key), c_col)
			)`},
		{name: "qnames",
			cql: `(
		 		name		text,
		 		id			int,
		 		primary key (name)
		 	)`},
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("can't create session: %w", err)
	}

	for _, table := range tables {
		err = doWithAttempts(Attempts, time.Second, func() error {
			return session.Query(
				fmt.Sprintf(`create table if not exists %s.%s %s`, appPar.Keyspace, table.name, table.cql)).Exec()
		})
		if err != nil {
			return nil, fmt.Errorf("can't create table «%s»: %w", table.name, err)
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
			return
		}
		time.Sleep(delay)
	}
	return
}

func (s *appStorageType) keyspace() string {
	return s.appPar.Keyspace
}

func (s *appStorageType) GetRecord(workspace istructs.WSID, highConsistency bool, id istructs.RecordID, data *[]byte) (ok bool, err error) {
	*data = (*data)[0:0]
	idHi, idLow := crackID(istructs.IDType(id))
	err = s.session.Query(fmt.Sprintf("select data from %s.records where wsid=? and id_hi=? and id_low=?", s.keyspace()),
		int64(workspace),
		idHi,
		idLow).
		Consistency(getConsistency(highConsistency)).
		Scan(data)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *appStorageType) PutRecord(workspace istructs.WSID, highConsistency bool, id istructs.RecordID, data []byte) (err error) {
	idHi, idLow := crackID(istructs.IDType(id))
	return s.session.Query(fmt.Sprintf("insert into %s.records (wsid, id_hi, id_low, data) values (?,?,?,?)", s.keyspace()),
		int64(workspace),
		idHi,
		idLow,
		data).
		Consistency(getConsistency(highConsistency)).
		Exec()
}

func (s *appStorageType) PutPLogEvent(partition istructs.PartitionID, offset istructs.Offset, event []byte) (err error) {
	offsetHi, offsetLow := crackID(istructs.IDType(offset))
	return s.session.Query(fmt.Sprintf("insert into %s.plog (partition_id, offset_hi, offset_low, event) values (?,?,?,?)", s.keyspace()),
		int16(partition),
		offsetHi,
		offsetLow,
		event).
		Exec()
}

func (s *appStorageType) ReadPLog(ctx context.Context, partition istructs.PartitionID, offset istructs.Offset, toReadCount int, cb istorage.LogReaderCallback) (err error) {
	for i := 0; i < toReadCount; i++ {
		if ctx.Err() != nil {
			return
		}
		event := make([]byte, 0)
		plogOffset := offset + istructs.Offset(i)
		offsetHi, offsetLow := crackID(istructs.IDType(plogOffset))
		err = s.session.Query(fmt.Sprintf("select event from %s.plog where partition_id=? and offset_hi=? and offset_low=?", s.keyspace()),
			int16(partition),
			offsetHi,
			offsetLow).
			Scan(&event)
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}
		err = cb(plogOffset, event)
		if err != nil {
			return
		}
	}
	return
}

func (s *appStorageType) PutWLogEvent(workspace istructs.WSID, offset istructs.Offset, event []byte) (err error) {
	offsetHi, offsetLow := crackID(istructs.IDType(offset))
	return s.session.Query(fmt.Sprintf("insert into %s.wlog (wsid, offset_hi, offset_low, event) values (?,?,?,?)", s.keyspace()),
		int64(workspace),
		offsetHi,
		offsetLow,
		event).
		Exec()
}

func (s *appStorageType) ReadWLog(ctx context.Context, workspace istructs.WSID, offset istructs.Offset, toReadCount int, cb istorage.LogReaderCallback) (err error) {
	for i := 0; i < toReadCount; i++ {
		if ctx.Err() != nil {
			return
		}
		event := make([]byte, 0)
		plogOffset := offset + istructs.Offset(i)
		offsetHi, offsetLow := crackID(istructs.IDType(plogOffset))
		err = s.session.Query(fmt.Sprintf("select event from %s.wlog where wsid=? and offset_hi=? and offset_low=?", s.keyspace()),
			int64(workspace),
			offsetHi,
			offsetLow).
			Scan(&event)
		if errors.Is(err, gocql.ErrNotFound) {
			return nil
		}
		err = cb(plogOffset, event)
		if err != nil {
			return
		}
	}
	return
}

func (s *appStorageType) PutViewRecord(view istructs.QName, workspace istructs.WSID, pKey []byte, cCols []byte, value []byte) (err error) {
	qid, err := s.GetQNameID(view)
	if err != nil {
		//TODO panic???
		panic(err)
	}
	return s.session.Query(fmt.Sprintf("insert into %s.view_records (wsid, qname, p_key, c_col, value) values (?,?,?,?,?)", s.keyspace()),
		int64(workspace),
		int16(qid),
		pKey,
		cCols,
		value).
		Exec()
}

func (s *appStorageType) ReadView(ctx context.Context, view istructs.QName, workspace istructs.WSID, pKey []byte, partialCCols []byte, cb istorage.ViewReaderCallback) (err error) {
	qid, err := s.GetQNameID(view)
	if err != nil {
		return
	}
	c := partialClusteringColumns{partialCCols}
	var q *gocql.Query
	if c.isEmpty() {
		q = s.session.Query(fmt.Sprintf("select c_col, value from %s.view_records "+
			"where wsid=? and qname=? and p_key=?", s.keyspace()),
			int64(workspace),
			int16(qid),
			pKey)
	} else if c.isMax() {
		q = s.session.Query(fmt.Sprintf("select c_col, value from %s.view_records "+
			"where wsid=? and qname=? and p_key=? and c_col>=?", s.keyspace()),
			int64(workspace),
			int16(qid),
			pKey,
			partialCCols)
	} else {
		q = s.session.Query(fmt.Sprintf("select c_col, value from %s.view_records "+
			"where wsid=? and qname=? and p_key=? and c_col>=? and c_col<?", s.keyspace()),
			int64(workspace),
			int16(qid),
			pKey,
			partialCCols,
			c.doUpperBound())
	}
	scanner := q.Iter().Scanner()
	closeScanner := func(err error) error {
		e := scanner.Err()
		if e != nil {
			if err != nil {
				return fmt.Errorf("%s %w", err.Error(), e)
			}
			return e
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
			return
		}
	}
	return closeScanner(nil)
}

func (s *appStorageType) GetViewRecord(view istructs.QName, workspace istructs.WSID, pKey []byte, cCols []byte, data *[]byte) (ok bool, err error) {
	qid, err := s.GetQNameID(view)
	if err != nil {
		return
	}
	*data = (*data)[0:0]
	err = s.session.Query(fmt.Sprintf("select value from %s.view_records where wsid=? and qname=? and p_key=? and c_col=?", s.keyspace()),
		int64(workspace),
		int16(qid),
		pKey,
		cCols).
		Scan(data)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return
	}
	return true, nil
}

func (s *appStorageType) GetQNameID(name istructs.QName) (qid istorage.QNameID, err error) {
	qid, err = s.getQNameID(name)
	if err == nil {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	//Double check
	qid, err = s.getQNameID(name)
	if err == nil {
		return
	}

	var id int32
	err = s.session.Query(fmt.Sprintf("select max(id) from %s.qnames", s.keyspace())).Scan(&id)
	if err != nil {
		return 0, err
	}
	id++
	qid = istorage.QNameID(id)
	err = s.session.Query(fmt.Sprintf("insert into %s.qnames (name, id) values (?,?)", s.keyspace()), name.String(), id).Exec()
	return
}

func (s *appStorageType) getQNameID(name istructs.QName) (qid istorage.QNameID, err error) {
	var id int32
	err = s.session.Query(fmt.Sprintf("select id from %s.qnames where name = ?", s.keyspace()), name.String()).Scan(&id)
	if err == nil {
		qid = istorage.QNameID(id)
		return qid, nil
	}
	if !errors.Is(err, gocql.ErrNotFound) {
		return 0, err
	}
	return
}

func crackID(id istructs.IDType) (hi int64, low int16) {
	return int64(id >> PartitionBits), int16(id & LowMask)
}

func getConsistency(highConsistency bool) gocql.Consistency {
	if highConsistency {
		return gocql.LocalQuorum
	}
	return gocql.LocalOne
}

type partialClusteringColumns struct {
	slice []byte
}

func (c partialClusteringColumns) doUpperBound() []byte {
	var doUpperBound func(slice []byte, i int) []byte
	doUpperBound = func(slice []byte, i int) []byte {
		if slice[i] != math.MaxUint8 {
			slice[i] = slice[i] + 1
			return slice
		}
		slice[i] = 0
		return doUpperBound(slice, i-1)
	}
	sliceCopy := make([]byte, len(c.slice))
	copy(sliceCopy, c.slice)
	return doUpperBound(sliceCopy, len(sliceCopy)-1)
}

func (c partialClusteringColumns) isMax() bool {
	for _, e := range c.slice {
		if e != math.MaxUint8 {
			return false
		}
	}
	return true
}

func (c partialClusteringColumns) isEmpty() bool {
	return len(c.slice) == 0
}
