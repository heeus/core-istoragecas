/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	"bytes"
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

func (s *appStorageType) GetRecord(workspace istructs.WSID, _ bool, id istructs.RecordID, data *[]byte) (ok bool, err error) {
	*data = (*data)[0:0]
	idHi, idLow := crackID(istructs.IDType(id))
	err = s.session.Query(fmt.Sprintf("select data from %s.records where wsid=? and id_hi=? and id_low=?", s.keyspace()),
		int64(workspace),
		idHi,
		idLow).
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

func (s *appStorageType) PutRecord(workspace istructs.WSID, _ bool, id istructs.RecordID, data []byte) (err error) {
	idHi, idLow := crackID(istructs.IDType(id))
	return s.session.Query(fmt.Sprintf("insert into %s.records (wsid, id_hi, id_low, data) values (?,?,?,?)", s.keyspace()),
		int64(workspace),
		idHi,
		idLow,
		data).
		Consistency(gocql.Quorum).
		Exec()
}

func (s *appStorageType) PutPLogEvent(partition istructs.PartitionID, offset istructs.Offset, event []byte) (err error) {
	offsetHi, offsetLow := crackID(istructs.IDType(offset))
	return s.session.Query(fmt.Sprintf("insert into %s.plog (partition_id, offset_hi, offset_low, event) values (?,?,?,?)", s.keyspace()),
		int16(partition),
		offsetHi,
		offsetLow,
		event).
		Consistency(gocql.Quorum).
		Exec()
}

func (s *appStorageType) ReadPLog(ctx context.Context, partition istructs.PartitionID, offset istructs.Offset, toReadCount int, cb istorage.LogReaderCallback) (err error) {

	readQuery := func(part int64, offset_low_from, offset_low_to int16) (query *gocql.Query) {
		var qParams []interface{}
		qText := fmt.Sprintf("select offset_low, event from %s.plog where (partition_id = ?) and (offset_hi = ?)", s.keyspace())
		qParams = append(qParams, partition, part)

		if offset_low_from > 0 {
			qText = qText + " and (offset_low >= ?)"
			qParams = append(qParams, offset_low_from)
		}
		if offset_low_to < LowMask {
			qText = qText + " and (offset_low <= ?)"
			qParams = append(qParams, offset_low_to)
		}
		qText = qText + " order by offset_low" // key is (partition_id, offset_hi, offset_low)

		query = s.session.Query(qText, qParams...).Consistency(gocql.Quorum)

		return query
	}

	return readLog(ctx, offset, toReadCount, readQuery, cb)
}

func (s *appStorageType) PutWLogEvent(workspace istructs.WSID, offset istructs.Offset, event []byte) (err error) {
	offsetHi, offsetLow := crackID(istructs.IDType(offset))
	return s.session.Query(fmt.Sprintf("insert into %s.wlog (wsid, offset_hi, offset_low, event) values (?,?,?,?)", s.keyspace()),
		int64(workspace),
		offsetHi,
		offsetLow,
		event).
		Consistency(gocql.Quorum).
		Exec()
}

func (s *appStorageType) ReadWLog(ctx context.Context, workspace istructs.WSID, offset istructs.Offset, toReadCount int, cb istorage.LogReaderCallback) (err error) {

	readQuery := func(part int64, clustFrom, clustTo int16) (query *gocql.Query) {
		var qParams []interface{}
		qText := fmt.Sprintf("select offset_low, event from %s.wlog where (wsid = ?) and (offset_hi = ?)", s.keyspace())
		qParams = append(qParams, workspace, part)

		if clustFrom > 0 {
			qText = qText + " and (offset_low >= ?)"
			qParams = append(qParams, clustFrom)
		}
		if clustTo < LowMask {
			qText = qText + " and (offset_low <= ?)"
			qParams = append(qParams, clustTo)
		}
		qText = qText + " order by offset_low" // key is (wsid, offset_hi, offset_low)

		query = s.session.Query(qText, qParams...).Consistency(gocql.Quorum)

		return query
	}

	return readLog(ctx, offset, toReadCount, readQuery, cb)
}

func safeCcols(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}

func (s *appStorageType) PutViewRecord(view istructs.QName, workspace istructs.WSID, pKey []byte, cCols []byte, value []byte) (err error) {
	qid, err := s.GetQNameID(view)
	if err != nil {
		return err
	}
	return s.session.Query(fmt.Sprintf("insert into %s.view_records (wsid, qname, p_key, c_col, value) values (?,?,?,?,?)", s.keyspace()),
		int64(workspace),
		int16(qid),
		pKey,
		safeCcols(cCols),
		value).
		Consistency(gocql.Quorum).
		Exec()
}

func scanViewQuery(ctx context.Context, q *gocql.Query, cb istorage.ViewReaderCallback) (err error) {
	q.Consistency(gocql.Quorum)
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
			return nil // TCK contract
		}
	}
	return closeScanner(nil)
}

func (s *appStorageType) ReadView(ctx context.Context, view istructs.QName, workspace istructs.WSID, pKey []byte, partialCCols []byte, cb istorage.ViewReaderCallback) (err error) {
	c := partialClusteringColumns{partialCCols}
	if c.isEmpty() {
		return s.ReadViewRange(ctx, view, workspace, pKey, nil, nil, cb)
	} else if c.isMax() {
		return s.ReadViewRange(ctx, view, workspace, pKey, partialCCols, nil, cb)
	} else {
		return s.ReadViewRange(ctx, view, workspace, pKey, partialCCols, c.doUpperBound(), cb)
	}
}

func (s *appStorageType) ReadViewRange(ctx context.Context, view istructs.QName, workspace istructs.WSID, pKey []byte, startCCols, finishCCols []byte, cb istorage.ViewReaderCallback) (err error) {
	if (len(startCCols) > 0) && (len(finishCCols) > 0) && (bytes.Compare(startCCols, finishCCols) >= 0) {
		return nil // absurd range
	}

	qid, err := s.GetQNameID(view)
	if err != nil {
		return err
	}

	qText := fmt.Sprintf("select c_col, value from %s.view_records "+
		"where wsid=? and qname=? and p_key=?", s.keyspace())

	var q *gocql.Query
	if len(startCCols) == 0 {
		if len(finishCCols) == 0 {
			// opened range
			q = s.session.Query(qText, int64(workspace), int16(qid), pKey)
		} else {
			// left-opened range
			q = s.session.Query(qText+" and c_col<?", int64(workspace), int16(qid), pKey, finishCCols)
		}
	} else if len(finishCCols) == 0 {
		// right-opened range
		q = s.session.Query(qText+" and c_col>=?", int64(workspace), int16(qid), pKey, startCCols)
	} else {
		// closed range
		q = s.session.Query(qText+" and c_col>=? and c_col<?", int64(workspace), int16(qid), pKey, startCCols, finishCCols)
	}

	return scanViewQuery(ctx, q, cb)
}

func (s *appStorageType) GetViewRecord(view istructs.QName, workspace istructs.WSID, pKey []byte, cCols []byte, data *[]byte) (ok bool, err error) {
	qid, err := s.GetQNameID(view)
	if err != nil {
		return false, err
	}
	*data = (*data)[0:0]
	err = s.session.Query(fmt.Sprintf("select value from %s.view_records where wsid=? and qname=? and p_key=? and c_col=?", s.keyspace()),
		int64(workspace),
		int16(qid),
		pKey,
		safeCcols(cCols)).
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

func (s *appStorageType) GetQNameID(name istructs.QName) (qid istorage.QNameID, err error) {
	qid, err = s.getQNameID(name)
	if err == nil {
		return qid, err
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	//Double check
	qid, err = s.getQNameID(name)
	if err == nil {
		return qid, nil // success
	}

	var id int32
	err = s.session.Query(fmt.Sprintf("select max(id) from %s.qnames", s.keyspace())).Consistency(gocql.Quorum).Scan(&id)
	if err != nil {
		return 0, err
	}
	id++
	qid = istorage.QNameID(id)
	err = s.session.Query(fmt.Sprintf("insert into %s.qnames (name, id) values (?,?)", s.keyspace()), name.String(), id).Consistency(gocql.Quorum).Exec()
	return qid, err // success if insert
}

func (s *appStorageType) getQNameID(name istructs.QName) (qid istorage.QNameID, err error) {
	var id int32
	err = s.session.Query(fmt.Sprintf("select id from %s.qnames where name = ?", s.keyspace()), name.String()).Consistency(gocql.Quorum).Scan(&id)
	if err == nil {
		qid = istorage.QNameID(id)
		return qid, nil // success
	}
	return 0, err
}

func crackID(id istructs.IDType) (hi int64, low int16) {
	return int64(id >> PartitionBits), int16(id & LowMask)
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
