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

func implAppStorageProvider(apps map[istructs.AppName]CassandraParams) istorage.IAppStorageProvider {
	cache := make(map[istructs.AppName]istorage.IAppStorage)
	for name, params := range apps {
		storage, err := newStorage(params)
		if err != nil {
			panic(err)
		}
		cache[name] = storage
	}
	return &appStorageProvider{cache}
}

type appStorageProvider struct {
	cache map[istructs.AppName]istorage.IAppStorage
}

func (p appStorageProvider) AppStorage(appName istructs.AppName) (storage istorage.IAppStorage, err error) {
	storage, ok := p.cache[appName]
	if !ok {
		return nil, istructs.ErrAppNotFound
	}
	return storage, nil
}

type appStorage struct {
	session *gocql.Session
	lock    sync.Mutex
}

func newStorage(params CassandraParams) (storage istorage.IAppStorage, err error) {
	cluster := gocql.NewCluster(strings.Split(params.Hosts, ",")...)
	if params.Port > 0 {
		cluster.Port = params.Port
	}
	cluster.Consistency = getConsistency(true)
	cluster.Timeout = ConnectionTimeout
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: params.Username, Password: params.Pwd}

	//Prepare keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("can't connect to cluster %#v %w", params, err)
	}
	defer session.Close()
	err = session.Query(fmt.Sprintf(`
		create keyspace if not exists %s 
		with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }`, params.Keyspace, params.ReplicationFactor)).
		Exec()
	if err != nil {
		return nil, fmt.Errorf("can't create keyspace %s %w", params.Keyspace, err)
	}

	//Prepare tables
	cluster.Keyspace = params.Keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		session.Close()
		return nil, fmt.Errorf("can't connect to cluster %#v %w", params, err)
	}

	tables := []string{
		`create table if not exists records
		(
			wsid   		bigint,
			qid  		smallint,
			id_hi  		bigint,
			id_low 		smallint,
			data 		blob,
			primary key	((wsid, qid, id_hi), id_low)
		)`,
		`create table if not exists plog
		(
			partition_id	smallint,
			offset_hi 		bigint,
			offset_low 		smallint,
			event 			blob,
			primary key		((partition_id, offset_hi), offset_low)
		)`,
		`create table if not exists wlog
		(
			wsid			bigint,
			offset_hi 		bigint,
			offset_low 		smallint,
			event 			blob,
			primary key 	((wsid, offset_hi), offset_low)
		)`,
		`create table if not exists view_records
		(
			wsid   			bigint,
			partition_id	smallint,
			qname  			smallint,
			p_key 			blob,
			c_col			blob,
			value			blob,
			primary key 	((wsid, partition_id, qname, p_key), c_col)
		)`,
		`create table if not exists qnames
		(
			name		text,
			id			int,
			primary key (name)
		)`,
	}

	for _, table := range tables {
		err = doWithAttempts(Attempts, time.Second, func() error { return session.Query(table).Exec() })
		if err != nil {
			return nil, fmt.Errorf("can't create table %w", err)
		}
	}

	return &appStorage{session: session}, nil
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

func (s *appStorage) GetRecord(table istructs.QName, workspace istructs.WSID, highConsistency bool, id istructs.RecordID, data *[]byte) (ok bool, err error) {
	qid, err := s.GetQNameID(table)
	if err != nil {
		return
	}
	*data = (*data)[0:0]
	idHi, idLow := crackID(istructs.IDType(id))
	err = s.session.Query("select data from records where wsid=? and qid=? and id_hi=? and id_low=?",
		int64(workspace),
		int16(qid),
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

func (s *appStorage) PutRecord(table istructs.QName, workspace istructs.WSID, highConsistency bool, id istructs.RecordID, data []byte) (err error) {
	qid, err := s.GetQNameID(table)
	if err != nil {
		return
	}
	idHi, idLow := crackID(istructs.IDType(id))
	return s.session.Query("insert into records (wsid, qid, id_hi, id_low, data) values (?,?,?,?,?)",
		int64(workspace),
		int16(qid),
		idHi,
		idLow,
		data).
		Consistency(getConsistency(highConsistency)).
		Exec()
}

func (s *appStorage) PutPLogEvent(partition istructs.PartitionID, offset istructs.Offset, event []byte) {
	offsetHi, offsetLow := crackID(istructs.IDType(offset))
	err := s.session.Query("insert into plog (partition_id, offset_hi, offset_low, event) values (?,?,?,?)",
		int16(partition),
		offsetHi,
		offsetLow,
		event).
		Exec()
	if err != nil {
		//TODO panic?
		panic(err)
	}
}

func (s *appStorage) ReadPLog(ctx context.Context, partition istructs.PartitionID, offset istructs.Offset, toReadCount int, cb istorage.LogReaderCallback) (err error) {
	for i := 0; i < toReadCount; i++ {
		if ctx.Err() != nil {
			return
		}
		event := make([]byte, 0)
		plogOffset := offset + istructs.Offset(i)
		offsetHi, offsetLow := crackID(istructs.IDType(plogOffset))
		err = s.session.Query("select event from plog where partition_id=? and offset_hi=? and offset_low=?",
			int16(partition),
			offsetHi,
			offsetLow).
			Scan(&event)
		if errors.Is(err, gocql.ErrNotFound) {
			plogOffset = istructs.NullOffset
		}
		if err != nil && !errors.Is(err, gocql.ErrNotFound) {
			return
		}
		err = cb(plogOffset, event)
		if err != nil {
			return
		}
	}
	return
}

func (s *appStorage) PutWLogEvent(workspace istructs.WSID, offset istructs.Offset, event []byte) {
	offsetHi, offsetLow := crackID(istructs.IDType(offset))
	err := s.session.Query("insert into wlog (wsid, offset_hi, offset_low, event) values (?,?,?,?)",
		int64(workspace),
		offsetHi,
		offsetLow,
		event).
		Exec()
	if err != nil {
		//TODO panic?
		panic(err)
	}
}

func (s *appStorage) ReadWLog(ctx context.Context, workspace istructs.WSID, offset istructs.Offset, toReadCount int, cb istorage.LogReaderCallback) (err error) {
	for i := 0; i < toReadCount; i++ {
		if ctx.Err() != nil {
			return
		}
		event := make([]byte, 0)
		plogOffset := offset + istructs.Offset(i)
		offsetHi, offsetLow := crackID(istructs.IDType(plogOffset))
		err = s.session.Query("select event from wlog where wsid=? and offset_hi=? and offset_low=?",
			int64(workspace),
			offsetHi,
			offsetLow).
			Scan(&event)
		if errors.Is(err, gocql.ErrNotFound) {
			plogOffset = istructs.NullOffset
		}
		if err != nil && !errors.Is(err, gocql.ErrNotFound) {
			return
		}
		err = cb(plogOffset, event)
		if err != nil {
			return
		}
	}
	return
}

func (s *appStorage) PutViewRecord(view istructs.QName, partition istructs.PartitionID, workspace istructs.WSID, pKey []byte, cCols []byte, value []byte) {
	qid, err := s.GetQNameID(view)
	if err != nil {
		//TODO panic???
		panic(err)
	}
	err = s.session.Query("insert into view_records (wsid, partition_id, qname, p_key, c_col, value) values (?,?,?,?,?,?)",
		int64(workspace),
		int16(partition),
		int16(qid),
		pKey,
		cCols,
		value).
		Exec()
	if err != nil {
		//TODO panic???
		panic(err)
	}
}

func (s *appStorage) ReadView(view istructs.QName, partition istructs.PartitionID, workspace istructs.WSID, pKey []byte, partialCCols []byte, cb istorage.ViewReaderCallback) (err error) {
	qid, err := s.GetQNameID(view)
	if err != nil {
		return
	}
	c := partialClusteringColumns{partialCCols}
	var q *gocql.Query
	if c.isEmpty() {
		q = s.session.Query("select value from view_records "+
			"where wsid=? and partition_id=? and qname=? and p_key=?",
			int64(workspace),
			int16(partition),
			int16(qid),
			pKey)
	} else if c.isMax() {
		q = s.session.Query("select value from view_records "+
			"where wsid=? and partition_id=? and qname=? and p_key=? and c_col>=?",
			int64(workspace),
			int16(partition),
			int16(qid),
			pKey,
			partialCCols)
	} else {
		q = s.session.Query("select value from view_records "+
			"where wsid=? and partition_id=? and qname=? and p_key=? and c_col>=? and c_col<?",
			int64(workspace),
			int16(partition),
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
		viewRecord := make([]byte, 0)
		err = scanner.Scan(&viewRecord)
		if err != nil {
			return closeScanner(err)
		}
		err = cb(viewRecord)
		if err != nil {
			return closeScanner(err)
		}
	}
	return closeScanner(nil)
}

func (s *appStorage) GetViewRecord(view istructs.QName, partition istructs.PartitionID, workspace istructs.WSID, pKey []byte, cCols []byte, data *[]byte) (ok bool, err error) {
	qid, err := s.GetQNameID(view)
	if err != nil {
		return
	}
	*data = (*data)[0:0]
	err = s.session.Query("select value from view_records where wsid=? and partition_id=? and qname=? and p_key=? and c_col=?",
		int64(workspace),
		int16(partition),
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

func (s *appStorage) GetQNameID(name istructs.QName) (qid istorage.QNameID, err error) {
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
	err = s.session.Query("select max(id) from qnames").Scan(&id)
	if err != nil {
		return 0, err
	}
	id++
	qid = istorage.QNameID(id)
	err = s.session.Query("insert into qnames (name, id) values (?,?)", name.String(), id).Exec()
	return
}

func (s *appStorage) getQNameID(name istructs.QName) (qid istorage.QNameID, err error) {
	var id int32
	err = s.session.Query("select id from qnames where name = ?", name.String()).Scan(&id)
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
