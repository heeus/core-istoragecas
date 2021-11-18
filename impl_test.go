/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	"fmt"
	istorage "github.com/heeus/core-istorage"
	istoragemem "github.com/heeus/core-istoragemem"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	istructs "github.com/heeus/core-istructs"
)

func TestBasicUsage(t *testing.T) {
	defer tearDown()
	rnd := rand.New(rand.NewSource(time.Now().UnixMilli()))
	params := CassandraParams{
		Hosts:             hosts("127.0.0.1"),
		Port:              port(9042),
		ReplicationFactor: 1,
		Keyspace:          fmt.Sprintf("test_space_%d", rnd.Int63()),
	}
	storage, err := Provide()(map[istructs.AppName]CassandraParams{"": params}).AppStorage("")
	if err != nil {
		panic(err)
	}
	fmt.Println("=== storage keyspace", params.Keyspace)
	istoragemem.TechnologyCompatibilityKit(t, storage)
	t.Run("TestAppStorage_ViewRecords_Cassandra", func(t *testing.T) { testAppStorage_ViewRecords_Cassandra(t, storage) })
	t.Run("TestAppStorage_GetQNameID_Cassandra", func(t *testing.T) { testAppStorage_GetQNameID_Cassandra(t, storage) })
}

func testAppStorage_GetQNameID_Cassandra(t *testing.T, storage istorage.IAppStorage) {
	t.Run("Should get id concurrently", func(t *testing.T) {
		require := require.New(t)
		qids := make(chan istorage.QNameID, 5)
		wg := sync.WaitGroup{}
		wg.Add(5)
		for i := 0; i < 5; i++ {
			go func() {
				qid, err := storage.GetQNameID(istructs.NewQName("test", "GetIdConcurrently"))

				qids <- qid
				require.NoError(err)
				wg.Done()
			}()
		}
		wg.Wait()
		close(qids)
		result := make(map[istorage.QNameID]bool)
		for qid := range qids {
			result[qid] = true
		}

		require.Len(result, 1)
	})
}

func testAppStorage_ViewRecords_Cassandra(t *testing.T, storage istorage.IAppStorage) {
	t.Run("Should read view records by partial clustering columns when partial clustering columns each byte is 0xff", func(t *testing.T) {
		require := require.New(t)
		viewRecords := make(map[string]bool)
		reader := func(viewRecord []byte) (err error) {
			viewRecords[string(viewRecord)] = true
			return err
		}
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.PartitionID(200), istructs.WSID(200), []byte{0xff}, []byte{0xff, 0xff, 0xfe}, []byte("Cola"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.PartitionID(200), istructs.WSID(200), []byte{0xff}, []byte{0xff, 0xff, 0xff}, []byte("7up"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.PartitionID(200), istructs.WSID(200), []byte{0xff}, []byte{0xff, 0xfe, 0xff}, []byte("Sprite"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.PartitionID(200), istructs.WSID(200), []byte{0xff}, []byte{0xfe, 0xff, 0xff}, []byte("Pepsi"))

		require.NoError(storage.ReadView(istructs.NewQName("bo", "Article"), istructs.PartitionID(200), istructs.WSID(200), []byte{0xff}, []byte{0xff, 0xff}, reader))

		require.Len(viewRecords, 2)
		require.True(viewRecords["Cola"])
		require.True(viewRecords["7up"])
	})
	t.Run("Should read view records by partial clustering columns when partial clustering columns last byte is 0xff", func(t *testing.T) {
		require := require.New(t)
		viewRecords := make(map[string]bool)
		reader := func(viewRecord []byte) (err error) {
			viewRecords[string(viewRecord)] = true
			return err
		}
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.PartitionID(201), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xfe, 0xfe}, []byte("Cola"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.PartitionID(201), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xfe, 0xff}, []byte("7up"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.PartitionID(201), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xff, 0xfe}, []byte("Sprite"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.PartitionID(201), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xff, 0xff}, []byte("Pepsi"))

		require.NoError(storage.ReadView(istructs.NewQName("bo", "Article"), istructs.PartitionID(201), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xff}, reader))

		require.Len(viewRecords, 2)
		require.True(viewRecords["Sprite"])
		require.True(viewRecords["Pepsi"])
	})
}

func tearDown() {
	cluster := gocql.NewCluster(strings.Split(hosts("127.0.0.1"), ",")...)
	cluster.Port = port(9042)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = ConnectionTimeout

	s, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	keyspaceNames := make([]string, 0)
	rows, err := s.Query("describe keyspaces").Iter().SliceMap()
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		keyspaceNames = append(keyspaceNames, row["keyspace_name"].(string))
	}
	for _, keyspaceName := range keyspaceNames {
		if strings.HasPrefix(keyspaceName, "test_space") {
			err = s.Query(fmt.Sprintf("drop keyspace if exists %s", keyspaceName)).Exec()
			if err != nil {
				panic(err)
			}
		}
	}
}

func hosts(defaultValue string) string {
	value, ok := os.LookupEnv("ISTORAGECAS_HOSTS")
	if !ok {
		return defaultValue
	}
	return value
}

func port(defaultValue int) int {
	value, ok := os.LookupEnv("ISTORAGECAS_PORT")
	if !ok {
		return defaultValue
	}
	result, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return result
}

func TestProvide(t *testing.T) {
	require.Panics(t, func() {
		Provide()(map[istructs.AppName]CassandraParams{"": {}})
	})
}

func TestAppStorageProvider_AppStorage(t *testing.T) {
	require := require.New(t)
	p := appStorageProvider{map[istructs.AppName]istorage.IAppStorage{}}

	storage, err := p.AppStorage("app")

	require.Nil(storage)
	require.ErrorIs(err, istructs.ErrAppNotFound)
}

func Test_newStorage(t *testing.T) {
	t.Run("Should return error when keyspace is wrong", func(t *testing.T) {
		require := require.New(t)
		params := CassandraParams{
			Hosts:             hosts("127.0.0.1"),
			Port:              port(9042),
			ReplicationFactor: 1,
			Keyspace:          "wrong-keyspace",
		}

		storage, err := newStorage(params)

		require.Nil(storage)
		require.Contains(err.Error(), "can't create keyspace")
	})
}

func Test_getConsistency(t *testing.T) {
	require := require.New(t)

	require.Equal(gocql.LocalQuorum, getConsistency(true))
	require.Equal(gocql.LocalOne, getConsistency(false))
}
