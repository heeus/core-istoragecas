/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/gocql/gocql"
	istorage "github.com/heeus/core-istorage"
	istoragemem "github.com/heeus/core-istoragemem"
	istructs "github.com/heeus/core-istructs"
	"github.com/stretchr/testify/require"
)

func TestBasicUsage(t *testing.T) {
	setUp(1)         // setup test sandbox
	defer tearDown() // clear test sandbox

	casPar := CassandraParamsType{
		Hosts: hosts("127.0.0.1"),
		Port:  port(9042),
	}
	appPar := AppCassandraParamsType{
		Keyspace:          "testspace_0",
		ReplicationFactor: 1,
	}
	storage, err := Provide(casPar, map[istructs.AppName]AppCassandraParamsType{"testApp": appPar}).AppStorage("testApp")
	if err != nil {
		panic(err)
	}
	fmt.Println("=== storage keyspace", appPar.Keyspace)
	istoragemem.TechnologyCompatibilityKit(t, storage)
	t.Run("TestAppStorage_ViewRecords_Cassandra", func(t *testing.T) { testAppStorage_ViewRecords_Cassandra(t, storage) })
	t.Run("TestAppStorage_GetQNameID_Cassandra", func(t *testing.T) { testAppStorage_GetQNameID_Cassandra(t, storage) })
}

func TestMultiplyApps(t *testing.T) {
	const appCount = 5

	setUp(appCount)  // setup test sandbox
	defer tearDown() // clear test sandbox

	require := require.New(t)

	casPar := CassandraParamsType{
		Hosts: hosts("127.0.0.1"),
		Port:  port(9042),
	}
	appPar := make(map[istructs.AppName]AppCassandraParamsType, appCount)
	for appNo := 0; appNo < appCount; appNo++ {
		appPar[istructs.AppName(fmt.Sprintf("app%d", appNo))] = AppCassandraParamsType{
			Keyspace:          fmt.Sprintf("testspace_%d", appNo),
			ReplicationFactor: 1,
		}
	}

	wg := sync.WaitGroup{}

	provide := Provide(casPar, appPar)

	testApp := func(app istructs.AppName) {
		defer wg.Done()
		storage, err := provide.AppStorage(app)
		require.Nil(err)
		istoragemem.TechnologyCompatibilityKit(t, storage)
		testAppStorage_ViewRecords_Cassandra(t, storage)
	}

	for n := range appPar {
		wg.Add(1)
		go testApp(n)
	}

	wg.Wait()
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

	ctx := context.Background()

	t.Run("Should read view records by partial clustering columns when partial clustering columns each byte is 0xff", func(t *testing.T) {
		require := require.New(t)
		viewRecords := make(map[string]bool)
		reader := func(viewRecord []byte) (err error) {
			viewRecords[string(viewRecord)] = true
			return err
		}
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.WSID(200), []byte{0xff}, []byte{0xff, 0xff, 0xfe}, []byte("Cola"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.WSID(200), []byte{0xff}, []byte{0xff, 0xff, 0xff}, []byte("7up"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.WSID(200), []byte{0xff}, []byte{0xff, 0xfe, 0xff}, []byte("Sprite"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.WSID(200), []byte{0xff}, []byte{0xfe, 0xff, 0xff}, []byte("Pepsi"))

		require.NoError(storage.ReadView(ctx, istructs.NewQName("bo", "Article"), istructs.WSID(200), []byte{0xff}, []byte{0xff, 0xff}, reader))

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
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xfe, 0xfe}, []byte("Cola"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xfe, 0xff}, []byte("7up"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xff, 0xfe}, []byte("Sprite"))
		storage.PutViewRecord(istructs.NewQName("bo", "Article"), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xff, 0xff}, []byte("Pepsi"))

		require.NoError(storage.ReadView(ctx, istructs.NewQName("bo", "Article"), istructs.WSID(201), []byte{0xff}, []byte{0x00, 0xff}, reader))

		require.Len(viewRecords, 2)
		require.True(viewRecords["Sprite"])
		require.True(viewRecords["Pepsi"])
	})
}

func setUp(testKeyspacesCount int) {
	// Prepare test keyspaces

	cluster := gocql.NewCluster(strings.Split(hosts("127.0.0.1"), ",")...)
	cluster.Port = port(9042)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = ConnectionTimeout

	s, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	for ksNo := 0; ksNo < testKeyspacesCount; ksNo++ {
		keyspace := fmt.Sprintf("testspace_%d", ksNo)
		fmt.Printf("Creating %s…\n", keyspace)
		err = s.Query(fmt.Sprintf(`
			create keyspace if not exists %s
			with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }`, keyspace, 1)).Exec()
		if err != nil {
			panic(fmt.Errorf("can't create keyspace «%s»: %w", keyspace, err))
		}
	}
}

func tearDown() {
	// drop test keyspaces
	cluster := gocql.NewCluster(strings.Split(hosts("127.0.0.1"), ",")...)
	cluster.Port = port(9042)
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = ConnectionTimeout

	s, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	keyspaceNames := make([]string, 0)
	rows, err := s.Query("select * from system_schema.keyspaces").Iter().SliceMap()
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		keyspaceNames = append(keyspaceNames, row["keyspace_name"].(string))
	}
	for _, keyspace := range keyspaceNames {
		if strings.HasPrefix(keyspace, "testspace_") {
			fmt.Printf("Droppping %s…\n", keyspace)
			err = s.Query(fmt.Sprintf("drop keyspace if exists %s", keyspace)).Exec()
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
		Provide(CassandraParamsType{}, map[istructs.AppName]AppCassandraParamsType{"": {}})
	})
}

func TestAppStorageProvider_AppStorage(t *testing.T) {
	require := require.New(t)
	p := appStorageProviderType{
		cache: map[istructs.AppName]istorage.IAppStorage{},
	}

	storage, err := p.AppStorage("testApp")

	require.Nil(storage)
	require.ErrorIs(err, istructs.ErrAppNotFound)
}

func Test_newStorage(t *testing.T) {
	casPar := CassandraParamsType{
		Hosts: hosts("127.0.0.1"),
		Port:  port(9042),
	}

	t.Run("Should return error when keyspace is wrong", func(t *testing.T) {
		appPar := AppCassandraParamsType{
			Keyspace:          "wrong-keyspace",
			ReplicationFactor: 1,
		}

		require.Panics(t, func() {
			_ = Provide(casPar, map[istructs.AppName]AppCassandraParamsType{"testApp": appPar})
		})
	})
}

func Test_getConsistency(t *testing.T) {
	require := require.New(t)

	require.Equal(gocql.LocalQuorum, getConsistency(true))
	require.Equal(gocql.LocalOne, getConsistency(false))
}
