/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
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
		Keyspace: "testspace_0",
	}
	asp, cleanup := Provide(casPar, map[istructs.AppQName]AppCassandraParamsType{istructs.AppQName_test1_app1: appPar})
	defer cleanup()
	storage, err := asp.AppStorage(istructs.AppQName_test1_app1)
	if err != nil {
		panic(err)
	}
	fmt.Println("=== storage keyspace", appPar.Keyspace)
	istoragemem.TechnologyCompatibilityKit(t, storage)
}

func TestMultiplyApps(t *testing.T) {
	const appCount = 3

	setUp(appCount)  // setup test sandbox
	defer tearDown() // clear test sandbox

	require := require.New(t)

	casPar := CassandraParamsType{
		Hosts: hosts("127.0.0.1"),
		Port:  port(9042),
	}
	appPar := make(map[istructs.AppQName]AppCassandraParamsType, appCount)
	for appNo := 0; appNo < appCount; appNo++ {
		aqn := istructs.NewAppQName("test", fmt.Sprintf("app%d", appNo))
		appPar[aqn] = AppCassandraParamsType{
			Keyspace: fmt.Sprintf("testspace_%d", appNo),
		}
	}

	wg := sync.WaitGroup{}

	provide, cleanup := Provide(casPar, appPar)
	defer cleanup()

	testApp := func(app istructs.AppQName) {
		defer wg.Done()
		storage, err := provide.AppStorage(app)
		require.Nil(err)
		istoragemem.TechnologyCompatibilityKit(t, storage)
	}

	for n := range appPar {
		wg.Add(1)
		go testApp(n)
	}

	wg.Wait()
}

func setUp(testKeyspacesCount int) {

	tearDown()

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
			with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : %d }`, keyspace, 1)).Consistency(gocql.Quorum).Exec()
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
	rows, err := s.Query("select * from system_schema.keyspaces").Consistency(gocql.Quorum).Iter().SliceMap()
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		keyspaceNames = append(keyspaceNames, row["keyspace_name"].(string))
	}
	for _, keyspace := range keyspaceNames {
		if strings.HasPrefix(keyspace, "testspace_") {
			fmt.Printf("Dropping %s…\n", keyspace)
			err = s.Query(fmt.Sprintf("drop keyspace if exists %s", keyspace)).Consistency(gocql.Quorum).Exec()
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
		Provide(CassandraParamsType{}, map[istructs.AppQName]AppCassandraParamsType{istructs.AppQName_test1_app1: {}})
	})
}

func TestAppStorageProvider_AppStorage(t *testing.T) {
	require := require.New(t)
	p := appStorageProviderType{
		cache: map[istructs.AppQName]istorage.IAppStorage{},
	}

	storage, err := p.AppStorage(istructs.AppQName_test1_app1)

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
			Keyspace: "wrong-keyspace",
		}

		require.Panics(t, func() {
			_, _ = Provide(casPar, map[istructs.AppQName]AppCassandraParamsType{istructs.AppQName_test1_app1: appPar})
		})
	})
}
