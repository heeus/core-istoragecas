/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import (
	"os"
	"strconv"
	"sync"
	"testing"

	istorage "github.com/heeus/core/istorage"
	"github.com/stretchr/testify/require"
)

const casDefaultPort = 9042
const casDefaultHost = "127.0.0.1"

func TestBasicUsage(t *testing.T) {
	casPar := CassandraParamsType{
		Hosts:                   hosts(),
		Port:                    port(),
		NumRetries:              retryAttempt,
		KeyspaceWithReplication: SimpleWithReplication,
	}
	asf, err := Provide(casPar)
	require.NoError(t, err)
	istorage.TechnologyCompatibilityKit(t, asf)

}

func TestMultipleApps(t *testing.T) {
	const appCount = 3

	require := require.New(t)

	casPar := CassandraParamsType{
		Hosts:                   hosts(),
		Port:                    port(),
		KeyspaceWithReplication: SimpleWithReplication,
	}

	wg := sync.WaitGroup{}

	asf, err := Provide(casPar)
	require.NoError(err)

	testApp := func() {
		defer wg.Done()
		istorage.TechnologyCompatibilityKit(t, asf)
	}

	for appNo := 0; appNo < appCount; appNo++ {
		wg.Add(1)
		go testApp()
	}

	wg.Wait()
}

func hosts() string {
	value, ok := os.LookupEnv("ISTORAGECAS_HOSTS")
	if !ok {
		return casDefaultHost
	}
	return value
}

func port() int {
	value, ok := os.LookupEnv("ISTORAGECAS_PORT")
	if !ok {
		return casDefaultPort
	}
	result, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return result
}

func TestCassandraParamsType_cqlVersion(t *testing.T) {
	tests := []struct {
		name           string
		cqlVersion     string
		wantCqlVersion string
	}{
		{
			name:           "Should get default",
			cqlVersion:     "",
			wantCqlVersion: "3.0.0",
		},
		{
			name:           "Should get custom",
			cqlVersion:     "1.2.3",
			wantCqlVersion: "1.2.3",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.wantCqlVersion, CassandraParamsType{CQLVersion: test.cqlVersion}.cqlVersion())
		})
	}
}
