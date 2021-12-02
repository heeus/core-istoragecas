/*
 * Copyright (c) 2021-present Sigma-Soft, Ltd.
 * @author: Nikolay Nikitin
 */

package istoragecas

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_logIterateType_iterate(t *testing.T) {
	const logSize = int64(100 * 1000)
	type resultType struct {
		parts []string
		reads int64
		err   error
	}
	tests := []struct {
		name   string
		iter   *logIterateType
		result resultType
	}{
		{
			name: "0, 4096: read all first partition",
			iter: logIterate(0, 4096),
			result: resultType{
				parts: []string{"0: 0…4095"},
				reads: 4096,
				err:   nil,
			},
		},
		{
			name: "0, 8: read 8 first recs from first partition",
			iter: logIterate(0, 8),
			result: resultType{
				parts: []string{"0: 0…7"},
				reads: 8,
				err:   nil,
			},
		},
		{
			name: "4090, 6: read last 6 recs from first partition",
			iter: logIterate(4090, 6),
			result: resultType{
				parts: []string{"0: 4090…4095"},
				reads: 6,
				err:   nil,
			},
		},
		{
			name: "4090, 10: read 10 recs from tail of first partition and head of second",
			iter: logIterate(4090, 10),
			result: resultType{
				parts: []string{"0: 4090…4095", "1: 0…3"},
				reads: 10,
				err:   nil,
			},
		},
		{
			name: "4000, 10000: read 10’000 recs from tail of first partition, across second and third, and from head of fourth",
			iter: logIterate(4000, 10000),
			result: resultType{
				parts: []string{"0: 4000…4095", "1: 0…4095", "2: 0…4095", "3: 0…1711"},
				reads: 10000,
				err:   nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			parts := make([]string, 0)
			reads := int64(0)

			readPart := func(part int64, clustFrom, clustTo int16) error {
				parts = append(parts, fmt.Sprintf("%d: %d…%d", part, clustFrom, clustTo))
				return nil
			}

			iterator := func(part int64, clust int16) error {
				if part+int64(clust) >= logSize {
					return io.EOF
				}
				reads++
				return nil
			}

			err := tt.iter.iterate(readPart, iterator)

			if err != tt.result.err {
				t.Errorf("logIterateType.iterate() error = %v, want %v", err, tt.result.err)
			}

			require.Equal(tt.result.parts, parts, "logIterateType.iterate() parts = %v, want %v", tt.result.parts, parts)
			require.Equal(tt.result.reads, reads, "logIterateType.iterate() reads = %v, want %v", tt.result.reads, reads)
		})
	}
}
