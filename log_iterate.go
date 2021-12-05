/*
 * Copyright (c) 2021-present Sigma-Soft, Ltd.
 * @author: Nikolay Nikitin
 */

package istoragecas

import (
	"context"

	"github.com/gocql/gocql"
	istorage "github.com/heeus/core-istorage"
	istructs "github.com/heeus/core-istructs"
)

// logReadPartFuncType: function type to read log part (≤ 4096 events). Must return ok and nil error to read next part.
type logReadPartFuncType func(part int64, clustFrom, clustTo int16) (ok bool, err error)

// readLogParts: In a loop: reads events from the log by parts (≤ 4096 events) using specified readPart function
func readLogParts(startOffset istructs.Offset, toReadCount int, readPart logReadPartFuncType) error {
	finishOffset := int64(startOffset) + int64(toReadCount) - 1
	if toReadCount == istructs.ReadToTheEnd {
		finishOffset = int64(istructs.ReadToTheEnd)
	}

	minPart, minClust := crackOffset(startOffset)
	maxPart, maxClust := crackOffset(istructs.Offset(finishOffset))

	for part := minPart; part <= maxPart; part++ {
		clustFrom := int16(0)
		if part == minPart {
			clustFrom = minClust
		}
		clustTo := int16(LowMask)
		if part == maxPart {
			clustTo = maxClust
		}

		ok, err := readPart(part, clustFrom, clustTo)

		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}

	return nil
}

// logReadPartQueryFuncType: constructs and return new query to read event records from log [part] in range [clustFrom, clustTo]
type logReadPartQueryFuncType func(part int64, clustFrom, clustTo int16) (query *gocql.Query)

// readLog: reads (i.e. calls the cb function) from event log toReadCount records at offset using the specified readQuery constructor
func readLog(ctx context.Context, offset istructs.Offset, toReadCount int, readQuery logReadPartQueryFuncType, cb istorage.LogReaderCallback) (err error) {
	return readLogParts(offset, toReadCount,
		func(part int64, clustFrom, clustTo int16) (ok bool, err error) {
			iter := readQuery(part, clustFrom, clustTo).Iter()

			readed := 0
			for clust, event := clustFrom, make([]byte, 0); iter.Scan(&clust, &event); readed++ {
				if ctx.Err() != nil {
					iter.Close()
					return false, nil // breaked from context
				}
				e := make([]byte, len(event))
				copy(e, event)
				if err = cb(istructs.Offset(uncrackOffset(part, clust)), e); err != nil {
					iter.Close()
					return false, err // breaked from callback
				}
			}

			if err = iter.Close(); err != nil {
				return false, err // breaked by query error. Panic?
			}

			return readed > 0, nil // false if nothing to read
		})
}
