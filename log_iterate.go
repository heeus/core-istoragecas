/*
 * Copyright (c) 2021-present Sigma-Soft, Ltd.
 * @author: Nikolay Nikitin
 */

package istoragecas

import istructs "github.com/heeus/core-istructs"

// logReadPartFuncType: function type to read log partition (4096 recs). Must return ok and nil error to read next part.
type logReadPartFuncType func(part int64, clustFrom, clustTo int16) (ok bool, err error)

// readLogParts: In a loop, reads events from the log in partitions (by 4096 events) using call readPart()
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
