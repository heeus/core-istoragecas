/*
 * Copyright (c) 2021-present Sigma-Soft, Ltd.
 * @author: Nikolay Nikitin
 */

package istoragecas

type (
	// logReadPartFuncType: function type to read log partition (4096 recs)
	logReadPartFuncType func(part int64, clustFrom, clustTo int16) error

	// logIteratorFuncType: function type to iterate one readed log event
	logIteratorFuncType func(part int64, clust int16) error

	// logIterateType: iterate type. Organizes log reading in partitions (by 4096 events) and iteration for each readed event
	logIterateType struct {
		startOffset, finishOffset int64
	}
)

// logIterate: new iterate
func logIterate(startOffset int64, toRead int64) *logIterateType {
	return &logIterateType{startOffset, startOffset + toRead - 1}
}

// iterate: In a loop, reads events from the log in partitions (by 4096 events) using call readPart(), inside the loop a inner loop for each readed event to call iterator()
func (iter *logIterateType) iterate(readPart logReadPartFuncType, iterator logIteratorFuncType) error {
	minPart, minClust := crackOffset(iter.startOffset)
	maxPart, maxClust := crackOffset(iter.finishOffset)

	for part := minPart; part <= maxPart; part++ {
		clustFrom := int16(0)
		if part == minPart {
			clustFrom = minClust
		}
		clustTo := int16(LowMask)
		if part == maxPart {
			clustTo = maxClust
		}
		if err := readPart(part, clustFrom, clustTo); err != nil {
			return err
		}
		for clust := clustFrom; clust <= clustTo; clust++ {
			if err := iterator(part, clust); err != nil {
				return err
			}
		}
	}

	return nil
}
