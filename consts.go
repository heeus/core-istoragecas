/*
 * Copyright (c) 2021-present unTill Pro, Ltd.
 */

package istoragecas

import "time"

// ConnectionTimeout s.e.
const ConnectionTimeout = 30 * time.Second
const Attempts = 5

const (
	PartitionBits        = 12
	LowMask              = (1 << PartitionBits) - 1
	PartitionRecordCount = 1 << PartitionBits
)
