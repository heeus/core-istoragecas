/*
 * Copyright (c) 2021-present Sigma-Soft, Ltd.
 * @author: Nikolay Nikitin
 */

package istoragecas

import istructs "github.com/heeus/core-istructs"

// crackOffset returns low and hi part of value
func crackOffset(value istructs.Offset) (hi int64, lo int16) {
	return int64(value >> PartitionBits), int16(value & LowMask)
}

// uncrackOffset s.e.
func uncrackOffset(hi int64, low int16) istructs.Offset {
	return istructs.Offset((hi << PartitionBits) | int64(low))
}
