/*
 * Copyright (c) 2021-present Sigma-Soft, Ltd.
 * @author: Nikolay Nikitin
 */

package istoragecas

// crackOffset returns low and hi part of value
func crackOffset(value int64) (hi int64, lo int16) {
	return int64(value >> PartitionBits), int16(value & LowMask)
}

// uncrackOffset s.e.
func uncrackOffset(hi int64, low int16) (original int64) {
	return (hi << PartitionBits) | int64(low)
}
