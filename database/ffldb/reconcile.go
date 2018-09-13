package ffldb

import (
	"fmt"
	"hash/crc32"

	"github.com/hawkit/btcd-demo/database"
)

// The serialized write cursor location format is
//
// [0:4] Block file name (4 bytes)
// [4:8] File offset (4 bytes)
// [8:12] Castagnoli CRC-32 checksum (4 bytes)
//
// serializeWriteRow serialize the current block file and offset
// where new will be written into a format suitable for storage
// into metadata
func serializeWriteRow(curBlockFileNum, curFileOffset uint32) []byte {
	var serializeRow [12]byte
	byteOrder.PutUint32(serializeRow[0:4], curBlockFileNum)
	byteOrder.PutUint32(serializeRow[4:8], curFileOffset)
	checksum := crc32.Checksum(serializeRow[0:8], castagnoli)
	byteOrder.PutUint32(serializeRow[8:12], checksum)
	return serializeRow[:]
}

// deserializeWriteRow deserializes the write cursor location stored in the
// metadata.  Returns ErrCorruption if the checksum of the entry doesn't match.
func deserializeWriteRow(writeRow []byte) (uint32, uint32, error) {
	// Ensure the checksum matches. The checksum is at the end
	wantChecksum := byteOrder.Uint32(writeRow[8:12])
	gotChecksum := crc32.Checksum(writeRow[:8], castagnoli)
	if wantChecksum != gotChecksum {
		str := fmt.Sprintf("metadata for write cursor dose not math "+
			"the expected checksume - got %d, want %d", gotChecksum, wantChecksum)
		return 0, 0, makeDbErr(database.ErrInvalid, str, nil)
	}
	fileNum := byteOrder.Uint32(writeRow[:4])
	fileOffset := byteOrder.Uint32(writeRow[4:8])
	return fileNum, fileOffset, nil
}
