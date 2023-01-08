package bigcache

import "encoding/binary"

const (
	timestampLen = 8
	hashLen      = 8
	keySizeLen   = 2
)

func warpEntry(k, v []byte, timestamp uint64, hashIndex uint64, buffer *[]byte) []byte {

	blobLen := timestampLen + hashLen + keySizeLen + len(k) + len(v)
	blob := *buffer

	binary.LittleEndian.PutUint64(blob, timestamp)
	binary.LittleEndian.PutUint64(blob[timestampLen:], hashIndex)
	binary.LittleEndian.PutUint16(blob[timestampLen+hashLen:], uint16(len(k)))
	copy(blob[timestampLen+hashLen+keySizeLen:], k)
	copy(blob[timestampLen+hashLen+keySizeLen+len(k):], v)

	return blob[:blobLen]

}

func readEntry(entry []byte) ([]byte, []byte, uint64, uint64) {
	timeStamp := binary.LittleEndian.Uint64(entry[:timestampLen])
	hashIndex := binary.LittleEndian.Uint64(entry[timestampLen : timestampLen+hashLen])
	keyLen := binary.LittleEndian.Uint16(entry[timestampLen+hashLen : timestampLen+hashLen+keySizeLen])

	k := make([]byte, keyLen)
	v := make([]byte, len(entry)-timestampLen-hashLen-keySizeLen-int(keyLen))
	copy(k, entry[timestampLen+hashLen+keySizeLen:timestampLen+hashLen+keySizeLen+keyLen])
	copy(v, entry[timestampLen+hashLen+keySizeLen+keyLen:])
	return k, v, timeStamp, hashIndex
}

func readEntryTimestamp(entry []byte) uint64 {
	timeStamp := binary.LittleEndian.Uint64(entry[:timestampLen])
	return timeStamp
}

func readEntryHash(entry []byte) uint64 {
	hashIndex := binary.LittleEndian.Uint64(entry[timestampLen:timestampLen+hashLen])
	return hashIndex
}