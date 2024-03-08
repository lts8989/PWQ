package slot

import "encoding/binary"

// Entry 写入文件的记录
type Entry struct {
	Key            []byte
	FuncName       []byte
	FuncParams     []byte
	KeySize        uint32
	FuncNameSize   uint32
	FuncParamsSize uint32
	Mark           uint8
}

// getSize 文件记录的二进制块的大小
func (e *Entry) GetSize() uint32 {
	return entryHeaderSize + e.KeySize + e.FuncNameSize + e.FuncParamsSize
}

// Encode 文件记录对象转为二进制块
func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetSize())
	var begin, end uint32
	begin = 0
	end = e.KeySize
	copy(buf[begin:end], e.Key)

	begin = end
	end += e.FuncNameSize
	copy(buf[begin:end], e.FuncName)

	begin = end
	end += e.FuncParamsSize
	copy(buf[begin:end], e.FuncParams)

	idx := end
	binary.BigEndian.PutUint32(buf[idx:idx+4], e.KeySize)
	binary.BigEndian.PutUint32(buf[idx+4:idx+8], e.FuncNameSize)
	binary.BigEndian.PutUint32(buf[idx+8:idx+12], e.FuncParamsSize)
	binary.BigEndian.PutUint16(buf[idx+12:idx+14], uint16(e.Mark))

	return buf, nil
}

// newEntry 创建新的文件记录
func newEntry(key, funcName, funcParam []byte, mark uint8) *Entry {
	return &Entry{
		Key:            key,
		FuncName:       funcName,
		FuncParams:     funcParam,
		KeySize:        uint32(len(key)),
		FuncNameSize:   uint32(len(funcName)),
		FuncParamsSize: uint32(len(funcParam)),
		Mark:           mark,
	}
}

// Decode 从文件中读出的二进制解析为 Entry
func Decode(buf []byte) (*Entry, error) {
	keySize := binary.BigEndian.Uint32(buf[0:4])
	funcNameSize := binary.BigEndian.Uint32(buf[4:8])
	funcParamsSize := binary.BigEndian.Uint32(buf[8:12])
	mark := binary.BigEndian.Uint16(buf[12:14])
	return &Entry{
		KeySize:        keySize,
		FuncNameSize:   funcNameSize,
		FuncParamsSize: funcParamsSize,
		Mark:           uint8(mark),
	}, nil
}
