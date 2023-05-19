package bitcask

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	MetaSize   = 29
	DeleteFlag = 1
)

// key value meta元数据
type Entry struct {
	Key   []byte
	Value []byte
	Meta  *Meta
}

// 元数据
type Meta struct {
	//校验
	Crc uint32
	//时间戳
	TimeStamp uint64
	//存放位置
	position uint64
	//key 长度
	KeySize uint32
	//value 长度
	ValueSize uint32
	//标记
	Flag uint8
}

// 数据进行encode
// 在计算机内部，小端序被广泛应用于现代性 CPU 内部存储数据；而在其他场景譬如网络传输和文件存储使用大端序。
func (e *Entry) encode() []byte {
	size := e.size()
	buf := make([]byte, size)
	//8
	binary.LittleEndian.PutUint64(buf[4:12], e.Meta.position)
	//8
	binary.LittleEndian.PutUint64(buf[12:20], e.Meta.TimeStamp)
	//4
	binary.LittleEndian.PutUint32(buf[20:24], e.Meta.KeySize)
	//4
	binary.LittleEndian.PutUint32(buf[24:28], e.Meta.ValueSize)
	buf[28] = e.Meta.Flag

	if e.Meta.Flag != DeleteFlag {
		copy(buf[MetaSize:MetaSize+len(e.Key)], e.Key)
		copy(buf[MetaSize+len(e.Key):MetaSize+len(e.Key)+len(e.Value)], e.Value)
	}
	c32 := crc32.ChecksumIEEE(buf[4:])

	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

func (e *Entry) size() int64 {

	return int64(MetaSize + e.Meta.KeySize + e.Meta.ValueSize)

}
