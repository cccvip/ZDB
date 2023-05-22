package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	MetaSize   = 29
	DeleteFlag = 1
)

type Hint struct {
	Offset int64
	Fid    int
}

// 空数据
func NewEntry() *Entry {
	e := &Entry{Meta: &Meta{}}
	return e
}

func NewEntryWithData(key []byte, value []byte) *Entry {
	e := &Entry{}
	e.Key = key
	e.Value = value
	e.Meta = &Meta{
		TimeStamp: uint64(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
	return e
}

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
func (e *Entry) Encode() []byte {
	size := e.Size()
	buf := make([]byte, size)
	//size 8
	binary.LittleEndian.PutUint64(buf[4:12], e.Meta.position)
	//size 8
	binary.LittleEndian.PutUint64(buf[12:20], e.Meta.TimeStamp)
	//size 4
	binary.LittleEndian.PutUint32(buf[20:24], e.Meta.KeySize)
	//size 4
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

// 解码元数据
func (e *Entry) DecodeMeta(meta []byte) {
	e.Meta.Crc = binary.LittleEndian.Uint32(meta[0:4])
	e.Meta.position = binary.LittleEndian.Uint64(meta[4:12])
	e.Meta.TimeStamp = binary.LittleEndian.Uint64(meta[12:20])
	e.Meta.KeySize = binary.LittleEndian.Uint32(meta[20:24])
	e.Meta.ValueSize = binary.LittleEndian.Uint32(meta[24:28])
}

// 解码消息体
func (e *Entry) DecodePlayload(playload []byte) {
	keyHighBound := int(e.Meta.KeySize)
	valueHighBound := keyHighBound + int(e.Meta.ValueSize)
	e.Key = playload[0:keyHighBound]
	e.Value = playload[keyHighBound:valueHighBound]
}

// 获取一条消息的size
func (e *Entry) Size() int64 {

	return int64(MetaSize + e.Meta.KeySize + e.Meta.ValueSize)

}

// 获得编码
func (e *Entry) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}
