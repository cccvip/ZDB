package storage

import (
	"ZDB/bitcask"
	"errors"
	"fmt"
	"os"
)

var (
	ReadMissDataErr  = errors.New("miss data during read")
	WriteMissDataErr = errors.New("miss data during write")
	DeleteEntryErr   = errors.New("read an entry which had deleted")
	MissOldFileErr   = errors.New("miss old file error")
	CrcErr           = errors.New("crc error")
)

const (
	FileSuffix = ".dat"
	B          = 1
	KB         = 1024 * B
	MB         = 1024 * KB
	GB         = 1024 * MB
)

// 定义文件操作
type oldFile struct {
	fd *os.File
}

type oldFiles map[int]*oldFile

func newOldFiles() oldFiles {
	return oldFiles{}
}

// 定义bitcask存储格式
type DataFiles struct {
	//存储路径
	dir string
	//已经存储文件ID
	oIds []int
	//可写入文件阈值
	segementsize int
	//活跃文件
	active  *ActiveFile
	oIdFile oldFiles
}

// 激活文件
type ActiveFile struct {
	//文件描述符
	fid int
	//文件
	fd *os.File
	//下标
	offset int64
}

func newActiveFile(dir string, fid int) (af *ActiveFile, err error) {
	path := getFilePath(dir, fid)
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	af = &ActiveFile{
		fd:     fd,
		offset: fi.Size(),
		fid:    fid,
	}
	return af, nil
}

// 写
func (af *ActiveFile) writeEntity(e bitcask.Entry) (h *bitcask.Hint, err error) {
	buf := e.Encode()
	n, err := af.fd.WriteAt(buf, af.offset)
	if n < len(buf) {
		return nil, WriteMissDataErr
	}
	if err != nil {
		return nil, err
	}
	h = &bitcask.Hint{Fid: af.fid, Offset: af.offset}
	af.offset += e.Size()
	return h, nil
}

// 从 active table 读数据
func (af *ActiveFile) ReadEntity(off int64, length int) (e *bitcask.Entry, err error) {
	return readEntry(af.fd, off, length)
}

func readEntry(fd *os.File, off int64, length int) (e *bitcask.Entry, err error) {
	buf := make([]byte, length)
	n, err := fd.ReadAt(buf, off)
	if n < length {
		return nil, ReadMissDataErr
	}
	if err != nil {
		return nil, err
	}
	e = bitcask.NewEntry()
	e.DecodeMeta(buf[:bitcask.MetaSize])
	e.DecodePlayload(buf[bitcask.MetaSize:])
	return e, nil
}

func getFilePath(dir string, fid int) string {

	return fmt.Sprintf("%s/%d%s", dir, fid, FileSuffix)

}
