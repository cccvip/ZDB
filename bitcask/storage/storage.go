package storage

import (
	"ZDB/bitcask"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
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
	segementsize int64
	//活跃文件
	active  *ActiveFile
	oIdFile oldFiles
}

// 初始化
func NewDataFileWithFiles(dir string, segmentSize int64) (dfs *DataFiles, err error) {
	dfs = &DataFiles{
		dir:          dir,
		oIdFile:      newOldFiles(),
		segementsize: segmentSize,
	}

	fids, err := getFids(dir)
	if err != nil {
		return nil, err
	}
	aFid := fids[len(fids)-1]
	dfs.active, err = NewActiveFile(dir, aFid)
	if err != nil {
		return nil, err
	}
	if len(fids) == 1 {
		return dfs, nil
	}
	oldFids := fids[:len(fids)-1]
	for _, fid := range oldFids {
		path := getFilePath(dir, fid)
		reader, err := NewOldFile(path)
		if err != nil {
			return nil, err
		}
		dfs.oIdFile[fid] = reader
	}

	return dfs, nil
}

// 打开只读文件
func NewOldFile(path string) (of *oldFile, err error) {
	fd, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	of = &oldFile{fd: fd}
	return of, nil
}

// NewDataFiles create a DataFiles Object with an empty dir
func NewDataFiles(path string, segmentSize int64) (dfs *DataFiles, err error) {
	err = os.Mkdir(path, os.ModePerm)
	if err != nil {
		return nil, err
	}
	af, err := NewActiveFile(path, 1)
	if err != nil {
		return nil, err
	}
	dfs = &DataFiles{
		dir:          path,
		oIds:         nil,
		active:       af,
		oIdFile:      map[int]*oldFile{},
		segementsize: segmentSize,
	}
	return dfs, nil
}

// 读取path下所有文件描述符
func getFids(dir string) (fids []int, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		fileName := f.Name()
		filePath := path.Base(fileName)
		if path.Ext(filePath) == FileSuffix {
			filePrefix := strings.TrimSuffix(filePath, FileSuffix)
			fid, err := strconv.Atoi(filePrefix)
			if err != nil {
				return nil, err
			}
			fids = append(fids, fid)
		}
	}
	return fids, nil
}

func (d *DataFiles) GetOldFile() []int {
	return d.oIds
}

func (d *DataFiles) GetOldFileFid(fid int) *oldFile {
	return d.oIdFile[fid]
}

// 把旧的文件整个删除
func (dfs *DataFiles) RemoveFile(fid int) error {
	of := dfs.oIdFile[fid]
	err := of.fd.Close()
	if err != nil {
		return err
	}
	path := getFilePath(dfs.dir, fid)
	err = os.Remove(path)
	if err != nil {
		return err
	}
	delete(dfs.oIdFile, fid)
	return nil
}

// 解析存储的消息块
func (of *oldFile) ReadEntityWithOutLength(offset int64) (e *bitcask.Entry, err error) {
	e = &bitcask.Entry{Meta: &bitcask.Meta{}}
	buf := make([]byte, bitcask.MetaSize)
	n, err := of.fd.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if n < bitcask.MetaSize {
		return nil, ReadMissDataErr
	}
	offset += bitcask.MetaSize
	e.DecodeMeta(buf)
	payloadSize := e.Meta.KeySize + e.Meta.ValueSize
	buf = make([]byte, payloadSize)
	n, err = of.fd.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if n < int(payloadSize) {
		return nil, ReadMissDataErr
	}
	e.DecodePlayload(buf)
	return e, nil
}

func (d *DataFiles) WriterEntity(e *bitcask.Entry) (h *bitcask.Hint, err error) {
	h, err = d.active.WriteEntity(e)
	if err != nil {
		return nil, err
	}
	if d.canRotate() {
		err := d.rotate()
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

// 按照文件位置 读取内容
func (dfs *DataFiles) ReadEntry(index *bitcask.DataPosition) (e *bitcask.Entry, err error) {
	dataSize := bitcask.MetaSize + index.KeySize + index.ValueSize
	if index.Fid == dfs.active.fid {
		return dfs.active.ReadEntity(index.Offet, dataSize)
	}
	of, exist := dfs.oIdFile[index.Fid]
	if !exist {
		return nil, MissOldFileErr
	}
	return of.ReadEntity(index.Offet, dataSize)
}

// 判断是否当前空间足够存放,否则就重新开辟一块空间。====重新写一个文件
func (dfs *DataFiles) canRotate() bool {
	return dfs.active.offset > dfs.segementsize
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

func NewActiveFile(dir string, fid int) (af *ActiveFile, err error) {
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

func (of *oldFile) ReadEntity(off int64, length int) (e *bitcask.Entry, err error) {
	return readEntry(of.fd, off, length)
}

// 写
func (af *ActiveFile) WriteEntity(e *bitcask.Entry) (h *bitcask.Hint, err error) {
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

// 压缩
func (dfs *DataFiles) rotate() error {
	aFid := dfs.active.fid
	path := getFilePath(dfs.dir, aFid)
	fd, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	r := &oldFile{fd: fd}
	dfs.oIdFile[dfs.active.fid] = r
	dfs.oIds = append(dfs.oIds, aFid)

	af, err := NewActiveFile(dfs.dir, aFid+1)
	if err != nil {
		return err
	}
	dfs.active = af
	return nil
}
