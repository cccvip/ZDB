package db

import (
	"ZDB/bitcask"
	"ZDB/bitcask/storage"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
)

var (
	KeyNotFoundErr   = errors.New("key not found")
	NoNeedToMergeErr = errors.New("no need to merge")
)

type DB struct {
	rw      sync.RWMutex
	kd      *bitcask.KeyDir
	storage *storage.DataFiles
	opt     *bitcask.Options
}

// 创建一个DB NewDB
func NewDB(opt *bitcask.Options) (db *DB, err error) {
	db = &DB{}
	db.kd = bitcask.NewKD()
	db.opt = opt
	if isExist, _ := isDirExist(opt.Dir); isExist {
		if err := db.recovery(opt); err != nil {
			return nil, err
		}
		return db, nil
	}
	var fileSize = getSegmentSize(opt.SegmentSize)
	db.storage, err = storage.NewDataFiles(opt.Dir, fileSize)
	if err != nil {
		return nil, err
	}
	return db, err
}

// 当前用户目录是否存在
func isDirExist(dir string) (bool, error) {
	_, err := os.Stat(dir)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// 获取segment size
func getSegmentSize(size int64) int64 {
	var fileSize int64
	if size <= 0 {
		fileSize = bitcask.DefaultSegmentSize
	} else {
		fileSize = size
	}
	return fileSize
}

// 如果是第一次启动,默认会读取文件目录,初始化
func (db *DB) recovery(opt *bitcask.Options) (err error) {
	fileSize := getSegmentSize(opt.SegmentSize)
	db.storage, err = storage.NewDataFileWithFiles(opt.Dir, fileSize)
	if err != nil {
		return err
	}
	fids := db.storage.GetOldFile()
	//排序
	sort.Ints(fids)
	for _, fid := range fids {
		var off int64 = 0
		reader := db.storage.GetOldFileFid(fid)
		//循环读 一直读取完数据
		for {
			entry, err := reader.ReadEntityWithOutLength(off)
			if err == nil {
				db.kd.AddIndexByRawInfo(fid, off, entry.Key, entry.Value)
				off += entry.Size()
			} else {
				if err == storage.DeleteEntryErr {
					continue
				}
				if err == io.EOF {
					break
				}
				return err
			}
		}
	}
	return err
}

// Set sets a key-value pairs into DB
func (db *DB) Set(key []byte, value []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	entry := bitcask.NewEntryWithData(key, value)
	h, err := db.storage.WriterEntity(entry)
	if err != nil {
		return err
	}
	db.kd.AddIndexByData(h, entry)
	return nil
}

// Get gets value by using key
func (db *DB) Get(key []byte) (value []byte, err error) {
	db.rw.RLock()
	defer db.rw.RUnlock()
	i := db.kd.Find(string(key))
	if i == nil {
		return nil, KeyNotFoundErr
	}
	entry, err := db.storage.ReadEntry(i)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

// Delete delete a key
func (db *DB) Delete(key []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	keyStr := string(key)
	index := db.kd.Find(keyStr)
	if index == nil {
		return KeyNotFoundErr
	}
	e := bitcask.NewEntry()
	e.Meta.Flag = bitcask.DeleteFlag
	_, err := db.storage.WriterEntity(e)
	if err != nil {
		return err
	}
	db.kd.Delete(keyStr)
	return nil
}
