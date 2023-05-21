package db

import (
	"ZDB/bitcask"
	"ZDB/bitcask/storage"
	"sync"
)

type DB struct {
	rw      sync.RWMutex
	index   *bitcask.KeyDir
	storage *storage.DataFiles
}
