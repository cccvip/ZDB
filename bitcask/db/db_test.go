package db

import (
	"ZDB/bitcask"
	"ZDB/bitcask/storage"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BitCaskTest(t *testing.T, opt *bitcask.Options, test func(db *DB)) {
	if opt == nil {
		opt = bitcask.DefaultOptions
	}
	db, err := NewDB(opt)
	assert.NoError(t, err)
	test(db)
	os.RemoveAll(opt.Dir)
}

func TestDB_Base(t *testing.T) {
	var test = func(db *DB) {
		err := db.Set([]byte("test_key"), []byte("test_value"))
		assert.NoError(t, err)
		value, err := db.Get([]byte("test_key"))
		assert.NoError(t, err)
		assert.Equal(t, "test_value", string(value))

		err = db.Set([]byte("test_key"), []byte("test_value_2"))
		assert.NoError(t, err)

		value, err = db.Get([]byte("test_key"))
		assert.NoError(t, err)
		assert.Equal(t, "test_value_2", string(value))
	}
	BitCaskTest(t, nil, test)
}

func TestDB_SegmentSize(t *testing.T) {
	opt := &bitcask.Options{
		Dir:         "db",
		SegmentSize: 4 * storage.KB,
	}
	var test = func(db *DB) {
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("test_key_%d", i)
			value := fmt.Sprintf("test_value_%d", i)
			err := db.Set([]byte(key), []byte(value))
			assert.NoError(t, err)
		}
	}
	BitCaskTest(t, opt, test)
}

func TestDB_Merge(t *testing.T) {
	opt := &bitcask.Options{
		Dir:         "db",
		SegmentSize: 4 * storage.KB,
	}
	var test = func(db *DB) {
		key := "test_key"
		for i := 0; i < 1000; i++ {
			value := fmt.Sprintf("test_value_%d", i)
			err := db.Set([]byte(key), []byte(value))
			assert.NoError(t, err)
		}
		err := db.Merge()
		assert.NoError(t, err)
		value, err := db.Get([]byte("test_key"))
		assert.NoError(t, err)
		assert.Equal(t, "test_value_999", string(value))
	}
	BitCaskTest(t, opt, test)
}
