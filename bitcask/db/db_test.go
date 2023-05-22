package db

import (
	"ZDB/bitcask"
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
