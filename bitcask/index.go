package bitcask

//定义接口 CRUD
type Index interface {
	Find(key string) *DataPosition
	Delete(key string)
	Update(key string, dp *DataPosition)
	Add(key string, dp *DataPosition)
}

//存储位置
type DataPosition struct {
	//文件
	Fid int
	//文件下标
	Offet int64
	//时间戳
	TimeStamp uint64
	//key的size
	KeySize int
	//value的size
	ValueSize int
}

type indexer map[string]*DataPosition

//key存储路径
type KeyDir struct {
	Index indexer
}

//Find
func (k *KeyDir) Find(key string) *DataPosition {
	return k.Index[key]
}

//Add
func (kd *KeyDir) Add(key string, dp *DataPosition) {
	kd.Index[key] = dp
}

// Update
func (kd *KeyDir) Update(key string, dp *DataPosition) {
	kd.Index[key] = dp
}

// Delete
func (kd *KeyDir) Delete(key string) {
	delete(kd.Index, key)
}

func (kd *KeyDir) AddIndexByData(hint *Hint, entry *Entry) {
	kd.AddIndexByRawInfo(hint.Fid, hint.Offset, entry.Key, entry.Value)
}

func (kd *KeyDir) AddIndexByRawInfo(fid int, off int64, key, value []byte) {
	index := newDataPosition(fid, off, key, value)
	kd.Add(string(key), index)
}

func newDataPosition(fid int, off int64, key, value []byte) *DataPosition {
	dp := &DataPosition{}
	dp.Fid = fid
	dp.Offet = off
	dp.KeySize = len(key)
	dp.ValueSize = len(value)
	return dp
}

func (i *DataPosition) IsEqualPos(fid int, off int64) bool {
	return i.Offet == off && i.Fid == fid
}
