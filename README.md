# 数据

##  数据定义

数据分为两部分组成,一部分是元数据定义,另外一部分是数据定义(key/value). 元数据中包含数据的定义信息例如 crc 校验数据的是否篡改。

## 定义长度

首先为了考虑内存对齐,对系统性能的影响。因此在声明size大小的时候 采用2的幂定义长度

```
CPU 只从对齐的地址开始加载数据（这一点可能遗漏,需要注意）
CPU 读取块的大小是固定的，通常为2的整数幂次
```

## 定义Entry
![image](image/crg.png)



# 存储

- 根据论文,数据会分层存储,只存在一个活跃的table(write),其它都是 old table（read）。
- 并且会使用索引存储最新的数据,实现o(1)的数据查找。

## 内存索引

> 方便代码实现,使用hashmap

key: 需要查找的key
value: 存储的value以及坐标信息


定义
```golang
type indexer map[string]*DataPosition

//key存储路径
type KeyDir struct {
	Index indexer
}
```

> CRUD操作


## 磁盘存储


```golang
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
```

### Active Table存储

> 可读可写的文件权限

- 操作文件
- 写入文件
- 读取文件

### Old Table存储

> 修改当前文件的操作权限

> 重新生成一份新的文件

```
func rotate(){

}
```

# DB操作

不考虑任何所学的数据库,从我们的常识思考。为了保持高并发下的数据正确性，最简单的操作应该是加锁

使用读写锁

- 在读数据的时候,使用读锁
- 在写数据的,使用写锁,不允许修改,允许读



还有细节正在coding中,主要参考代码地址(https://github.com/elliotchenzichang/tiny-bitcask)[https://github.com/elliotchenzichang/tiny-bitcask]






Bitcask简洁、优雅的key/value存储引擎
在关系数据库存储上，Btree一直是主角，但在某些情况下，log(n)的读写操作并不是总是让人满意。 Bitcask是一种连续写入很快速的Key/Value数据存储结构，读写操作的时间复杂度近似常量。

Bitcask连续写入操作

Bitcask具有高效的连续写入操作，连续写操作类似向log文件追加记录，因此Bitcask也被称作是日志结构存储。

Bitcask将存储对象的key、value分别存储：

在内存中对key创建索引
磁盘文件存储value数据
当有数据需要写入时，磁盘无需遍历文件，直接写入到数据块或者文件的末尾，避免了磁盘机械查找的时间，写入磁盘之后，只需要在内存的HashMap中更新相应的索引,内存中用HashMap来保存一条记录的索引部分，一条索引包含的信息如下：

[Key: Jason, Filename: employee.db, Offset:0, Size:146, ModifiedDate:2343432312]

[Key: Bill, Filename: employee.db, Offset:146, Size:146, ModifiedDate:5489354345]

Key表示一条记录的主键，查找通过它在HashMap中找到完整索引信息 Filename是磁盘文件名字，通过它和Offset找到Value在磁盘的开始位置 Offset是Value在文件中偏移量，通过它和Size可以读取一条记录 Size是Value所占的磁盘大小，单位是Byte 假设目前数据库中已有上述两条的记录，当我要写入key为 "Jobs"， value为: object的一条新记录时， 只需要在文件employee.db的末尾写入value=object，在HashMap中添加索引：[Key: Jobs, Filename: employee.db, Offset:292, Size:146, ModifiedDate:9489354343] 即可。

最后数据库就包含了三条索引信息：

[Key: Jason, Filename: employee.db, Offset:0, Size:146, ModifiedDate:2343432312]
[Key: Bill, Filename: employee.db, Offset:146, Size:150, ModifiedDate:5489354345]
[Key: Jobs, Filename: employee.db, Offset:294, Size:136, ModifiedDate:948965443]
Bitcask随机读取操作

由于数据在内存当中使用HashMap作为索引，查找索引的时间为Hash查找的时间，近似常量。比如查找Bill，直接通过Key就可以找到它的索引信息，再根据索引信息，找到value在文件位置和大小，精确读取出bytes，反序列化成value对象。 当然在value存入文件时需要序列化内存对象成bytes。磁盘读取的过程的时间复杂度也是常量， 并不会随时数据的增大而增大。

Bitcask 数据删除和更新

一条记录包含了索引和数据两个部分,删除索引容易，但要彻底的删除数据不是件容易的事情（参考磁盘空间整理）。对于更新数据，Bitcask通常采用的策略是append一条新数据，并更新已有的索引，至于旧数据则在清理数据的时候把它删除掉。

Bitcask适合的场景

适合连续写入，随机的读取，连续读取性能不如Btree；
记录的key可以完全的载入内存；
value的大小比key大很多,否则意义不大；





















