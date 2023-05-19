# 数据

##  数据定义

数据分为两部分组成,一部分是元数据定义,另外一部分是数据定义(key/value). 元数据中包含数据的定义信息例如 crc 校验数据的是否篡改。

## 定义长度

首先为了考虑内存对齐,对系统性能的影响。因此在声明size大小的时候 采用 2的幂定义长度

```
CPU 只从对齐的地址开始加载数据（这一点可能遗漏了）
CPU 读取块的大小是固定的，通常为 B 的 2 的整数幂次
```

## 定义Entry
![image](image/crg.png)











