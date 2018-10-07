# SimpleDB
Database Homework of Berkeley University: Implement A Simple Database Management System 

## Details
**You can get more details in https://sites.google.com/site/cs186fall2013/homeworks**
proj1-proj4依次实现了数据存储、数据操作、查询优化和事务管理，每个project是在上一个project的基础上完成的，所以本项目的完整代码在proj4。
### 存储
	每个table存储了多个page，page中有位图与用来存放tuple的slots，位图记录相应slot是否被占用，在添加和删除tuple时修改相应bit（采用大端法）。每个table对应一个磁盘文件，读取磁盘上指定的page时需要计算字节偏移量。本数据库支持缓存，使用LRU替换算法。
### 操作
	支持常用数据库操作，并实现了基于代价估算的查询优化，其中对于每个table构造直方图计算Filter Selectivity。用动态规划算法得到left-deep tree，join时采用了排序合并算法。
### 事务
	事务管理实现了NO STEAL/FORCE，因此不需要事务的重做与撤销。事务锁实现了严格两段锁协议，粒度为page，使用了资源等待图法进行死锁检测。
