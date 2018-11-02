# SimpleDB
Database Homework of Berkeley University: Implement A Simple Database Management System 
**(You can get more details in https://sites.google.com/site/cs186fall2013/homeworks)**

## Details
proj1-proj4依次实现了数据存储、数据操作、查询优化和事务管理，每个project是在上一个project的基础上完成的，所以本项目的完整代码在proj4。
### 存储
每个table存储了多个page，page中有位图与用来存放tuple的slots，位图记录相应slot是否被占用，在添加和删除tuple时修改相应bit（采用大端法）。每个table对应一个磁盘文件，读取磁盘上指定的page时需要计算字节偏移量。本数据库支持缓存，使用LRU替换算法。
### 操作
支持常用数据库操作，并实现了基于代价估算的查询优化。用动态规划算法得到left-deep tree，join时采用了排序合并算法。
### 事务
事务管理实现了NO STEAL/FORCE，因此不需要事务的重做与撤销。事务锁实现了严格两段锁协议，粒度为page，使用了资源等待图法进行死锁检测。

## 本人改动的部分
在cache中使用了LRU算法，以双端链表实现。
在多表联接中使用了排序合并算法。
使用了资源等待图法判断死锁。

## 课程原有的Bug
没有指明page number是从0开始的整数。
在加载table文件时默认是绝对路径，没有处理相对路径的情况。
默认数据库表的catalog必须和.dat文件同一目录下。
JoinOptimizer.computeCostAndCardOfSubplan中，当cost2<cost1时，没有交换card
在一些测试中，本该使用table的alias时未使用
