1.compile statement

主要两部分(1):表达式的简化，规范化
         (2):全局条件的定义。比如时间范围，是否仅查询，指定查询的列名等等

2.prepare statement
(1).确定需要查询的field的类型 需要用到shard mapper，需集群化处理
(2).创建一个迭代器参数结构，用于在各个shard上进行迭代。单机版只需要本地执行，拿到本地的shard，集群上需要将shardid和迭代器参数发送给不同的node

3.select statement
（1）根据上一步提到的shard mapper和迭代器参数，在每个shard上构建迭代器。每个迭代器返回一列数据
（2）生成scannerIterator
（3）生成Scanner

所以主要是实现shard mapper,整个查询逻辑是清楚的