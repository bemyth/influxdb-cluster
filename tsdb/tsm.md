
influxdb 数据结构及概念



---------------------------------------------------------------------------------------------------------------
【数据库逻辑】

database -> retentionPolicy -> shard group -> shard -> point


database            数据库名称     与所有数据库概念相同 一个数据库下可以有多个保留策略，且这些保留策略下的数据均不相同

retentionPolicy     保留策略，保留策略表示数据的过期时间，influxdb会自动检测该保留策略下的数据哪些已过期而删除。
                    一个保留策略下可以有多个shard group。retentionPolicy还指定了副本个数，但仅仅用于集群中。

shard group         保留策略的具体实现，每个shard group表示该保留策略下的一个时间段，属于哪一时间段的数据会写入
                    对应的shard group。同样的，在查询和删除的时候，可以根据时间来对某几个shard group进行操作

shard               一个shard group下可以有多个shard。单机状态下，副本个数为0，每个shard group下仅有一个shard便可以实现
                    整个数据库逻辑，但是在集群方案中。但在集群方案中，需要由多个shard来实现。

point               每个shard中存储的数据点



---------------------------------------------------------------------------------------------------------------
【point 逻辑】
——————————————————————————————————————————————————
|              |         |           |           |
|  measurement | tag set | field set | timestamp |
|              |         |           |           |
——————————————————————————————————————————————————

以一条请求的插入为例，我们指定database=mydb，retentionPolicy=six_month_rollup 写入数据
point本身属于某一database,retentionPolicy,shard group,shard

curl -X POST 'http://localhost:8086/write?db=mydb&rp=six_month_rollup'
--data-binary 'cpu,vendor=Intel,type=i3, temperature=72,useage=0.98 timestamp=1435362189575692182'

measurement  可以理解为表名，是一种测量指标，cpu即为measurement名
tag          这个测量指标的某些标签，也称维度列。值得注意的是维度列的所有值都是string形式存储，
             即 3，“3”，‘3’ 都是不一样的tag值，且都是string。vendor，type为tag的key，Intel，i3，为其对应的tag value
field        field为数值列，存放实际的时序数据
timestamp    时间戳



-------------------------------------------------------------------------------------------------------------
【实际存储】

为了减少数据信息，influxdb在存储时实际只存储field和timestamp的值信息。tag set以及feild set作为索引存储。
为了压缩数据，每条数据的每个field和timestamp，都作为独立的列顺序存储，并单独压缩。
——————————  —————————
| value  |  |  time |
| value  |  |  time |
| value  |  |  time |
| value  |  |  time |
| value  |  |  time |
| value  |  |  time |
| value  |  |  time |
| value  |  |  time |
——————————  —————————


为了将实际存存储的值和tag set对应起来，就需要利用tag set的索引。




---------------------------------------------------------------------------------------------------------------
【series】
series  也称时间线，就是一个数据源的一个指标随着时间推移而源源不断产生的数据。
数据源以及其指标是固定的，但其产生的数据值和时间是一一对应且变化的
所以我们把measurement + tag set的组合作为一个serieskey

而seriesvalue 当然就是这些时间线下产生的数据 也就是fields + timestamps

serieskey   = measurement + tags
seriesvalue = fieldvalue  + timestamp



------------------------------------------------------------------------------------------------------

我们将一个serieskey对应的具体存储的数值称为series data block；
由于一个series中的数值列值（field）不止一个，且同一个数值列数据量也有可能过大，
所以同一个serieskey可能存在好几个series data block，每个series data block表示同一个一个serieskey下同一个数值列的值

[series data block]
|————————————————————————|
|         Type           |
|————————————————————————|
|         Length         |
|————————————————————————|
|       Timestamps       |
|————————————————————————|
|         Values         |
|————————————————————————|
解释：Type     标示该field的类型，支持很多，float64，string等
     Length   表示该存储的数据量
     value    标示值，不同类型的值编码方式不同




--------------------------------------------------------------------------------------------------
如何定位到这些series data block，我们需要series index block。
由于serieskey下有不同的field，注意是field，不是fields。
所以我们将serieskey + fieldkey作为索引的主键，series index block如下



[series index block]
——————————————————————
    Index Entry
——————————————————————
       .....
——————————————————————
    Index Entry
——————————————————————
   Index Block Meta
——————————————————————


[Index Block Meta]
——————————————————————
     Key Length
——————————————————————
        Key
——————————————————————
       Type
——————————————————————
       Count
——————————————————————


[Index Entry]
——————————————————————
        Min Time
——————————————————————
        Max Time
——————————————————————
        Offset
——————————————————————
        Size
——————————————————————

**
series block meta ：key表示serieskey + fieldkey，即当前series index block记录的是该series下该field在存储结构中的位置
                    其他几个字段尚不明确，应该做解析用

Index Entry       ：在找到当前block meta的情况下，进一步根据时间范围，确定是哪一个index entry，
                    然后根据Offset找到该serieskey+fieldkey在data block存储的位置
----------------------------------------------------------------------------------------------------


所以整个tsm结构如下所示

【tsm结构】
——————————————————————
    data block set
——————————————————————
    index block set
——————————————————————
         footer
——————————————————————