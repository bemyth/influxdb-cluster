


对于查询来说，我们不能提供完整的serieskey和fieldskey，那么只根据部分信息查询符合条件的数据点我们需要额外的解决方案

在tsm文件中我们可以根据serieskey + seriesfield找到我们需要的列，那么考虑如下查询语句

select * from cpu
我们的信息只有一个measurement。我们需要根据这个measurement找到所有数据点，influxdb给出了倒排索引，称之为TimeSeriesIndex

[time series index]
——————————————————————
    Series Block
——————————————————————
    Tag Block
——————————————————————
        ...
——————————————————————
    Tag Block
——————————————————————
  Measurement Block
——————————————————————
  Index File Trailer
——————————————————————

Index File Trailer ：存储Measurement Block|Tag Block|Series Block的位置和大小
Measurement Block  ：存储所measurement的信息
Tag Block          ：存储所有Tag的信息
Series Block       ：存储所有的Series信息
这些信息具体是什么，下边解释


---------------------------------------------------------------------------------

首先看一下measurement block
[Measurement Block]
————————————————————
    Measurement
————————————————————
      ......
————————————————————
    Measurement
————————————————————
    Hash Index
————————————————————
   Block Trailer
————————————————————

Hash Index : 就是一个map结构，map【measurement-name】measurement-offset
             根据名称找到对应的measurement



[Measurement]
————————————————————
        Name
————————————————————
  Tag Block Offset
————————————————————
  Tag Block Size
————————————————————
      Series n
————————————————————
    Series data
————————————————————

其中series n       :标示series data数量

Series data       :是一系列seriesID的集合，如果没有其他的限定条件，仅根据measurement查询，我们就可以得到所有的seriesid信息
                    seriesID 是serieskey哈希后的值，为了方便存储,而seriesid其实就是该series在seriesBlock中的偏移量。在不考虑其他条件
                    的情况下，我们便可以从这里得到所有该measurement下所有seriesid，然后可以从series block中获取serieskey
Tag Block Offset  :当前measurement所述的Tags在tag block中的偏移量。



-----------------------------------------------------------------------------------------------------------------
然后看一下tag block
tag block中存储的都是同一measurement的tags信息

每个tag key对应一个tag value set
key 和 value 都通过索引确定位置，而不通过遍历

[Tag Block]
————————————————————
    Tag Value Set
————————————————————
   Value Hash Index
————————————————————
      ......
————————————————————
    Tag Value Set
————————————————————
   Value Hash Index
————————————————————
    Tag Key Set
————————————————————
   Key Hash Index
————————————————————
    Block Trailer
————————————————————

Block Trailer    ：存储Tag Block中各部分的入口和大小，方便定位
Key Hash Index   ：维度名对应的tag key，方便查找
Value Hash Index ：某一维度明的维度值对应的tag value，方便查找
Tag Key Set      ：一系列tag key的集合，存储了维度名和当前维度值在TagValue set的偏移量。
Tag Value Set    ：一系列Tag value的集合，存储当前维度值对应的seriesId

备注：hash index只能判断维度名等于维度值即根据host=localhost，我们可以很方便的找到该维度值下所有的seriesid
     但当我们的条件为host > localhost时便无法确定，只能遍历查找，实际上，tag的值都是string类型的，所以不存在这个问题

------------------------------------------------------------------------------------------------------------------

经过以上的理解，我们便可以根据部分信息确定一系列符合条件的seriesid，那么接下来的问题就是seriesid到serieskey的映射
[Series Block]
——————————————————
  SeriesKeyChunk
——————————————————
      ......
——————————————————
  SeriesKeyChunk
——————————————————
  SeriesIndexChunk
——————————————————
   Bloom Filter
——————————————————
   Block Trailer
——————————————————

Block Trailer       ：其他结构在该文件块中的偏移及大小
Bloom Filter        ：用来表征给定的seriesKey是否存在
Series Index Chunk  ：是一个b+树结构，根据seriesId值判断查找方向，利用其中hashIndex定位到series key chunk
Series Key Chunk    ：一系列seriesKey的集合
---------------------------------------------------------------------------------------------------------------------

由此，利用tsi文件系统。可以根据部分信息，快速的查找符合条件的serieskey
根据这个serieskey,便可以去tsm文件系统中，结合field和时间条件，查询需要的数据