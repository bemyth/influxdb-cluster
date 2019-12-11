
集群设计图  https://i.loli.net/2018/08/23/5b7e66182f06b.jpg  【--del https://sm.ms/delete/3yQUsHGSC4bOitq 请勿执行！！！】

【模块解释】
coordinator 包括读写操作，任一需集群执行的指令均转由meta client的server执行

meta client 【agent，gateway，server】 收到coordinator指令，分析指令如何执行，创建执行计划，将所计划交由rpc处理

rpc 收到上一层数据请求，转由coordinator处理，coordinator调度本地执行引擎执行后回传数据


【可行性设计】
启动流程：
step 1.注册：本地节点以注册的形式向etcd中心申请租约 /influxdb/agent/:nodeid rpchost --agent put
step 2.启动watch，监听远端etcd中的cachedata，若有put事件则更新本地cachedata  --gateway watch
step 3.首次拉取cachedata。若etcd中cachedata的nodeinfo中不包含本机，则添加本机nodeinfo，若包含本机，根据本机配置更新nodeinfo。--agent get
step 4.首次提交cachedata。 --agent put
step 5.正常情况下本地首次提交的cachedata会被所有节点的gateway监听到，引起所有节点cachedata的更新 --gateway sync

【测试考虑】
 1.引起cachedata更新的事件除每个节点的第一次上线外，其他均为commit函数，凡涉及create & drop的指令均会调用commit
 2.部分show指令可在单机情况下完成
 3.节点启动，可以随时新增节点
 4.每个节点id必须保证不同(暂时这样方便处理，可以做修改)，peers暂时无效直接忽略
 5.节点下线是危险行为，暂未做处理
 6.cachedata 结构为Data的proto编码
 7.synccachedata仅在收到put事件时对cachedata加锁