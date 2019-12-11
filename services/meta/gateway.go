package meta

type Gateway interface {
	//WaitPeers()
	SyncCacheData(dir string)
	SyncNodeState(dir string)
}

// Deprecated: give up this function
//func (c *Client) WaitPeers() {
//	var wg sync.WaitGroup
//	kv := clientv3.NewKV(c.c)
//	for NodeID, _ := range c.Peers {
//		wg.Add(1)
//		sid := uint64ToString(NodeID)
//		dir := combineDir(sid, DefaultProxyNodeDir)
//		c.logger.Info(fmt.Sprintf("Wait node %s go online %s", sid, dir))
//		go func() {
//			defer wg.Done()
//			for true {
//				getResp, err := kv.Get(c.ctx, dir)
//				if err != nil {
//					c.logger.Error("Get peers error")
//					return
//				}
//				if len(getResp.Kvs) == 1 {
//					c.Peers[NodeID] = string(getResp.Kvs[0].Value)
//					c.logger.Info(fmt.Sprintf("Detected node %s is already online", sid))
//					return
//				}
//				time.Sleep(1 * time.Second)
//			}
//		}()
//	}
//
//	wg.Wait()
//
//	c.logger.Info(fmt.Sprintf("All peers have already online"))
//}
//
//func (c *Client) SyncCacheData(dir string) {
//	watch := clientv3.NewWatcher(c.c)
//	//watch cache data from current version
//	watchChan := watch.Watch(c.ctx, dir)
//	for watchResp := range watchChan {
//		for _, event := range watchResp.Events {
//			switch event.Type {
//			case mvccpb.PUT:
//				{
//					c.mu.Lock()
//					data := &internal.Data{}
//					if err := proto.Unmarshal(event.Kv.Value, data); err != nil {
//						c.logger.Error("Read error CacheData format")
//						continue
//					}
//					d := c.cacheData.Clone()
//					d.unmarshal(data)
//					c.version = watchResp.Header.GetRevision()
//					c.cacheData = d
//					c.logger.Info("Detected new cache data and update finished")
//					//if c.version >= d.PreVersion {
//					//	c.version = watchResp.Header.GetRevision()
//					//	c.cacheData = d
//					//	c.logger.Info("Detected new cache data and update finished")
//					//} else {
//					//	c.logger.Error("Detected old cache data and ignore it")
//					//}
//					c.mu.Unlock()
//				}
//			default:
//				{
//				}
//			}
//		}
//	}
//}
//
////
//func (c *Client) SyncNodeState(dir string) {
//	watch := clientv3.NewWatcher(c.c)
//	//watch cache data from current version
//	watchChan := watch.Watch(c.ctx, dir, clientv3.WithPrefix())
//	for watchResp := range watchChan {
//		for _, event := range watchResp.Events {
//			switch event.Type {
//			case mvccpb.PUT:
//				{
//					uid, err := AgentDirToNodeID(event.Kv.Key)
//					if err != nil {
//						c.logger.Info("unknow message format in etcd")
//						//fmt.Println(fmt.Sprintf("unknow message type"))
//						continue
//					}
//					c.NodeState[uid] = Online
//					c.logger.Info(fmt.Sprintf("node %d has online", uid))
//					//fmt.Println(fmt.Sprintf("node %d has online",uid))
//				}
//			case mvccpb.DELETE:
//				{
//					uid, err := AgentDirToNodeID(event.Kv.Key)
//					if err != nil {
//						c.logger.Info("unknow message format in etcd")
//						//fmt.Println(fmt.Sprintf("unknow message type"))
//						continue
//					}
//					c.NodeState[uid] = Offline
//					c.logger.Info(fmt.Sprintf("node %d has offline", uid))
//					//fmt.Println(fmt.Sprintf("node %d has offline",uid))
//				}
//			default:
//				{
//					c.logger.Info("unknow event type in etcd")
//					//fmt.Println(fmt.Sprintf("unknow message type"))
//				}
//			}
//		}
//	}
//}
//func uint64ToString(id uint64) string {
//	return strconv.Itoa(int(id))
//}
//func combineDir(sid string, pdir string) string {
//	return strings.TrimRight(pdir, "/") + "/" + sid
//}
//func stringToUint64(sid string) (uint64, error) {
//	return strconv.ParseUint(sid, 10, 64)
//}
//func AgentDirToNodeID(key []byte) (uint64, error) {
//	s := string(key)
//	sl := strings.Split(s, "/")
//	sid := sl[len(sl)-1]
//
//	uid, err := strconv.ParseUint(sid, 10, 64)
//	if err != nil {
//		return 0, errors.New("error message read in etcd")
//	}
//	return uid, nil
//}
