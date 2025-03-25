/*
	分布式决策树的master结点，协调各worker进行工作
*/

package call

import (
	"context"
	"errors"
	"github.com/apache/thrift/lib/go/thrift"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/gen-go/rpc"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/rpc_type_trans"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/remote"
	"sort"
	"sync"
)

// AttrInfo 属性的一些信息，每次划分时会进行更新
type AttrInfo struct {
	min        float64
	max        float64
	nanWeights float64
	values     []float64 // 这一项只在不进行区间划分时有有效值
}

type PartitionInsInfo struct {
	singleCount uint64
	multiCount  uint64
	weightsSum  float64
}

// MasterMem master结点的内存中需要存放的一些结构，统一进行管理。
// 这么看是不是不封在结构里会好一点，用函数内的局部变量就好，如果用结构的话，到时候就调这个结构的方法来查看运行情况。
type MasterMem struct {
	etcdEndPoints []string // etcdEndPoints 还是要存一下etcd的地址
	// worker相关信息记录
	workers []remote.Obj // workers 记录各个worker的信息，能够访问到各worker，fixme:这个到后面执行任务的时候再去获取吧，看看会不会有同步问题，应该不会
	// split流程相关控制，这些信息还是放在这里，因为master进行rpc调用worker时，该worker不会直接返回结果的(只是通知开始某一阶段了)，要和其他worker一起完成之后，各自返回各自的。
	// 所以还是等worker自己rpc调用master的方法来更新吧，那么就要在这里存一些信息了

	//splitId         uint64             // splitId 每一次对树中结点的划分都有个id，标志着对哪个结点进行划分，每次划分完之后左右结点的id分别为'id << 1'和'(id << 1) +1'
	allInsWeightSum float64          // allInsWeightSum 所有实例的权重和
	bestSplit       tree.SplitRecord // bestSplit 记录最优的一次划分
	finishedFeature chan int32       // finishedFeature 有哪些特征做完了，用这个参数来同步等待，每次split时自己创建

	mu sync.RWMutex // 这一块应该会涉及并发相关的，到时候注意
}

func (mem *MasterMem) _getWorkers() (workers []remote.Obj, err error) {
	// fixme:这里先不加锁，因为获取workers的话肯定是在执行某一流程，这些流程应该是顺序的
	if len((*mem).workers) == 0 {
		etcdClient := remote.NewEtcdClient((*mem).etcdEndPoints)
		defer etcdClient.Close()

		workerMap := map[string]string(nil)
		workerMap, err = etcdClient.GetWithPrefix(WORKER_PREFIX_ETCD)
		if err != nil {
			return
		}
		if len(workerMap) == 0 {
			err = errors.New("workers not registered in etcd!! ")
			return
		}
		workers = make([]remote.Obj, 0, len(workerMap))
		for _, v := range workerMap {
			workers = append(workers, remote.Obj{Addr: v})
		}
		sort.Slice(workers, func(i, j int) bool {
			return workers[i].Addr < workers[j].Addr
		})
		(*mem).workers = workers
	} else {
		workers = (*mem).workers
	}

	return
}

// ManagerOnMaster master端管理，外部通过 ManagerOnMaster 对Split流程进行控制
type ManagerOnMaster struct {
	mem *MasterMem
}

// DataFrameReset 重置一下dataFrame，不然不能重新跑
func (m *ManagerOnMaster) DataFrameReset(worker *ServerOnWorker) (err error) {
	//workers, err := m.mem._getWorkers()
	//if len(workers) == 0 || err != nil {
	//	log.Error().Msgf("workers not available!! %v", err)
	//	return
	//}
	//workerNum := len(workers)
	//workerClients := getWorkerClients(workers)
	//defer func() {
	//	// 关闭连接
	//	for _, client := range workerClients {
	//		client.Close()
	//	}
	//}()
	//if workerNum != len(workerClients) {
	//	// 可能有些没连上，这种情况当错误处理，那其实上面那直接返回也行的
	//	err = errors.New("some workers fail to start! ")
	//	return
	//}
	//
	//wg := &sync.WaitGroup{}
	//for _, worker := range workerClients {
	//	wg.Add(1)
	//	go func(client *rpc.ServeOnWorkerClient) {
	//		defer wg.Done()
	//		err := client.DataInit(context.TODO())
	//		if err != nil {
	//			log.Error().Msgf("error in DataFrameReset: %v", err)
	//			return
	//		}
	//	}(worker.c)
	//}
	//wg.Wait()
	//return

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.DataInit(context.TODO())
		if err != nil {
			logger.Errorf("error in DataFrameReset: %v", err)
			return
		}
	}()
	wg.Wait()
	return
}

// Split 进行一次split操作
func (m *ManagerOnMaster) Split(
	worker *ServerOnWorker,
	parentId int64, isLeft bool, newId int64,
	nodeImpurity float64,
	maxFeatureNum uint32, minSamplesInSplit int,
	parentFeatures []tree.FeatureId,
	split *tree.SplitRecord, statistic *tree.StatisticInfo,
	stopTask bool) {
	split.Init()
	statistic.Init()

	//workers, err := m.mem._getWorkers()
	//if len(workers) == 0 || err != nil {
	//	log.Error().Msgf("workers not available!! %v", err)
	//	return
	//}
	//workerNum := len(workers)
	//workerClients := getWorkerClients(workers)
	//defer func() {
	//	// 关闭连接
	//	for _, client := range workerClients {
	//		client.Close()
	//	}
	//}()
	//if workerNum != len(workerClients) {
	//	// 可能有些没连上，这种情况当错误处理，那其实上面那直接返回也行的
	//	return
	//}
	defer func() {
		// 因为下面有很多提前返回的，这个就放到defer里
		//for _, client := range workerClients {
		//	err := client.c.AfterSplit(context.TODO())
		err := worker.AfterSplit(context.TODO())
		if err != nil {
			logger.Errorf("error in split cleaning: %v", err)
		}
		//}

	}()

	wg := &sync.WaitGroup{}
	curPartition := &rpc.PartitionRef{
		SplitId: parentId,
		IsLeft:  isLeft,
	}
	partitionInfoCh := make(chan *rpc.PartitionInsBasic, WorkerNum)
	// 先统计实例的基本信息
	//log.Debug().Msgf("<split on %d> start to collect ins info!", newId)
	//for _, worker := range workerClients {
	//	wg.Add(1)
	//	go func(client *rpc.ServeOnWorkerClient) {
	//		defer wg.Done()
	//		basicInfo, err := client.CollectInsBasicInfo(context.Background(), curPartition, rpc_type_trans.FeatureListToThrift(parentFeatures))
	//		if err != nil {
	//			log.Error().Msgf("error in CollectInsBasicInfo: %v", err)
	//			return
	//		}
	//		partitionInfoCh <- basicInfo
	//	}(worker.c)
	//}
	wg.Add(1)
	go func() {
		defer wg.Done()
		basicInfo, err := worker.CollectInsBasicInfo(context.Background(), curPartition, rpc_type_trans.FeatureListToThrift(parentFeatures))
		if err != nil {
			logger.Errorf("error in CollectInsBasicInfo: %v", err)
			return
		}
		partitionInfoCh <- basicInfo
	}()
	go func() {
		wg.Wait()
		close(partitionInfoCh)
	}()
	partitionInfo := (*rpc.PartitionInsBasic)(nil)
	for info := range partitionInfoCh {
		partitionInfo = mergePartitionInsBasic(partitionInfo, info)
	}
	if partitionInfo == nil {
		return
	}
	fillStatisticInfo(statistic, partitionInfo) // 统计完实例信息就可以填充了
	if partitionInfo.MultiCount < int64(minSamplesInSplit) {
		// 现在这里是非NaN的，就是所有相关属性都非NaN的才计入这个Count里
		return
	}

	if parentId == -1 {
		// 是第一次划分，有所有的实例
		m.mem.allInsWeightSum = partitionInfo.Weights
	}
	if nodeImpurity < tree.EPSILON {
		return
	}

	// 收集一下各属性的基本信息
	//log.Debug().Msgf("<split on %d> start to attr basic info!", newId)
	//logger.Debugf("worker:%p, split on %d, start to attr basic info!", worker, newId)
	attrInfoCh := make(chan map[int32]*rpc.AttrBasic, WorkerNum)
	//for _, worker := range workerClients {
	//	wg.Add(1)
	//	go func(client *rpc.ServeOnWorkerClient) {
	//		defer wg.Done()
	//		attrInfo, err := client.CollectAttrBasicInfo(context.Background(), curPartition)
	//		if err != nil {
	//			log.Error().Msgf("error in CollectAttrBasicInfo: %v", err)
	//			return
	//		}
	//		attrInfoCh <- attrInfo
	//	}(worker.c)
	//}
	wg.Add(1)
	go func() {
		defer wg.Done()
		attrInfo, err := worker.CollectAttrBasicInfo(context.Background(), curPartition)
		if err != nil {
			logger.Errorf("error in CollectAttrBasicInfo: %v", err)
			return
		}
		attrInfoCh <- attrInfo
	}()

	go func() {
		wg.Wait()
		close(attrInfoCh)
	}()
	attrInfo := map[int32]*rpc.AttrBasic(nil)
	for info := range attrInfoCh {
		attrInfo = mergeAttrBasic(attrInfo, info)
	}
	if len(attrInfo) == 0 {
		return
	}

	// 开始划分了
	//log.Debug().Msgf("<split on %d> start to get best split!!", newId)
	//logger.Debugf("worker:%p, split on %d, start to get best split!", worker, newId)
	nonEmptyInfo := make(map[int32]AttrInfo, len(attrInfo))
	nonNaNlabelInfo := make(map[int32]map[float64]float64, len(attrInfo))
	// fixme:先不分批了，之后再说
	selectedFeatureNum := uint32(0)
	smallAVCs := make(map[int32][]float64, len(attrInfo))
	conciseAVCs := make(map[int32][]float64, len(attrInfo))
	for attr, info := range attrInfo {
		if info.Empty {
			// 其中Empty的属性要去掉
			continue
		}
		if info.Max-info.Min < tree.EPSILON {
			// 常量，跳过
			// 这里常量不再计入选择的特征中了，简单一点
			continue
		}
		selectedFeatureNum += 1
		if selectedFeatureNum > maxFeatureNum {
			// 达到最大特征数限制
			break
		}
		nonNaNlabelInfo[attr] = info.ValidClassWeightCount
		nonEmptyInfo[attr] = AttrInfo{
			min:        info.Min,
			max:        info.Max,
			nanWeights: info.NaNWeights,
			values:     info.Values,
		}
		if len(info.Values) != 0 {
			// 有限
			smallAVCs[attr] = info.Values
		} else {
			// 要划分一下区间
			// todo:现在是简单的等间距边界，之后可以用那个quantiles
			borders := divideIntervals(info.Min, info.Max, IntervalNum)
			if len(borders) == 0 {
				// 可能倒也没有必要减？
				selectedFeatureNum--
				delete(nonEmptyInfo, attr)
				continue
			}
			conciseAVCs[attr] = borders
		}
	}

	//for _, client := range workerClients {
	//	err = client.c.BeforeGenAVC(context.TODO(), nonNaNlabelInfo)
	err := worker.BeforeGenAVC(context.TODO(), nonNaNlabelInfo)
	if err != nil {
		logger.Errorf("error in before-gen-avc: %v", err)
	}
	//}
	m.mem.bestSplit.Init()
	m.mem.finishedFeature = make(chan int32, len(nonEmptyInfo))
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.GenGeneralAVC(context.TODO(), curPartition, smallAVCs, conciseAVCs)
		if err != nil {
			logger.Errorf("error in gen-general-avc: %v", err)
		}
	}()
	//for _, client := range workerClients {
	//	// 这里不同机器之间可以并发
	//	wg.Add(1)
	//	go func(client *_clientToWorker) {
	//		defer wg.Done()
	//		// todo:到时候还是分批来做，好像也没太大必要？因为各机器是串行执行的
	//		err := client.c.GenGeneralAVC(context.TODO(), curPartition, smallAVCs, conciseAVCs)
	//		if err != nil {
	//			log.Error().Msgf("error in gen-general-avc: %v, %s", err, client.addr)
	//		}
	//	}(client)
	//}
	// 等worker来更新
	for i := 0; i < len(nonEmptyInfo); i++ {
		if stopTask {
			return
		}
		// 等待所有属性完成
		<-m.mem.finishedFeature
	}
	// 这里要等两步，不然可能这里的client调用还没完全结束就开始下面的调用了
	wg.Wait()

	//logger.Debugf("worker:%p, split on %d, all features finish getting best split!", worker, newId)
	*split = m.mem.bestSplit
	if !split.Valid() {
		//log.Warn().Msg("split is still invalid after attrs splitting")
		return
	}
	split.Improvement = split.Improvement / m.mem.allInsWeightSum

	// 通知各worker进行划分
	//log.Debug().Msgf("<split on %d> start to split on workers!!", newId)
	//logger.Debugf("worker:%p, split on %d, start to split!", worker, newId)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := worker.Split(context.TODO(), curPartition, newId, int32(split.Feature), split.SplitValue, attrInfo[int32(split.Feature)].NaNWeights != 0)
		if err != nil {
			logger.Errorf("error in Split: %v", err)
		}
	}()
	wg.Wait()
	//log.Debug().Msgf("<split on %d> end split on workers!!", newId)
}

func (m *ManagerOnMaster) GetService() *ServerOnMaster {
	return &ServerOnMaster{
		mem: (*m).mem,
	}
}

// ServerOnMaster master端的一些服务，供他人远程调用
type ServerOnMaster struct {
	mem    *MasterMem     // mem 保存运行过程中各个阶段的信息
	server thrift.TServer // server 保存一个server，主要用来停止服务
}

func (s *ServerOnMaster) SetServer(server thrift.TServer) {
	(*s).server = server
}

func (s *ServerOnMaster) GetManager() *ManagerOnMaster {
	return &ManagerOnMaster{
		mem: (*s).mem,
	}
}

func NewManager() *ManagerOnMaster {
	return &ManagerOnMaster{
		mem: &MasterMem{},
	}
}

func NewServer() *ServerOnMaster {
	return &ServerOnMaster{
		mem: &MasterMem{},
	}
}

func NewServerManager(etcdEndPoints []string) (*ServerOnMaster, *ManagerOnMaster) {
	mem := &MasterMem{
		etcdEndPoints: etcdEndPoints,
	}
	return &ServerOnMaster{mem: mem}, &ManagerOnMaster{mem}
}

func (s *ServerOnMaster) UpdateSplitInfo(ctx context.Context, record *rpc.SplitRecord) (_err error) {
	if record.ImprovementProxy > s.mem.bestSplit.Improvement {
		s.mem.bestSplit = *rpc_type_trans.SplitRecordFromThrift(record)
	}
	s.mem.finishedFeature <- record.Feature
	return
}

func (s *ServerOnMaster) Stop(ctx context.Context) (_err error) {
	if (*s).server != nil {
		(*s).server.Stop()
		(*s).server = nil
	}
	// todo:清一下资源、etcd等，可以把name换成etcd那里的key对应的
	panic("")
}

type _clientToMaster struct {
	// todo:这个client当初好像还想加点什么来着，可以考虑把name、addr什么的放这里
	addr string // addr 这个addr是所连接的远端的master的addr
	c    *rpc.ServeOnMasterClient
	sock *thrift.TSocket
}

func (c *_clientToMaster) Close() error {
	if c.sock != nil {
		return c.sock.Close()
	}
	return nil
}

func getMasterClient(master remote.Obj) *_clientToMaster {
	transFactory, protoFactory := thrift.NewTTransportFactory(), thrift.NewTBinaryProtocolFactoryConf(nil)
	sock := thrift.NewTSocketConf(master.Addr, &thrift.TConfiguration{})
	transport, err := transFactory.GetTransport(sock)
	if err != nil {
		logger.Errorf("get client transport failed %v", err)
		return nil
	}
	if err = transport.Open(); err != nil {
		logger.Errorf("client transport open to %v failed %v", master.Addr, err)
		return nil
	}
	client := rpc.NewServeOnMasterClientFactory(transport, protoFactory)
	return &_clientToMaster{
		addr: master.Addr,
		c:    client,
		sock: sock,
	}
}
