/*
	分布式决策树的worker结点，具体进行分布式各算法执行
*/

package call

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/gen-go/rpc"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/rpc_type_trans"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/cluster"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/format"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/remote"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/util/add"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/util/bitset"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/util/dataManager"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

var _gWorkerManager *ManagerOnWorker

func SetData(worker *ServerOnWorker, data *format.DataFrame, weights []float64) (err error) {
	key := fmt.Sprint(unsafe.Pointer(worker))
	value, exist := Worker2Manager.Get(key)
	if !exist {
		logger.Errorf("Can not find manager of worker %s", key)
		return &dataManager.MyError{Msg: fmt.Sprintf("Can not find manager of worker %s", key)}
	}
	_gWorkerManager := value.(*ManagerOnWorker)
	_gWorkerManager.DataReSet(data, weights)
	//// 通知etcd准备完成了
	//etcdClient := remote.NewEtcdClient(_gWorkerManager.w.etcdEndPoints)
	//defer etcdClient.Close()
	//
	//err = etcdClient.Put(WORKER_PREFIX_ETCD+_gWorkerManager.w.self.Name, _gWorkerManager.w.self.Addr)
	return
}

type State int8

const (
	INITIAL State = iota
	WORKING
	FINISH
)

// Partition 记录一个分区的实例id，算是顺序排好，每次划分之后会分成左右两块，考虑到nan，这里权重可能需要建立多份，不能再共用一份了
type Partition struct {
	left         []int
	leftWeights  []float64
	right        []int
	rightWeights []float64
}

type AttrKeepUnit struct {
	avc         *tree.AVC
	receivedNum int        // 收到了多少worker返回的结果
	mu          sync.Mutex // 这个锁分在不同的属性下好一点
}

func (unit *AttrKeepUnit) Update(other *tree.AVC) int {
	unit.mu.Lock()
	defer unit.mu.Unlock()

	unit.receivedNum += 1
	if unit.avc == nil {
		unit.avc = other
	} else {
		unit.avc.Merge(other)
	}

	return unit.receivedNum
}

// DataMem worker结点中存放数据的部分
type DataMem struct {
	data    *format.DataFrame // data 存放该机器上存有的实例，todo:这个结构到时候看看要不要改，如果不改的话，要获取离散属性和连续属性，暂时就先认为不改？
	weights []float64         // weights 各实例对应的权重，还是保存一份，这个保存之后是不变的
	// todo:然后有些和split有关的信息，要等那个结构定了
}

// WorkMem worker结点中进行各阶段任务执行需要保存的信息
type WorkMem struct {
	self           remote.Obj        // self 自己的地址
	master         *remote.Obj       // master 任务中的master地址，fixme:现在看来master是要每次建树的时候重置了，就放在重置dataFrame的时候吧，之后再改
	workers        []remote.Obj      // workers 记录一下一起执行一个任务的worker(包含自己)，需要传信息给对应的机器
	workerHashRing *cluster.HashRing // workerHashRing 能保证各个机器上的hash环是一样的吗，目前能保证workers是一样的，那应该是一样的

	// 换新的pattern时要重置，如果不创建新的worker来做的话
	partitionMap map[int64]Partition // partitionMap 记录一下，一个workId(全局统一)对应一块实例区间，因为对NaN的处理方式改了，所以不能一直复用之前的实例数组，对部分实例要存两份，注意及时清就好

	// beforeAVC时会更新
	attrNonNaNcount    map[int32]float64 // attrNonNaNcount 各属性的非NaN实例权重和
	attrNonNaNimpurity map[int32]float64 // attrNonNaNimpurity 各属性非NaN数据的impurity

	// afterSplit时会重置
	curPartition *rpc.PartitionRef       // curPartition 还是要记录一下，不然进行区间内的统计时不知道对哪块实例操作了，这个每次也要置零一下
	generalAttrs map[int32]*AttrKeepUnit // generalAttrs 该机器负责合并的属性，每次新的划分开始的时候要重置一下这个map

	etcdEndPoints []string // etcdEndPoints 还是要存一下etcd的地址

	// todo:暂时就先不要这个了，挺奇怪的
	// 存一下任务列表
	//state State
	//smallAVCTasks chan smallAVCTask
	//conciseAVCTasks chan conciseAVCTask
	//partialAVCTasks chan partialAVCTask

	criterion tree.Criterion // criterion 这个划分策略还是先放到这里，因为划分在这里

	mu sync.RWMutex
}

func (m *WorkMem) Init(d *DataMem) {
	data := d.data
	weights := make([]float64, len(d.weights))
	copy(weights, d.weights)
	insIdx := make([]int, data.Len())
	for i := 0; i < len(insIdx); i++ {
		insIdx[i] = i
	}
	m.partitionMap = map[int64]Partition{-1: {
		// 根节点的话左右一样的，但这里只存left，不然会影响垃圾回收
		left:         insIdx,
		leftWeights:  weights,
		right:        nil,
		rightWeights: nil,
	}}
	m.curPartition = nil
	m.generalAttrs = make(map[int32]*AttrKeepUnit)
	runtime.GC()
}

func (m *WorkMem) resetAfterSplit() {
	// 一次split结束之后，哪些要重置
	m.curPartition = nil
	m.generalAttrs = make(map[int32]*AttrKeepUnit)
}

func (m *WorkMem) _getWorkers() (workers []remote.Obj, err error) {
	// fixme:这里先不加锁，因为获取workers的话肯定是在执行某一流程，这些流程应该是顺序的
	if len((*m).workers) == 0 {
		etcdClient := remote.NewEtcdClient((*m).etcdEndPoints)
		defer etcdClient.Close()

		workerMap := map[string]string(nil)
		workerMap, err = etcdClient.GetWithPrefix(WORKER_PREFIX_ETCD)
		if err != nil {
			return
		}
		workers = make([]remote.Obj, 0, len(workerMap))
		for _, v := range workerMap {
			workers = append(workers, remote.Obj{Addr: v})
		}
		sort.Slice(workers, func(i, j int) bool {
			return workers[i].Addr < workers[j].Addr
		})
		(*m).workers = workers
	} else {
		workers = (*m).workers
	}

	return
}

func (m *WorkMem) _getMaster() (master remote.Obj, err error) {
	// fixme:这里先不加锁
	if (*m).master == nil {
		etcdClient := remote.NewEtcdClient((*m).etcdEndPoints)
		defer etcdClient.Close()

		masterAddr := ""
		has := false // etcd中是不是存在对应的key
		masterAddr, has, err = etcdClient.Get(MASTER_PREFIX_ETCD)
		if err != nil {
			return
		}
		if !has {
			err = errors.New("master not registered in etcd!! ")
			return
		}
		master.Addr = masterAddr
		(*m).master = &master
	} else {
		master = *(*m).master
	}
	return
}

func (m *WorkMem) _getHashRing() (ring *cluster.HashRing, err error) {
	// 其实拿到workers之后就可以构建了
	// fixme:这里也先不加锁
	if (*m).workerHashRing != nil {
		ring = (*m).workerHashRing
		return
	}
	workers, err := m._getWorkers()
	if err != nil {
		return
	}
	(*m).workerHashRing = cluster.NewHashRing()
	for _, worker := range workers {
		// fixme:还是都当ipv4来处理
		addr := strings.Split(worker.Addr, ":")
		(*m).workerHashRing.AddNodesOfLocalHost(addr[0], addr[1], WorkerDuplicateNumOnRing)
	}
	ring = (*m).workerHashRing
	return
}

func (m *WorkMem) _getPartition() *rpc.PartitionRef {
	m.mu.RLock() // 其实这里不用锁也没事的
	defer m.mu.RUnlock()
	return m.curPartition // 这里就不再做检查了
}

func (m *WorkMem) CheckPartition(p *rpc.PartitionRef) bool {
	m.mu.RLock()
	if m.curPartition == nil {
		m.mu.RUnlock()
		m.mu.Lock()
		if m.curPartition == nil {
			m.curPartition = p
		}
		m.mu.Unlock()
	} else {
		m.mu.RUnlock()
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return *m.curPartition == *p
}

func (m *WorkMem) GetInsByPartition(partition *rpc.PartitionRef) (insIdx []int, weights []float64, err error) {
	if p, has := m.partitionMap[partition.SplitId]; !has {
		err = fmt.Errorf("get partition failed! %d not in %v", partition.SplitId, m.partitionMap)
	} else {
		if partition.IsLeft {
			insIdx = p.left
			weights = p.leftWeights
		} else {
			insIdx = p.right
			weights = p.rightWeights
		}
	}
	return
}

// ManagerOnWorker worker端管理，外部通过 ManagerOnWorker 对worker进行重置等操作
type ManagerOnWorker struct {
	d *DataMem
	w *WorkMem
}

func (m *ManagerOnWorker) GetService() *ServerOnWorker {
	return &ServerOnWorker{
		d: (*m).d,
		w: (*m).w,
	}
}

func (m *ManagerOnWorker) DataReSet(data *format.DataFrame, weights []float64) {
	// 先设置data数据
	m.d.data = data
	m.d.weights = make([]float64, len(weights))
	copy(m.d.weights, weights) // 拷贝一份，防止被修改

	// 初始化workMem中的内容
	// fixme:看看要不要在这里调这个Init，因为如果外面会手动调用DataInit的话，好像没必要在这里Init
	//m.w.Init(m.d)

	// master重置，因为可能换新的
	m.w.master = nil // fixme:如果后续改了别的实现可能就不用清了
}

// ServerOnWorker worker的一些服务
type ServerOnWorker struct {
	d      *DataMem
	w      *WorkMem
	server thrift.TServer // server 保存一个server，主要用来停止服务
}

// todo:到时候像criterion这种参数封一个结构吧
func NewWorker(name, addr string, criterion tree.Criterion, etcdEndPoints []string) *ServerOnWorker {
	w := &WorkMem{
		self:          remote.Obj{Name: name, Addr: addr},
		criterion:     criterion,
		etcdEndPoints: etcdEndPoints,
		generalAttrs:  make(map[int32]*AttrKeepUnit),
	}
	d := &DataMem{}
	return &ServerOnWorker{
		d: d,
		w: w,
	}
}

func (s *ServerOnWorker) SetServer(server thrift.TServer) {
	(*s).server = server
}

func (s *ServerOnWorker) GetManager() *ManagerOnWorker {
	return &ManagerOnWorker{
		d: (*s).d,
		w: (*s).w,
	}
}

func (s *ServerOnWorker) DataInit(ctx context.Context) (_err error) {
	// 把workMem的信息重置一下
	s.w.Init(s.d)
	return
}

func (s *ServerOnWorker) CollectAttrBasicInfo(ctx context.Context, partition *rpc.PartitionRef) (_r map[int32]*rpc.AttrBasic, _err error) {
	if !s.w.CheckPartition(partition) {
		_err = errors.New("partition not consistent, when collect attr info")
		return
	}
	// 统计一下属性的一些基础信息
	insIdx := []int(nil)
	insWeights := []float64(nil)
	insIdx, insWeights, _err = s.w.GetInsByPartition(partition)
	if _err != nil {
		return
	}
	data := s.d.data
	features := data.GetXIndexList()
	featureNum := len(features)
	valueBuffer := make([]float64, len(insIdx)) // 缓存获取的value值
	labelBuffer := make([]float64, len(insIdx)) // 缓存获取的label值
	data.GetSpecificValueList(insIdx, data.GetYIndex(), labelBuffer)
	// 一些临时存储信息
	numeric := true
	empty := true
	overflow := false // 数值类型的取值数量是否超过限制 LimitedNumericValueNumLimit
	weight := 1.0
	nanWeights := add.NewFloatAdder() // nan的权重累和
	min := tree.INFINITY
	max := tree.NEG_INFINITY
	nonDuplicatedValues := make(map[float64]struct{}, LimitedNumericValueNumLimit)
	_r = make(map[int32]*rpc.AttrBasic, featureNum)

	for i := 0; i < featureNum; i++ {
		info := new(rpc.AttrBasic)
		labelWeightAdder := make(map[float64]*add.FloatAdder) // 正常来说，一次运行中label种类是固定的，这个好像也不用每次重新建立
		_r[int32(i)] = info
		numeric = data.IsNumeric(features[i])
		data.GetSpecificValueList(insIdx, features[i], valueBuffer)
		for insPid, v := range valueBuffer {
			if len(insWeights) > 0 {
				weight = insWeights[insPid]
			}
			if math.IsNaN(v) {
				nanWeights.Add(weight)
			} else {
				// 非NaN的，统计一下各label的权重和
				label := labelBuffer[insPid]
				if adder, has := labelWeightAdder[label]; has {
					adder.Add(weight)
				} else {
					adder = add.NewFloatAdder()
					adder.Add(weight)
					labelWeightAdder[label] = adder
				}
				empty = false // 只要有一项非NaN的，就是非空的
				if v < min {
					min = v
				}
				if v > max {
					max = v
				}
				if !overflow || !numeric {
					// 如果非数值类型就一直放就好
					nonDuplicatedValues[v] = struct{}{}
					if len(nonDuplicatedValues) > LimitedNumericValueNumLimit {
						overflow = true
					}
				}
			}
		}
		info.Empty = empty
		info.NaNWeights = nanWeights.Result()
		info.Min = min
		info.Max = max
		if !overflow || !numeric {
			// 把这些value存一下
			keepValues := make([]float64, 0, len(nonDuplicatedValues))
			for v := range nonDuplicatedValues {
				keepValues = append(keepValues, v)
			}
			sort.Float64s(keepValues) // 排个序
			info.Values = keepValues
		}
		info.ValidClassWeightCount = make(map[float64]float64, len(labelWeightAdder))
		for l, adder := range labelWeightAdder {
			info.ValidClassWeightCount[l] = adder.Result()
		}
		// 重置一下信息
		empty = true
		overflow = false
		nanWeights.Clear()
		min = tree.INFINITY
		max = tree.NEG_INFINITY
		nonDuplicatedValues = make(map[float64]struct{}, LimitedNumericValueNumLimit)
	}

	return
}

func (s *ServerOnWorker) CollectInsBasicInfo(ctx context.Context, partition *rpc.PartitionRef, relatedFeatures []int32) (_r *rpc.PartitionInsBasic, _err error) {
	if !s.w.CheckPartition(partition) {
		_err = errors.New("partition not consistent, when collect ins info")
		return
	}
	insIdx := []int(nil)
	insWeights := []float64(nil)
	insIdx, insWeights, _err = s.w.GetInsByPartition(partition)
	if _err != nil {
		return
	}
	data := s.d.data
	weight := 1.0
	weightsSum := add.NewFloatAdder()
	singleCount := bitset.NewBitSetByMapWithCap(0)
	multiCount := int64(0)
	classSingleCountMap := make(map[float64]*bitset.BitSetByMap)
	classMultiCount := make(map[float64]int64)
	y := make([]float64, len(insIdx))
	data.GetSpecificValueList(insIdx, data.GetYIndex(), y)

	featureCoding := data.GetXIndexList()
	invalidIds := make(map[int]struct{}) // 哪些实例是无效的，也就是在relatedFeatures中的属性值为NaN
	testFeatures := make([]float64, len(insIdx))
	for _, f := range relatedFeatures {
		data.GetSpecificValueList(insIdx, featureCoding[f], testFeatures)
		for i, insId := range insIdx {
			if math.IsNaN(testFeatures[i]) {
				invalidIds[insId] = struct{}{}
			}
		}
	}
	testFeatures = nil // help gc

	// fixme:再看看要不要一次把所有实例的pivot都给拿到
	for i, ins := range insIdx {
		// weight是计全部的
		if len(insWeights) > 0 {
			weight = insWeights[i]
		}
		weightsSum.Add(weight)
		// 其他统计信息是计非NaN的
		if _, in := invalidIds[ins]; in {
			continue
		}
		pivot := int(data.GetPivotWithId(ins))
		singleCount.SetBit(pivot)
		multiCount += 1
		// 再看各类的
		classMultiCount[y[i]] += 1
		if bitMap, has := classSingleCountMap[y[i]]; has {
			bitMap.SetBit(pivot)
		} else {
			bitMap = bitset.NewBitSetByMapWithCap(0)
			bitMap.SetBit(pivot)
			classSingleCountMap[y[i]] = bitMap
		}
	}

	classSingleCount := make(map[float64]int64, len(classSingleCountMap))
	for k, v := range classSingleCountMap {
		classSingleCount[k] = int64(v.Count())
	}
	_r = &rpc.PartitionInsBasic{
		SingleCount:      int64(singleCount.Count()),
		MultiCount:       multiCount,
		Weights:          weightsSum.Result(),
		ClassSingleCount: classSingleCount,
		ClassMultiCount:  classMultiCount,
	}

	return
}

// BeforeGenAVC 做一些初始化，传一些参数什么的
func (s *ServerOnWorker) BeforeGenAVC(ctx context.Context, nonNaNlabelWeights map[int32]map[float64]float64) (_err error) {
	s.w.attrNonNaNcount = make(map[int32]float64, len(nonNaNlabelWeights))
	s.w.attrNonNaNimpurity = make(map[int32]float64, len(nonNaNlabelWeights))
	for l, c := range nonNaNlabelWeights {
		impurity, countSum := s.w.criterion.Impurity(c)
		s.w.attrNonNaNcount[l] = countSum
		s.w.attrNonNaNimpurity[l] = impurity
	}
	return
}

// AfterSplit 做一些收尾清理工作，看要不要返回些什么
func (s *ServerOnWorker) AfterSplit(ctx context.Context) (_err error) {
	s.w.resetAfterSplit()
	return
}

// map无序，对key进行排序以使输出的路径稳定，否则选择最佳分裂时，当多个分裂的提升度有相同时，同样数据输入，输出的路径可能不同
func sortAVCTaskKeys(kvs map[int32][]float64) []int32 {
	keys := make([]int32, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func (s *ServerOnWorker) GenGeneralAVC(ctx context.Context, partition *rpc.PartitionRef, smallAVCTasks, conciseAVCTasks map[int32][]float64) (_err error) {
	if !s.w.CheckPartition(partition) {
		_err = errors.New("partition not consistent, when gen general-avc")
		return
	}
	insIdx := []int(nil)
	insWeights := []float64(nil)
	insIdx, insWeights, _err = s.w.GetInsByPartition(partition)
	if _err != nil {
		return
	}
	data := s.d.data
	featureCoding := data.GetXIndexList()
	featureValues := make([]float64, len(insIdx))
	labels := make([]float64, len(insIdx))
	//// 先把各worker拿到
	//workers, _err := s.w._getWorkers()
	//if len(workers) == 0 || _err != nil {
	//	log.Error().Msgf("workers not available!! %v", _err)
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
	//hashRing, _err := (*s).w._getHashRing()
	//if _err != nil {
	//	return
	//}
	// todo:到时候看看这里要不要整点并发，并发的话注意，client不能并发用，要建新的
	// todo:这里可以先把各属性(attrId)根据hash值确定发送到的机器，然后对属于一个机器的当作一个整体，不必多次建立连接，这样可能好一点
	// 先做concise的再做small的，因为concise分两步，可以增加并发度
	data.GetSpecificValueList(insIdx, data.GetYIndex(), labels)
	sortedConciseAVCTaskKeys := sortAVCTaskKeys(conciseAVCTasks)
	for _, featureId := range sortedConciseAVCTaskKeys {
		globalV := conciseAVCTasks[featureId]
		data.GetSpecificValueList(insIdx, featureCoding[featureId], featureValues)
		avc := tree.GenConciseAVC(tree.FeatureId(featureId), globalV, featureValues, labels, insWeights)
		// 根据featureId发送到对应机器上
		// todo:到时候对接hash环
		thriftAVC := rpc_type_trans.AVCtoThrift(avc)
		//target := hashRing.GetNodeByHashCode(cluster.CalcHash(strconv.Itoa(int(featureId))))
		//targetAddr := fmt.Sprintf("%s:%s", target.GetIp(), target.GetPort())
		//hit := false
		//for _, client := range workerClients {
		//	if client.addr == targetAddr {
		//		_err = client.c.MergeGeneralAVC(context.TODO(), partition, thriftAVC)
		//		hit = true
		//		break
		//	}
		//}
		//if _err != nil {
		//	return
		//}
		//if !hit {
		//	_err = fmt.Errorf("machine %s on hash-ring not found in workers", target.String())
		//	return
		//}
		s.MergeGeneralAVC(context.TODO(), partition, thriftAVC)
		//logger.Debugf("feature %v finish getting best split", featureId)
	}
	sortedSmallAVCTaskKeys := sortAVCTaskKeys(smallAVCTasks)
	for _, featureId := range sortedSmallAVCTaskKeys {
		globalV := smallAVCTasks[featureId]
		data.GetSpecificValueList(insIdx, featureCoding[featureId], featureValues)
		avc := tree.GenSmallAVC(tree.FeatureId(featureId), data.IsNumeric(featureCoding[featureId]), globalV, featureValues, labels, insWeights)
		// 根据featureId发送到对应机器上
		// todo:到时候对接hash环
		thriftAVC := rpc_type_trans.AVCtoThrift(avc)
		//target := hashRing.GetNodeByHashCode(cluster.CalcHash(strconv.Itoa(int(featureId))))
		//targetAddr := fmt.Sprintf("%s:%s", target.GetIp(), target.GetPort())
		//hit := false
		//for _, client := range workerClients {
		//	if client.addr == targetAddr {
		//		_err = client.c.MergeGeneralAVC(context.TODO(), partition, thriftAVC)
		//		hit = true
		//		break
		//	}
		//}
		//if _err != nil {
		//	return
		//}
		//if !hit {
		//	_err = fmt.Errorf("machine %s on hash-ring not found in workers", target.String())
		//	return
		//}
		_err = s.MergeGeneralAVC(context.TODO(), partition, thriftAVC)
		//logger.Debugf("feature %v finish getting best split", featureId)
	}
	return
}

func (s *ServerOnWorker) GenPartialAVC(ctx context.Context, partition *rpc.PartitionRef, featureId int32, tasks []*rpc.Interval) (_r []*rpc.AVC, _err error) {
	if !s.w.CheckPartition(partition) {
		_err = errors.New("partition not consistent, when gen partial-avc")
		return
	}
	insIdx := []int(nil)
	insWeights := []float64(nil)
	insIdx, insWeights, _err = s.w.GetInsByPartition(partition)
	if _err != nil {
		return
	}
	data := s.d.data
	theFeature := data.GetXIndexList()[featureId]
	featureValues := make([]float64, len(insIdx))
	data.GetSpecificValueList(insIdx, theFeature, featureValues)
	labels := make([]float64, len(insIdx))
	data.GetSpecificValueList(insIdx, data.GetYIndex(), labels)

	_r = make([]*rpc.AVC, len(tasks))
	// todo:这里也可以并发，但应该没必要，加了并发不一定好
	for i, task := range tasks {
		_r[i] = rpc_type_trans.AVCtoThrift(tree.GenPartialAVC(tree.FeatureId(featureId), [2]float64{task.Left, task.Right}, featureValues, labels, insWeights))
	}

	return
}

func (s *ServerOnWorker) MergeGeneralAVC(ctx context.Context, partition *rpc.PartitionRef, avc *rpc.AVC) (_err error) {
	if !s.w.CheckPartition(partition) {
		_err = errors.New("partition not consistent, when merge AVC")
		return
	}
	// 这里本来很简单的，但现在要同步
	unit := (*AttrKeepUnit)(nil)
	s.w.mu.RLock()
	keepUnit, has := s.w.generalAttrs[avc.Attr]
	s.w.mu.RUnlock()
	if has {
		unit = keepUnit
	} else {
		s.w.mu.Lock()
		keepUnit, has = s.w.generalAttrs[avc.Attr]
		if has {
			unit = keepUnit
		} else {
			unit = new(AttrKeepUnit)
			s.w.generalAttrs[avc.Attr] = unit
		}
		s.w.mu.Unlock()
	}
	receivedNum := unit.Update(rpc_type_trans.AVCfromThrift(avc))
	// todo:现在是根据机器数来确定是否完成的，但这样可能不太好，到时候看看能不能根据实例数来控制
	if receivedNum == WorkerNum {
		// todo:这里执行任务的时候没有区分是不是在自己的机器上，所以自己也是通过一样的流程发送的
		// 开始划分
		best := unit.avc.BestSplit(s.w.criterion)
		if len(best.CandiIntervals) != 0 {
			// 对于划分了区间的那些，可能要再看某些区间内部，对于每个区间都要再统计一次，这个倒是可以同步等，不然就很乱
			intervals := make([]*rpc.Interval, len(best.CandiIntervals))
			for i, interval := range best.CandiIntervals {
				intervals[i] = &rpc.Interval{
					interval[0],
					interval[1],
				}
			}
			//workers := []remote.Obj(nil)
			//workers, _err = s.w._getWorkers()
			//if _err != nil {
			//	return
			//}
			//workerClients := getWorkerClients(workers)
			//defer func() {
			//	// 关闭连接
			//	for _, client := range workerClients {
			//		client.Close()
			//	}
			//}()
			avcOfIntervals := []*tree.AVC(nil)
			intervalAVCch := make(chan []*tree.AVC, WorkerNum)
			wg := &sync.WaitGroup{}
			//for _, client := range workerClients {
			wg.Add(1)
			//go func(client *rpc.ServeOnWorkerClient) {
			go func() {
				defer wg.Done()
				// 这里的开销可能会很大，因为区间内可能有很多值，如果区间划分不均匀的话
				avcs, err := s.GenPartialAVC(context.TODO(), partition, avc.Attr, intervals)
				if err != nil {
					logger.Errorf("error in gen-partial-avc %v", err)
					return
				}
				changedAVCs := make([]*tree.AVC, len(avcs))
				for i, avc := range avcs {
					changedAVCs[i] = rpc_type_trans.AVCfromThrift(avc)
				}
				intervalAVCch <- changedAVCs
			}()
			//}(client.c)
			//}
			go func() {
				wg.Wait()
				close(intervalAVCch)
			}()
			for intervalAVC := range intervalAVCch {
				avcOfIntervals = mergeIntervalAVC(avcOfIntervals, intervalAVC)
			}
			for _, intervalAVC := range avcOfIntervals {
				if intervalAVC != nil {
					tmp := intervalAVC.BestSplit(s.w.criterion)
					if tmp.Valid() && tmp.ImprovementProxy > best.ImprovementProxy {
						best = tmp
					}
				}
			}
		}
		// 将该属性划分信息告知master
		// 这里要重新计算一次improvement，因为在属性内部NaN是固定的，但不同属性间NaN情况是不一样的，要重新算一下涉及NaN的
		// (nodeImpurity-(left/node)*leftImpurity-(right/node)*rightImpurity)*(node/nodeAll(nan+non_nan))*(nodeAll/weightAll)，之后外面再除以一个总的(所有实例的)
		best.ImprovementProxy = s.w.attrNonNaNimpurity[avc.Attr]*s.w.attrNonNaNcount[avc.Attr] - best.LeftImpurity*best.LeftWeight - best.RightImpurity*best.RightWeight
		//master := remote.Obj{}
		//master, _err = s.w._getMaster()
		//if _err != nil {
		//	return
		//}
		//masterClient := getMasterClient(master)
		//defer masterClient.Close()
		//_err = masterClient.c.UpdateSplitInfo(context.TODO(), rpc_type_trans.SplitRecordToThrift(avc.Attr, &best))
		key := fmt.Sprint(unsafe.Pointer(s))
		value, exist := Worker2Master.Get(key)
		if !exist {
			logger.Errorf("Can not find master of worker %s", key)
			return &dataManager.MyError{Msg: fmt.Sprintf("Can not find master of worker %s", key)}
		}
		master, _ := value.(*ServerOnMaster)
		_err = master.UpdateSplitInfo(context.TODO(), rpc_type_trans.SplitRecordToThrift(avc.Attr, &best))
	}

	return
}

func (s *ServerOnWorker) Split(ctx context.Context, partition *rpc.PartitionRef, newPartitionId int64, splitAttr int32, splitValue float64, hasNaN bool) (_err error) {
	// 对自己worker上的实例进行划分，同时把之前的partition清空一下
	if !s.w.CheckPartition(partition) {
		_err = errors.New("partition not consistent, when split")
		return
	}

	insIdx := []int(nil)
	insWeights := []float64(nil)
	insIdx, insWeights, _err = s.w.GetInsByPartition(partition)
	if _err != nil {
		return
	}
	data := s.d.data
	splitFeature := data.GetXIndexList()[splitAttr]
	featureValues := make([]float64, len(insIdx))
	data.GetSpecificValueList(insIdx, splitFeature, featureValues)
	newPartition := Partition{}

	if hasNaN {
		// 该属性是存在NaN的，看在这个worker上有没有NaN
		hasNaN = false
		for _, v := range featureValues {
			if math.IsNaN(v) {
				hasNaN = true
				break
			}
		}
	}

	// 这里划分时注意
	if hasNaN {
		// 有NaN的话，要拷贝权重，拷贝实例
		// 这里节省空间，先遍历一遍计数，再来分配空间
		leftNum := 0
		leftWeightSum := add.NewFloatAdder()
		rightWeightSum := add.NewFloatAdder()
		nanNum := 0
		weight := 1.0
		// 忘了非数值类型了，可以考虑把attrT传过来
		if !data.IsNumeric(splitFeature) {
			for i, v := range featureValues {
				if math.IsNaN(v) {
					nanNum += 1
					continue
				}
				if len(insWeights) > 0 {
					weight = insWeights[i]
				}
				// 不等于的分到左边，因为不等于相当于是0，等于相当于是1
				if v != splitValue {
					leftNum += 1
					leftWeightSum.Add(weight)
				} else {
					rightWeightSum.Add(weight)
				}
			}
		} else {
			for i, v := range featureValues {
				if math.IsNaN(v) {
					nanNum += 1
					continue
				}
				if len(insWeights) > 0 {
					weight = insWeights[i]
				}
				if v < splitValue {
					leftNum += 1
					leftWeightSum.Add(weight)
				} else {
					rightWeightSum.Add(weight)
				}
			}
		}
		rightNum := len(featureValues) - leftNum
		weightSum := leftWeightSum.Result() + rightWeightSum.Result()
		leftRate := 0.0
		rightRate := 0.0
		if weightSum == 0 {
			// 全是NaN，那就一半一半吧，全部分下去，然后权重都减半
			leftRate = 0.5
			rightRate = 0.5
		} else {
			leftRate = leftWeightSum.Result() / weightSum
			rightRate = rightWeightSum.Result() / weightSum
		}
		// NaN两边都要分到，给不同的权重
		leftIdx := make([]int, 0, leftNum+nanNum)
		leftWeights := make([]float64, 0, leftNum+nanNum)
		rightIdx := make([]int, 0, rightNum+nanNum)
		rightWeights := make([]float64, 0, rightNum+nanNum)
		if !data.IsNumeric(splitFeature) {
			for i, v := range featureValues {
				if len(insWeights) > 0 {
					weight = insWeights[i]
				}
				if math.IsNaN(v) {
					leftIdx = append(leftIdx, insIdx[i])
					leftWeights = append(leftWeights, weight*leftRate)
					rightIdx = append(rightIdx, insIdx[i])
					rightWeights = append(rightWeights, weight*rightRate)
				} else if v != splitValue {
					// 不等于的分到左边，因为不等于相当于是0，等于相当于是1
					leftIdx = append(leftIdx, insIdx[i])
					leftWeights = append(leftWeights, weight)
				} else {
					rightIdx = append(rightIdx, insIdx[i])
					rightWeights = append(rightWeights, weight)
				}
			}
		} else {
			for i, v := range featureValues {
				if len(insWeights) > 0 {
					weight = insWeights[i]
				}
				if math.IsNaN(v) {
					leftIdx = append(leftIdx, insIdx[i])
					leftWeights = append(leftWeights, weight*leftRate)
					rightIdx = append(rightIdx, insIdx[i])
					rightWeights = append(rightWeights, weight*rightRate)
				} else if v < splitValue {
					leftIdx = append(leftIdx, insIdx[i])
					leftWeights = append(leftWeights, weight)
				} else {
					rightIdx = append(rightIdx, insIdx[i])
					rightWeights = append(rightWeights, weight)
				}
			}
		}
		newPartition.left = leftIdx
		newPartition.leftWeights = leftWeights
		newPartition.right = rightIdx
		newPartition.rightWeights = rightWeights
	} else {
		// 没有NaN的话，可以复用之前的空间
		left := 0
		right := len(insIdx)
		// 实例和权重一起分
		if !data.IsNumeric(splitFeature) {
			for left < right {
				if featureValues[left] != splitValue {
					// 不等于的分到左边，因为不等于相当于是0，等于相当于是1
					left += 1
				} else {
					right -= 1
					insIdx[left], insIdx[right] = insIdx[right], insIdx[left]
					// 其实只要把right那的value换过来就好，left的值无所谓了
					featureValues[left], featureValues[right] = featureValues[right], featureValues[left]
					if len(insWeights) > 0 {
						insWeights[left], insWeights[right] = insWeights[right], insWeights[left]
					}
				}
			}
		} else {
			for left < right {
				if featureValues[left] < splitValue {
					left += 1
				} else {
					right -= 1
					insIdx[left], insIdx[right] = insIdx[right], insIdx[left]
					// 其实只要把right那的value换过来就好，left的值无所谓了
					featureValues[left], featureValues[right] = featureValues[right], featureValues[left]
					if len(insWeights) > 0 {
						insWeights[left], insWeights[right] = insWeights[right], insWeights[left]
					}
				}
			}
		}
		newPartition.left = insIdx[:left]
		newPartition.right = insIdx[left:]
		if len(insWeights) > 0 {
			newPartition.leftWeights = insWeights[:left]
			newPartition.rightWeights = insWeights[left:]
		}
		// fixme:可能存在一种情况，后面切片越分越小，但一直占着底层的大数组，看之后要不要优化吧
	}
	oldPartition := s.w.partitionMap[partition.SplitId]
	// 清一下空间
	if partition.IsLeft {
		oldPartition.left = nil
		oldPartition.leftWeights = nil
	} else {
		oldPartition.right = nil
		oldPartition.rightWeights = nil
	}
	if oldPartition.left == nil && oldPartition.right == nil {
		delete(s.w.partitionMap, partition.SplitId)
	} else {
		s.w.partitionMap[partition.SplitId] = oldPartition
	}
	s.w.partitionMap[newPartitionId] = newPartition
	return
}

func (s *ServerOnWorker) Clear(ctx context.Context) (_err error) {
	panic("")
}

func (s *ServerOnWorker) Stop(ctx context.Context) (_err error) {
	// 清理etcd等信息就放到外面去做吧
	if (*s).server != nil {
		defer func() {
			(*s).server = nil
		}()
		return (*s).server.Stop()
	}
	return
}

type _clientToWorker struct {
	// todo:这个client当初好像还想加点什么来着，可以考虑把name、addr什么的放这里
	addr string // addr 这个addr是所连接的远端的worker的addr
	c    *rpc.ServeOnWorkerClient
	sock *thrift.TSocket
}

func (c *_clientToWorker) Close() error {
	if c.sock != nil {
		return c.sock.Close()
	}
	return nil
}

func getWorkerClients(workers []remote.Obj) []*_clientToWorker {
	workerNum := len(workers)
	workerClients := make([]*_clientToWorker, 0, workerNum)
	transFactory, protoFactory := thrift.NewTTransportFactory(), thrift.NewTBinaryProtocolFactoryConf(nil)
	// 先起一下连接各worker的client
	for _, worker := range workers {
		sock := thrift.NewTSocketConf(worker.Addr, &thrift.TConfiguration{})
		transport, err := transFactory.GetTransport(sock)
		if err != nil {
			logger.Errorf("get client transport failed %v", err)
			continue
		}
		if err = transport.Open(); err != nil {
			logger.Errorf("client transport open to %v failed %v", worker.Addr, err)
			continue
		}
		client := rpc.NewServeOnWorkerClientFactory(transport, protoFactory)
		workerClients = append(workerClients, &_clientToWorker{
			addr: worker.Addr,
			c:    client,
			sock: sock,
		}) // 存一下
	}

	return workerClients
}
