// 注册worker服务

package call

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/orcaman/concurrent-map"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/common"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/ml/tree"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/param/conf_cluster"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/remote"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// Registry 一台机器上的注册服务，用ip来区分不同的"机器"，name是机器的name，可以为空
type Registry struct {
	name string // 这个name要求各个worker是不同的
	ip   string // 标志一个worker，也就是一台机器
	etcd []string
	//hasMaster bool // 是否有master在这里注册
}

var _registry = (*Registry)(nil) // 用来注册服务

func RegisterFor(name string, ip string, etcdEndPoints []string) *Registry {
	return &Registry{
		name: name,
		ip:   ip,
		etcd: etcdEndPoints,
	}
}

//var Worker *ServerOnWorker
//var Master *ServerOnMaster

var Worker2Master = cmap.New()
var Worker2Manager = cmap.New()

const WorkerNum = 1

// startWorkerService 启动worker服务，需要指定地址
func (r *Registry) startWorkerService(addr string, criterion tree.Criterion) (worker *ServerOnWorker, server thrift.TServer, err error) {
	nameAsWorker := r.name

	//log.Debug().Msgf("<start worker>--start worker on: %s", addr)
	//defer func() {
	//	log.Debug().Msgf("<start worker>--start worker end!! %v", err)
	//}()
	worker = NewWorker(nameAsWorker, addr, criterion, r.etcd)

	//processor := rpc.NewServeOnWorkerProcessor(worker)
	//server, err, errCh := startThriftService(addr, processor)
	//if err != nil {
	//	return
	//}
	worker.SetServer(server)

	// 创建manager
	_gWorkerManager := worker.GetManager()
	key := fmt.Sprint(unsafe.Pointer(worker))
	Worker2Manager.Set(key, _gWorkerManager)
	return
}

// startMasterWithPort 启动完worker之后，从中选取一个作为master，要另外指定一个端口，addr中的ip必须要在某个worker中，不然就起不了master。
// 外面可以再做一步判断，确保master启动了。
func (r *Registry) startMasterWithPort(port int) (master *ServerOnMaster, server thrift.TServer, manager *ManagerOnMaster, err error) {
	//client := remote.NewEtcdClient(r.etcd)
	//defer client.Close()
	//
	//masterInfo, _ := client.GetWithPrefix(MASTER_PREFIX_ETCD)
	//if len(masterInfo) != 0 {
	//	err = errors.New("master has already registered in etcd! ")
	//	return
	//}

	//addr := fmt.Sprintf("%s:%d", r.ip, port)
	//log.Debug().Msgf("<start master>--start master on: %s", addr)
	//defer func() {
	//	log.Debug().Msgf("<start master>--start master end!! %v", err)
	//}()
	//// 在对应机器上启动master服务
	//// fixme:到时候这里可能要传一些参数
	master, manager = NewServerManager(r.etcd)
	//processor := rpc.NewServeOnMasterProcessor(serve)
	//server, err, errCh := startThriftService(addr, processor)
	//if err != nil {
	//	return
	//}
	//// fixme:这里可能会有一点点的空窗，但应该影响不大
	master.SetServer(server)
	//log.Debug().Msg("<start master>--wait until service start")
	//err = waitUntilServiceStart(addr, 30*time.Second, errCh)
	//if err != nil {
	//	return
	//}
	//
	//log.Debug().Msg("<start master>--register to etcd")
	//err = client.Put(MASTER_PREFIX_ETCD, addr)
	//if err != nil {
	//	//如果不能注册到etcd中的话，不认为这个服务正常启动了，停止
	//err = fmt.Errorf("start master service failed on %s:%s", addr, err.Error())
	//server.Stop()
	//server = nil
	//manager = nil
	//return
	//}
	//r.hasMaster = true
	return
}

// waitWorkerToBeReady 等待所有worker启动
func (r *Registry) waitWorkerToBeReady() error {
	// 等待所有worker注册完成
	client := remote.NewEtcdClient(r.etcd)
	defer client.Close()

	kvs, err := client.GetWithPrefix(WORKER_PREFIX_ETCD)
	workerNum := conf_cluster.Cluster.MachineNumber
	for len(kvs) != workerNum && err == nil {
		kvs, err = client.GetWithPrefix(WORKER_PREFIX_ETCD)
		time.Sleep(500 * time.Millisecond)
	}

	if err != nil {
		return err
	}

	return nil
}

// masterAttachedHere 选择一个机器作为master，要等所有worker都启动之后才能执行
func (r *Registry) masterAttachedHere(seq int) bool {
	// 这里要等待所有worker注册完成之后
	client := remote.NewEtcdClient(r.etcd)
	defer client.Close()
	kvs, _ := client.GetWithPrefix(WORKER_PREFIX_ETCD)
	if len(kvs) == 0 {
		logger.Error("workers not registered in etcd!! ")
		return false
	}
	sortedKs := make([]string, 0, len(kvs)) // 各个key肯定是不同的，这里排个序，然后根据seq来选
	for k := range kvs {
		sortedKs = append(sortedKs, k)
	}
	sort.Slice(sortedKs, func(i, j int) bool {
		return sortedKs[i] < sortedKs[j]
	})
	selfK := WORKER_PREFIX_ETCD + r.name
	return sortedKs[seq%len(sortedKs)] == selfK
}

func (r *Registry) unRegister() {
	//client := remote.NewEtcdClient(r.etcd)
	//defer client.Close()
	//
	//// 只有master会调这个
	//if err := client.Delete(MASTER_PREFIX_ETCD); err != nil {
	//	log.Error().Msgf("error in unregister master in etcd! --> %v", err)
	//}
	////r.hasMaster = false
	//if err := client.DeleteWithPrefix(WORKER_PREFIX_ETCD); err != nil {
	//	log.Error().Msgf("error in unregister workers in etcd! --> %v", err)
	//}
	//Worker.DataInit(context.TODO())
	//Master = nil
}

func (r *Registry) unRegisterWorkerAndMaster(worker *ServerOnWorker, master *ServerOnMaster) {
	//client := remote.NewEtcdClient(r.etcd)
	//defer client.Close()
	//
	//// 只有master会调这个
	//if err := client.Delete(MASTER_PREFIX_ETCD); err != nil {
	//	log.Error().Msgf("error in unregister master in etcd! --> %v", err)
	//}
	////r.hasMaster = false
	//if err := client.DeleteWithPrefix(WORKER_PREFIX_ETCD); err != nil {
	//	log.Error().Msgf("error in unregister workers in etcd! --> %v", err)
	//}
	key := fmt.Sprint(unsafe.Pointer(worker))
	Worker2Master.Remove(key)
	Worker2Manager.Remove(key)
	worker = nil
	master = nil
}

// Close 关闭client
//func (r *Registry) Close() error {
//	return r.client.Close()
//}

func StartMasterServer(port int, seq int) (master *ServerOnMaster, server thrift.TServer, manager *ManagerOnMaster, err error) {
	//err = _registry.waitWorkerToBeReady()
	//if err != nil {
	//	return
	//}
	//if _registry.masterAttachedHere(seq) {
	return _registry.startMasterWithPort(port)
	//}
	//return
}

func StartWorkerServer(name, addr string, etcdEndPoints []string, criterion string) (worker *ServerOnWorker, server thrift.TServer, err error) {
	//创建相应server，在etcd中注册名字，把manager放在内存全局变量中
	nameAsWorker := fmt.Sprintf("%s(%s)", name, addr)
	_registry = RegisterFor(nameAsWorker, strings.Split(addr, ":")[0], etcdEndPoints)
	return _registry.startWorkerService(addr, tree.GetCriterionByName(criterion))
}

// MasterDone master结点完成之后调用，worker会等待这个信号
func MasterDone() {
	_registry.unRegister()
}

func WorkerAndMasterDone(worker *ServerOnWorker, master *ServerOnMaster) {
	_registry.unRegisterWorkerAndMaster(worker, master)
}

// WaitMaster 各个没有master的结点会调用，等待master完成
func WaitMaster() {
	c1 := remote.NewEtcdClient(_gWorkerManager.w.etcdEndPoints)
	defer c1.Close()

	// 有这样一种情况，可能master已经发过完成信号了，但是这里还没起watch
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchCh := c1.Watch(ctx, WORKER_PREFIX_ETCD+_gWorkerManager.w.self.Name)
	// 现在是确定已经开始watch了，然后看一眼是不是已经没有了，这里不确定一个client能不能既watch又get，所以起个新的client
	c2 := remote.NewEtcdClient(_gWorkerManager.w.etcdEndPoints)
	defer c2.Close()

	_, has, _ := c2.Get(WORKER_PREFIX_ETCD + _gWorkerManager.w.self.Name)
	if has {
		// 如果还没有被清，就等待master来清
		<-watchCh
	}
	// 如果已经被清了，就直接返回就好
}

func startThriftService(addr string, processor thrift.TProcessor) (server thrift.TServer, instantErr error, errCh chan error) {
	rpcParam := common.GetRpcProtocolParam()
	if rpcParam == nil {
		instantErr = errors.New("distribute param not initialized")
		return
	}
	transFactory, protoFactory, _ := common.Prepare()
	transport := thrift.TServerTransport(nil)
	// secure
	if rpcParam.Secure() {
		cfg := new(tls.Config)
		if cert, err := tls.LoadX509KeyPair("server.crt", "server.key"); err == nil {
			cfg.Certificates = append(cfg.Certificates, cert)
		} else {
			instantErr = fmt.Errorf("LoadX509KeyPair failed: %s", err.Error())
			return
		}
		transport, instantErr = thrift.NewTSSLServerSocket(addr, cfg)
	} else {
		transport, instantErr = thrift.NewTServerSocket(addr)
	}
	if instantErr != nil {
		instantErr = fmt.Errorf("create server socket failed: %s", instantErr.Error())
		return
	}
	server = thrift.NewTSimpleServer4(processor, transport, transFactory, protoFactory)
	errCh = make(chan error, 1)
	go func() {
		errCh <- server.Serve()
		close(errCh)
	}()

	return
}

func waitUntilServiceStart(addr string, waitTime time.Duration, errCh chan error) (err error) {
	// 怎么确保服务启动之后再往etcd注册呢，当端口被占用，且Serve返回值没有error
	ctx, cancel := context.WithTimeout(context.Background(), waitTime)
	// todo:到时候看看在合适的位置cancel
	defer cancel()

	if !remote.WaitAddressAvailable(ctx, addr) {
		err = fmt.Errorf("start service on:%s failed! ", addr)
		return
	}
	time.Sleep(400 * time.Millisecond) // 稍微等待一会儿，让errCh有时间传递消息
	// 端口被占用，看是不是当前服务占用的
	select {
	case err = <-errCh:
		// 看看是不是起服务的时候出错了
	default:
	}

	return
}
