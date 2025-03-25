package call

import (
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/gen-go/rpc"
	"testing"
	"unsafe"
)

func TestMem(t *testing.T) {
	t.Log(unsafe.Sizeof(MasterMem{}))
}

func runServer() {
	addr := "127.0.0.1:8888"
	transFactory, protoFactory := thrift.NewTTransportFactory(), thrift.NewTBinaryProtocolFactoryConf(nil)
	var transport thrift.TServerTransport
	var err error
	transport, err = thrift.NewTServerSocket(addr)

	if err != nil {
		fmt.Println("create server socket failed ", err.Error())
		return
	}
	//fmt.Printf("%T\n", transport)

	processor := rpc.NewServeOnMasterProcessor(NewServer())
	server := thrift.NewTSimpleServer4(processor, transport, transFactory, protoFactory)

	go server.Serve()
}

func runClient(serverAddr string) {
	transFactory, protoFactory := thrift.NewTTransportFactory(), thrift.NewTBinaryProtocolFactoryConf(nil)
	var sock thrift.TTransport
	sock = thrift.NewTSocketConf(serverAddr, &thrift.TConfiguration{})
	transport, err := transFactory.GetTransport(sock)
	if err != nil {
		fmt.Println("get client transport failed ", err)
	}
	if err = transport.Open(); err != nil {
		fmt.Println(fmt.Sprintf("client transport open to %v failed %v", serverAddr, err))
	}
	for err != nil {
		err = transport.Open()
	}

	client := rpc.NewServeOnMasterClientFactory(transport, protoFactory)

	ctx, _ := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, "addr", sock.(*thrift.TSocket).Conn().LocalAddr().String())
	client.UpdateSplitInfo(ctx, &rpc.SplitRecord{})
}

func TestServeOnMaster_UpdateSplitInfo(t *testing.T) {
	runServer()
	runClient("localhost:8888")
	runClient("localhost:8888")

	select {}
}

//func TestStartService(t *testing.T) {
//	log.Level(zerolog.DebugLevel)
//	masterPort := 9009
//	// todo:到时候应该有个命令行，会通知有哪些worker，或者直接自己从etcd中拿，自己拿的话要排个序，保证不同机器上这个顺序是一致的
//	worker := func(id int, port int, masterPort int) {
//		m := RegisterFor(fmt.Sprintf("machine%d", id), "localhost", []string{"localhost:2379"})
//		_, err := m.startWorkerService(port, getDataFrame(id), nil, tree.Entropy{})
//		if err != nil {
//			t.Log(err)
//			return
//		}
//	}
//
//	for i := 0; i < WorkerNum; i++ {
//		go worker(i, 7788+i, masterPort)
//	}
//	// 正常来说底下各流程是和worker注册放在一起的，但这里在本机测试，如果放一起的话，master会被起多次
//	m := RegisterFor(fmt.Sprintf("machine%d", 0), "localhost", []string{"localhost:2379"})
//	log.Info().Msg("wait!!!!")
//	err := m.waitWorkerToBeReady()
//	if err != nil {
//		return
//	}
//	log.Info().Msg("after wait!!")
//	masterAddr := m.SelectMaster(masterPort)
//	log.Info().Msgf("select master:%v", masterAddr)
//	manager, master, err := m.startMasterWithPort(masterAddr)
//	log.Info().Msg("start master!!")
//	if err != nil {
//		return
//	}
//	if manager == nil || master == nil {
//		return
//	}
//	// 这里就直接返回了
//	classifier := NewClassifier()
//	log.Info().Msg("start to fit")
//	dt := classifier.Fit(manager)
//	dt.ToSimpleGraph("./tree.dot")
//	// 清理一下
//	log.Info().Msg("start to delete in etcd")
//	client := remote.NewEtcdClient([]string{"localhost:2379"})
//	client.DeleteWithPrefix(WORKER_PREFIX_ETCD)
//	client.DeleteWithPrefix(MASTER_PREFIX_ETCD)
//}

//func getDataFrame(id int) *standarlization.NodeDataFrame {
//	training_file, err := os.Open(fmt.Sprintf("./data/data_%d.csv", id))
//	if err != nil {
//		panic(err)
//	}
//	defer training_file.Close()
//	reader := csv.NewReader(training_file)
//	// 先读header
//	header, _ := reader.Read()
//	featureList := header[:len(header)-1] // 最后一个是y
//	yName := header[len(header)-1]
//	data := [][]float64(nil)
//	// 读所有数据
//	for oneLine, err := reader.Read(); err == nil; oneLine, err = reader.Read() {
//		oneLineData := make([]float64, len(oneLine))
//		for i, d := range oneLine {
//			fd, _ := strconv.ParseFloat(d, 64)
//			oneLineData[i] = fd
//		}
//		data = append(data, oneLineData)
//	}
//	// 转成dataframe
//	return standarlization.DataFrameFromRawData(data, featureList, yName)
//}
