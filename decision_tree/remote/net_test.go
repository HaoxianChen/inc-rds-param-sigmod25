package remote

import (
	"context"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/call/gen-go/rpc"
	"net"
	"os"
	"path"
	"testing"
	"time"
)

type sss struct {
}

func (s sss) UpdateSplitInfo(ctx context.Context, record *rpc.SplitRecord) (_err error) {
	return
}

func (s sss) Stop(ctx context.Context) (_err error) {
	return
}

func TestPortCheck(t *testing.T) {
	lsn, err := net.Listen("tcp", "127.0.0.1:0")

	if err == nil {
		fmt.Println(lsn.Addr())
		defer lsn.Close()
	} else {
		t.Log(err)
	}

	conn, err := net.Dial("tcp", lsn.Addr().String())
	if err == nil {
		fmt.Println(conn.LocalAddr().String(), conn.RemoteAddr().String())
		defer conn.Close()
	} else {
		t.Log(err)
	}

	transFactory, protoFactory := thrift.NewTTransportFactory(), thrift.NewTBinaryProtocolFactoryConf(nil)
	var transport thrift.TServerTransport

	transport, err = thrift.NewTServerSocket(lsn.Addr().String())

	if err != nil {
		fmt.Println("create server socket failed ", err.Error())
		return
	}

	processor := rpc.NewServeOnMasterProcessor(sss{})
	server := thrift.NewTSimpleServer4(processor, transport, transFactory, protoFactory)

	go func() {
		err := server.Serve()
		fmt.Println(err)
	}()

	select {}
}

func TestOther(t *testing.T) {
	t.Log(path.Dir("./me.txt"))
	_, err := os.Stat(".")
	t.Log(err)
	_, err = os.Stat("../remote")
	t.Log(err)
	t.Log(path.Join("./aetaset", "222/", "1.txt"))
	t.Log(time.Now().Format("2006/01/02_15-04-05"))
	//os.MkdirAll(".", 0777)
	//f, err := os.OpenFile("./level1/level2/level3/me.txt", os.O_RDWR|os.O_CREATE, 0666)
	//if err == nil {
	//	f.Close()
	//} else {
	//	t.Log(err)
	//}
}
