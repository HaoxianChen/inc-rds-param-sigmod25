package common

import (
	"crypto/tls"
	"github.com/apache/thrift/lib/go/thrift"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
)

// service name
const (
	// DGfdServiceName DGfd服务
	DGfdServiceName = "DGfd"
	// ProxyServiceName 分布式代理服务
	ProxyServiceName = "ProxyService"
	// FileTransSvcName 图文件传输服务
	FileTransSvcName = "FileTransService"
	// StatusServiceName 状态传输服务
	StatusServiceName = "StatusService"
)

type TaskCode string

//各任务代码，用于作为获取虚拟节点的key，可以是常量字符串，也可以是通过计算得到的字符串

const (
	PatternCounter = "PatternCounter"
	RuleMining     = "RuleMining"
	InstanceMerge  = "instanceMerge"
)

// ProtocolParam thrift协议栈参数
type ProtocolParam struct {
	protocol string
	useFrame bool
	//为0则非buffer
	buffered bool
	buffSize int
	secure   bool
}

var rpcParam *ProtocolParam
var transportFactory thrift.TTransportFactory
var protocolFactory thrift.TProtocolFactory
var compactProtoFactory thrift.TProtocolFactory //内部模式实例通信选用
var config *thrift.TConfiguration

func GetRpcProtocolParam() *ProtocolParam {
	return rpcParam
}

func (p ProtocolParam) Protocol() string {
	return p.protocol
}

func (p ProtocolParam) UseFrame() bool {
	return p.useFrame
}

func (p ProtocolParam) Buffered() bool {
	return p.buffered
}

func (p ProtocolParam) BuffSize() int {
	return p.buffSize
}

func (p ProtocolParam) Secure() bool {
	return p.secure
}

// InitRpcParam 程序启动后启动server和构建client之前需要先初始化参数
func InitRpcParam(proto string, framed bool, buffered bool, buffSize int, secure bool) {
	rpcParam = &ProtocolParam{protocol: proto, /*"compact"*/
		useFrame: framed,
		buffered: buffered,
		buffSize: buffSize,
		secure:   secure,
	}
	config = &thrift.TConfiguration{
		TLSConfig: &tls.Config{
			//InsecureSkipVerify: true, //for test
		},
	}

	if rpcParam.buffered {
		transportFactory = thrift.NewTBufferedTransportFactory(rpcParam.buffSize)
	} else {
		transportFactory = thrift.NewTTransportFactory()
	}

	if rpcParam.useFrame {
		transportFactory = thrift.NewTFramedTransportFactoryConf(transportFactory, config)
	}

	protocolFactory = createProtoFactory(rpcParam.protocol)
	if proto != "compact" {
		compactProtoFactory = createProtoFactory("compact")
	} else {
		compactProtoFactory = protocolFactory
	}
}

func createProtoFactory(proto string) thrift.TProtocolFactory {
	var factory thrift.TProtocolFactory
	switch proto {
	case "compact":
		factory = thrift.NewTCompactProtocolFactoryConf(nil)
	case "simplejson":
		factory = thrift.NewTSimpleJSONProtocolFactoryConf(nil)
	case "json":
		factory = thrift.NewTJSONProtocolFactory()
	case "binary", "":
		factory = thrift.NewTBinaryProtocolFactoryConf(nil)
	default:
		logger.Errorf("Invalid protocol specified %s", proto)
	}
	return factory
}

func Prepare() (thrift.TTransportFactory, thrift.TProtocolFactory, *thrift.TConfiguration) {
	return transportFactory, protocolFactory, config
}

func PrepareCompact() (thrift.TTransportFactory, thrift.TProtocolFactory, *thrift.TConfiguration) {
	return transportFactory, compactProtoFactory, config
}
