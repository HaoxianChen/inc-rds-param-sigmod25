package conf_cluster

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	conf "gitlab.grandhoo.com/rock/rock_v3/decision_tree/conf/cluster"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/param/conf_manager"
	"os"
	"strings"
	"time"
)

type clusterParam struct {
	// log setting
	LogOn              bool
	LogInfoLevel       int
	LogDir             string
	LogStdOutThreshold string

	//machine info
	EtcdResponseTimeout      time.Duration
	ClusterAddrInfo          map[string]int
	ClusterAddrSlice         []string
	LocalIp                  string
	EtcdAddr                 string
	MachineNumber            int
	MachineVirtualNodeNumber int

	// distribute info
	RpcFramed     bool
	RpcBuffered   bool
	RpcSecure     bool
	RpcBufferSize int
	RpcProtocol   string

	// service port info
	//DGfdServicePort          string
	//ProxyServicePort         string
	//StatusServicePort        string
	HspawnWorkerServicePort string
	HspawnMasterServicePort string
	//HspawnSupportServicePort string
}

var Cluster = clusterParam{
	LogOn: true,

	EtcdResponseTimeout:      600 * time.Second,
	ClusterAddrInfo:          map[string]int{},
	MachineVirtualNodeNumber: 10,
	MachineNumber:            1,

	RpcProtocol:   "binary",
	RpcFramed:     false,
	RpcBuffered:   true,
	RpcSecure:     false,
	RpcBufferSize: 8192,

	//grh
	LocalIp:                 conf.LocalIp,
	EtcdAddr:                conf.EtcdAddr,
	HspawnWorkerServicePort: conf.HspawnWorkerServicePort,
	HspawnMasterServicePort: conf.HspawnMasterServicePort,
}

func ClusterConfigInit(args []string) {
	err := conf_manager.ParseFlagsWithArgs(args)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(-1)
	}
}

func ClusterSettingsToString() string {
	builder := strings.Builder{}
	builder.WriteString("distributed system settings!!!\n===============================\n")

	builder.WriteString("log settings+++\n")
	builder.WriteString(fmt.Sprintf("\tlog is on: %v\n", Cluster.LogOn))
	builder.WriteString(fmt.Sprintf("\tlog dir: %v\n", Cluster.LogDir))
	builder.WriteString(fmt.Sprintf("\tlog info level: %v\n", Cluster.LogInfoLevel))
	builder.WriteString(fmt.Sprintf("\tlog std out threshold: %v\n", Cluster.LogStdOutThreshold))

	builder.WriteString("cluster settings+++\n")
	builder.WriteString(fmt.Sprintf("\tetcd response timeout: %v\n", Cluster.EtcdResponseTimeout))
	builder.WriteString(fmt.Sprintf("\tvirtual number: %v\n", Cluster.MachineVirtualNodeNumber))
	builder.WriteString(fmt.Sprintf("\tcluster addr info: %v\n", Cluster.ClusterAddrInfo))
	builder.WriteString(fmt.Sprintf("\tlocal ip: %v\n", Cluster.LocalIp))
	builder.WriteString(fmt.Sprintf("\tetcd addr: %v\n", Cluster.EtcdAddr))
	builder.WriteString(fmt.Sprintf("\tmachine number: %v\n", Cluster.MachineNumber))

	builder.WriteString("distribute settings+++\n")
	builder.WriteString(fmt.Sprintf("\tdistribute protocol: %v\n", Cluster.RpcProtocol))
	builder.WriteString(fmt.Sprintf("\tdistribute framed: %v\n", Cluster.RpcFramed))
	builder.WriteString(fmt.Sprintf("\tdistribute buffered: %v\n", Cluster.RpcBuffered))
	builder.WriteString(fmt.Sprintf("\tdistribute secure: %v\n", Cluster.RpcSecure))
	builder.WriteString(fmt.Sprintf("\tdistribute buffer size: %v\n", Cluster.RpcBufferSize))

	builder.WriteString("service settings+++\n")
	//builder.WriteString(fmt.Sprintf("\tdgfd port: %v\n", Cluster.DGfdServicePort))
	//builder.WriteString(fmt.Sprintf("\tproxy port: %v\n", Cluster.ProxyServicePort))
	//builder.WriteString(fmt.Sprintf("\tstatus port: %v\n", Cluster.StatusServicePort))
	builder.WriteString(fmt.Sprintf("\thspawn worker port: %v\n", Cluster.HspawnWorkerServicePort))
	builder.WriteString(fmt.Sprintf("\thspawn master port: %v\n", Cluster.HspawnMasterServicePort))
	//builder.WriteString(fmt.Sprintf("\thspawn support port: %v\n", Cluster.HspawnSupportServicePort))
	return builder.String()
}
