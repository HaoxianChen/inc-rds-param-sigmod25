package conf_cluster

import (
	"fmt"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/cmd"
	"gitlab.grandhoo.com/rock/rock_v3/decision_tree/param/conf_manager"
)

func Init() {
	//log setting
	logSettings := cmd.NewSecondaryValue(make(map[string]cmd.Value), nil)
	err := conf_manager.AddCmdArgs(&cmd.Flag{
		Name:      "log",
		Aliases:   []string{"Log"},
		Usage:     "--log --Log, specify log info, eg:switch=on dir=./ info-level=0 std-threshold=INFO",
		Required:  false,
		FlagValue: logSettings,
	})
	if err != nil {
		panic(err)
	}
	err = logSettings.AddSubValue("switch", &cmd.BoolCmdValue{Destination: &Cluster.LogOn})
	if err != nil {
		panic(err)
	}
	err = logSettings.AddSubValue("dir", cmd.NewStringValue(&Cluster.LogDir, nil))
	if err != nil {
		panic(err)
	}
	err = logSettings.AddSubValue("info-level", cmd.NewIntValue(&Cluster.LogInfoLevel, nil))
	if err != nil {
		panic(err)
	}
	err = logSettings.AddSubValue("std-threshold", cmd.NewStringValue(&Cluster.LogStdOutThreshold, nil))
	if err != nil {
		panic(err)
	}

	//net setting
	netSettings := cmd.NewSecondaryValue(make(map[string]cmd.Value), nil)
	err = conf_manager.AddCmdArgs(&cmd.Flag{
		Name:      "net",
		Aliases:   []string{"Net"},
		Usage:     "--net, --Net specify cluster net config",
		Required:  false,
		FlagValue: netSettings,
	})
	if err != nil {
		panic(err)
	}
	err = netSettings.AddSubValue("local-ip", cmd.NewStringValue(&Cluster.LocalIp, nil))
	if err != nil {
		panic(err)
	}
	err = netSettings.AddSubValue("etcd-addr", cmd.NewStringValue(&Cluster.EtcdAddr, nil))
	if err != nil {
		panic(err)
	}
	//err = netSettings.AddSubValue("proxy-port", cmd.NewStringValue(&Cluster.ProxyServicePort, nil))
	//if err != nil {
	//	panic(err)
	//}
	//err = netSettings.AddSubValue("dgfd-svc-port", cmd.NewStringValue(&Cluster.DGfdServicePort, nil))
	//if err != nil {
	//	panic(err)
	//}
	//err = netSettings.AddSubValue("status-port", cmd.NewStringValue(&Cluster.StatusServicePort, nil))
	//if err != nil {
	//	panic(err)
	//}
	err = netSettings.AddSubValue("hspawn-worker-port", cmd.NewStringValue(&Cluster.HspawnWorkerServicePort, nil))
	if err != nil {
		panic(err)
	}
	err = netSettings.AddSubValue("hspawn-master-port", cmd.NewStringValue(&Cluster.HspawnMasterServicePort, nil))
	if err != nil {
		panic(err)
	}
	//err = netSettings.AddSubValue("hspawn-support-port", cmd.NewStringValue(&Cluster.HspawnSupportServicePort, nil))
	//if err != nil {
	//	panic(err)
	//}
	err = netSettings.AddSubValue("machine-number", cmd.NewIntValue(&Cluster.MachineNumber, func(valueToCheck int) error {
		if valueToCheck <= 0 {
			return fmt.Errorf("expect machine number > 0")
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}
}
