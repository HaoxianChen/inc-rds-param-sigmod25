package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"gitlab.grandhoo.com/rock/rock_memery"
	"gitlab.grandhoo.com/rock/rock_v3/common"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig"
	logger_convert "gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils/storage_logger_convertor"
	config2 "gitlab.grandhoo.com/rock/storage/config"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"gitlab.grandhoo.com/rock/rock_v3/intersection"
	distributed_storage_initiator "gitlab.grandhoo.com/rock/storage/config/distributed_storage_initiator"

	"gitlab.grandhoo.com/rock/rock_v3/global_variables"

	decision_tree "gitlab.grandhoo.com/rock/rock_v3/decision_tree/execute"

	"github.com/gin-gonic/gin"
	"gitlab.grandhoo.com/rock/rock-share/base/config"
	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/distribute"
	"gitlab.grandhoo.com/rock/rock-share/global/channel"
	"gitlab.grandhoo.com/rock/rock-share/global/db"
	"gitlab.grandhoo.com/rock/rock-share/rpc/grpc/pb"
	"gitlab.grandhoo.com/rock/rock_v3/rds_config"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/rpchandler"
	"gitlab.grandhoo.com/rock/rock_v3/utils/storage_utils"
)

const (
	closeType = "closeType"
)

// k8s部署方式才要在任务结束时退出计算节点
//func closeTaskHandler() gin.HandlerFunc {
//	return func(c *gin.Context) {
//		defer func() {
//			if err := recover(); err != nil {
//				debug.PrintStack()
//				logger.Infof("to close failed task")
//				e := distribute.GEtcd.CloseTask(manager.CloseType_CLOSE_TYPE_TASK_FAILED)
//				if e != nil {
//					logger.Errorf("close failed task error: %s", e)
//					return
//				}
//				logger.Infof("finish close failed task")
//				c.JSON(http.StatusInternalServerError, gin.H{"error": err})
//				return
//			}
//		}()
//		c.Next()
//		logger.Infof("to lose task")
//		closeType := manager.CloseType_CLOSE_TYPE_TASK_DONE
//		if c.Writer.Status() != http.StatusOK {
//			closeType = manager.CloseType_CLOSE_TYPE_TASK_FAILED
//		}
//		err := distribute.GEtcd.CloseTask(closeType)
//		if err != nil {
//			logger.Errorf("close task error: %s, close type=%v", err, closeType)
//			return
//		}
//		logger.Infof("finish close task")
//	}
//}

func jobRunningHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if !c.IsAborted() {
				global_variables.IsJobRunning = false
			}
		}()

		logger.Debugf("job is running: %v", global_variables.IsJobRunning)
		// 如果有任务正在执行，直接返回
		if global_variables.IsJobRunning {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"message": "A job is running"})
			logger.Infof("c.IsAborted: %v", c.IsAborted())
			return
		}

		// 设置任务正在执行的标志为true
		global_variables.IsJobRunning = true

		// 继续处理下一个中间件或者路由处理函数
		c.Next()

		// 如果createtask的时候没有指定req参数，用户可以手动发请求，然后这里自动closetask。
		if c.Keys == nil {
			c.Keys = map[string]any{}
		}
		if c.Keys[closeType] != nil {
			// _, innerErr := distribute.GEtcd.Manager.CloseTask(context.Background(), &manager.CloseTaskRequest{
			// 	TaskId:    distribute.GEtcd.TaskKey.TaskId,
			// 	CloseType: int32(c.Keys[closeType].(int)),
			// })
			// if innerErr != nil {
			// 	logger.Errorf("call manager to close task failed, err=%v", innerErr)
			// }
		}
	}
}

func main() {
	go func() {
		for i := 8801; i < 9000; i++ {
			err := http.ListenAndServe(":"+strconv.Itoa(i), nil)
			if err != nil {
				fmt.Printf("http.ListenAndServe failed, err:%s", err)
			} else {
				fmt.Printf("http.ListenAndServe run on %d", i)
				break
			}
		}
	}()

	config.InitConfig1()
	taskName := flag.String("tn", "", "task name")
	rockConfig := flag.String("rock_config", "", "rock config")
	flag.Parse()
	fmt.Printf("input taskName=%v\n", *taskName)
	fmt.Printf("input rockConfig=%v\n", *rockConfig)
	var rockConf config.AllConfig
	// 用传入的配置替换自带的配置
	if len(*rockConfig) > 0 {
		err := json.Unmarshal([]byte(*rockConfig), &rockConf)
		if err != nil {
			fmt.Printf("json.Unmarshal rock config failed")
			panic(err)
		}
		config.All = &rockConf
	}
	fmt.Printf("config file content:%+v\n", *config.All)
	all := config.All
	l := all.Logger
	ss := all.Server
	logger.InitLogger(l.Level, distribute.GetEnvContainerName()+"_"+"rock", l.Path, l.MaxAge, l.RotationTime, l.RotationSize, ss.SentryDsn)
	db.InitGorm()

	go func() {
		for {
			massage, ok := <-channel.RDSChan
			if !ok {
				logger.Infof("读取RDSChan失败")
				break
			}
			logger.Infof("从RDSChan中收到消息:%v", massage)
		}
	}()
	r := gin.Default()

	r.POST("/import", importTable)

	r.POST("/single-decision-tree", ExecuteSingleRowDecisionTree)

	r.POST("/Get-table-sample", GetTableSampleData)

	r.POST("/hash-join", jobRunningHandler(), HashJoin)

	r.POST("/inc-rds", jobRunningHandler(), IncDigRules)

	r.POST("/delete-inc-rds", jobRunningHandler(), DeleteIncRdsTask)

	r.POST(distributed_storage_initiator.RLockApi, distributed_storage_initiator.DFSRLockHandler)

	r.POST(distributed_storage_initiator.RUnLockApi, distributed_storage_initiator.DFSRUnLockHandler)

	r.POST(distributed_storage_initiator.RCntApi, distributed_storage_initiator.DFSGetReadCount)

	r.POST(distributed_storage_initiator.RCntSetApi, distributed_storage_initiator.DFSSetReadCount)

	r.POST(distributed_storage_initiator.ApiFileList, distributed_storage_initiator.DFSFileList)

	r.POST(distributed_storage_initiator.ApiFileRemove, distributed_storage_initiator.DFSFileRemove)

	r.POST(distributed_storage_initiator.ApiTableShow, distributed_storage_initiator.DFSTableShow)

	r.POST(distributed_storage_initiator.ApiTableDelete, distributed_storage_initiator.DFSTableDelete)

	// set memory
	r.POST("/set-memory", SetMemory)

	var port uint32
	var err error
	var listener net.Listener
	for _, port = range rds_config.GinPorts {
		listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			continue
		}
		listener.Close()

		address := ":" + strconv.Itoa(int(port))
		go r.Run(address)
		break
	}
	if err != nil {
		panic(err)
	}
	if err := distribute.GEtcd.Distribute(*taskName, distribute.KRoleUnknown, port, rpchandler.Registry); err != nil {
		logger.Error(err)
	}

	config2.SetLogger(logger_convert.RockLogger{})
	if err := distributed_storage_initiator.UseDFS(); err != nil {
		logger.Error(err)
	}

	// 初始化内存管理
	var storageMem = "8G"
	var shuffleMem = "6G"
	var intersectionMem = "10G"
	if all.Memory.StorageMem != "" {
		storageMem = all.Memory.StorageMem
	}
	if all.Memory.ShuffleMem != "" {
		shuffleMem = all.Memory.ShuffleMem
	}
	if all.Memory.IntersectionMem != "" {
		intersectionMem = all.Memory.IntersectionMem
	}
	logger.Infof("memory config: storageMem:%s|shuffleMem:%s|intersectionMem:%s", storageMem, shuffleMem, intersectionMem)
	rock_memery.InitMemoryManager(&rock_memery.MemoryConfig{StorageMem: storageMem, ShuffleMem: shuffleMem, IntersectionMem: intersectionMem})

	// 初始化memory各stage的agent
	storageTaskMemManager := rock_memery.NewTaskMemoryManager(rock_memery.MemoryManagerInstance)
	common.StorageSliceAgent = rock_memery.NewRockSliceAgent(storageTaskMemManager, rock_memery.StorageCalc)
	shuffleTaskMemManager := rock_memery.NewTaskMemoryManager(rock_memery.MemoryManagerInstance)
	common.ShuffleSliceAgent = rock_memery.NewRockSliceAgent(shuffleTaskMemManager, rock_memery.ShuffleCalc)
	intersectionTaskMemManager := rock_memery.NewTaskMemoryManager(rock_memery.MemoryManagerInstance)
	common.IntersectionSliceAgent = rock_memery.NewRockSliceAgent(intersectionTaskMemManager, rock_memery.IntersectionCalc)

	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Infof("shutdown v3 server ...")
	distribute.GEtcd.Close()
}

func importTable(c *gin.Context) {
	var source request.DataSource
	if err := c.ShouldBindJSON(&source); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		fmt.Println("_____________________请求异常:")
		fmt.Println(err)
		return
	}
	tableId := storage_utils.ImportTable(&source)
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"tableId": tableId,
		"info":    "Start the mission",
	})
}

func ExecuteSingleRowDecisionTree(c *gin.Context) {
	var req request.SingleRowDecisionTreeReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		fmt.Println("_____________________请求异常:")
		fmt.Println(err)
		return
	}
	resp := decision_tree.ExecuteSingleRowDecisionTree(&req)
	c.JSON(http.StatusOK, gin.H{
		"data": resp,
	})
}

func GetTableSampleData(c *gin.Context) {
	var req request.GetTableSampleDataReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		fmt.Println("_____________________请求异常:")
		fmt.Println(err)
		return
	}
	resp := decision_tree.GetTableSampleData(&req)
	c.JSON(http.StatusOK, gin.H{
		"data": resp,
	})
}

func HashJoin(c *gin.Context) {
	var req request.HashJoinReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		fmt.Println("_____________________请求异常:")
		fmt.Println(err)
		return
	}
	resp := intersection.NewHashJoin().HashJoinHttp(&req)
	c.JSON(http.StatusOK, gin.H{
		"data": resp,
	})
}

func IncDigRules(c *gin.Context) {
	var err error
	defer func() {
		if c.Keys == nil {
			c.Keys = map[string]any{}
		}
		if err != nil {
			c.Keys[closeType] = distribute.KTaskStatusFailed
		} else {
			c.Keys[closeType] = distribute.KTaskStatusDone
		}
	}()
	var source inc_rule_dig.IncRdsRequest
	if err = c.ShouldBindJSON(&source); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		fmt.Println("_____________________请求异常:")
		fmt.Println(err)
		return
	}
	logger.Infof("IncDigRules source %v", source)
	masterNodeId, err := distribute.GEtcd.GetMasterEndpoint(distribute.TRANSPORT_TYPE_GRPC)
	if err != nil {
		msg := fmt.Sprintf("从etcd无法获得主节点,error=%v", err)
		logger.Error(msg)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	client, err := distribute.NewClient(nil, distribute.WithEndpoints([]string{masterNodeId}))
	if err != nil {
		msg := fmt.Sprintf("new stream error:%v", err)
		logger.Error(msg)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	//defer client.Close()
	rdsReqBytes, _ := json.Marshal(&source)
	pbReq := &pb.IncRdsTaskReq{Req: rdsReqBytes}
	pbResp := &pb.IncRdsTaskResp{}
	r := &inc_rule_dig.IncRdsResponse{}
	client.RoundRobin(pb.Rock_IncRdsTask_FullMethodName, pbReq, pbResp, func() {
		err = json.Unmarshal(pbResp.GetResp(), r)
		if err != nil {
			logger.Error("json.Unmarshal failed, err=", err, string(pbResp.GetResp()))
			return
		}
	})
	client.Close()
	//err = rule_dig.MultiDigRules2(&source)
	if len(client.Errors()) > 0 {
		err = fmt.Errorf("incRds has some error")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"data":    r,
	})
}

func DeleteIncRdsTask(c *gin.Context) {
	var err error
	var source inc_rule_dig.DeleteTaskReq
	if err = c.ShouldBindJSON(&source); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		fmt.Println("_____________________请求异常:")
		fmt.Println(err)
		return
	}
	logger.Infof("DeleteIncRdsTask source %v", source)

	err = inc_rule_dig.DeleteIncRdsTaskByIndicator(source.Support, source.Confidence)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"success": true,
	})
}

// set memory
func SetMemory(c *gin.Context) {
	//var req request3.MemoryConfigReq
	//
	//if err := c.ShouldBindJSON(&req); err != nil {
	//	c.JSON(http.StatusBadRequest, gin.H{
	//		"error": err.Error(),
	//	})
	//	fmt.Println("_____________________请求异常:")
	//	fmt.Println(err)
	//	return
	//}
	//
	//rock_memery.SetMemoryConfig(&rock_memery.MemoryConfig{
	//	StorageMem:      req.StorageMem,
	//	ShuffleMem:      req.ShuffleMem,
	//	IntersectionMem: req.IntersectionMem,
	//})
	c.JSON(http.StatusOK, gin.H{
		"data": "Method has expired",
	})
}
