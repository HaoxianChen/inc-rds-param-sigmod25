package rpchandler

import (
	"context"
	"encoding/json"
	"fmt"
	sharegrpc "gitlab.grandhoo.com/rock/rock-share/rpc/grpc"
	"gitlab.grandhoo.com/rock/rock_v3/inc_rule_dig"
	"runtime/debug"

	decision_tree "gitlab.grandhoo.com/rock/rock_v3/decision_tree/execute"
	"gitlab.grandhoo.com/rock/rock_v3/rpchandler/rpc_universal_call"
	"gitlab.grandhoo.com/rock/rock_v3/rule_dig"

	"gitlab.grandhoo.com/rock/rock-share/base/logger"
	"gitlab.grandhoo.com/rock/rock-share/rpc/grpc/pb"
	"gitlab.grandhoo.com/rock/rock_v3/request"
	"gitlab.grandhoo.com/rock/rock_v3/trees"
	"google.golang.org/grpc"
)

type RockServer struct {
}

func NewRockServer() *RockServer {
	return &RockServer{}
}

func (s *RockServer) IncRdsTask(ctx context.Context, in *pb.IncRdsTaskReq) (*pb.IncRdsTaskResp, error) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("recover", err)
			logger.Error(string(debug.Stack()))
		}
	}()
	req := &inc_rule_dig.IncRdsRequest{}
	err := json.Unmarshal(in.GetReq(), req)
	if err != nil {
		logger.Error("json.Unmarshal failed, err=", err, string(in.GetReq()))
		return nil, err
	}
	//err, loadDataTime, rdsTime, totalTime := rule_dig.MultiDigRules2(req)
	resp, err := rule_dig.IncDigRulesDistribute(req)
	if err != nil {
		return nil, err
	}
	resBytes, e := json.Marshal(resp)
	if e != nil {
		logger.Errorf("json.Marshal failed, err:%v, resp:%v", err, *resp)
		return nil, e
	}

	return &pb.IncRdsTaskResp{
		Resp: resBytes,
	}, nil
}

func (s *RockServer) ClearTask(ctx context.Context, req *pb.ClearTaskReq) (*pb.ClearTaskResp, error) {
	//TODO implement me
	panic("implement me")
}

func (s *RockServer) CorrectTask(ctx context.Context, in *pb.CorrectTaskReq) (*pb.CorrectTaskResp, error) {
	panic("implement me")
}

func (s *RockServer) CheckErrorTask(ctx context.Context, in *pb.CheckErrorTaskReq) (*pb.CheckErrorTaskResp, error) {
	panic("implement me")
}

func (s *RockServer) CreateBuckets(ctx context.Context, in *pb.CreateBucketsNewReq) (*pb.CreateBucketsNewResp, error) {
	panic("implement me")
}

func (s *RockServer) ShouldPrune(ctx context.Context, in *pb.ShouldPruneReq) (*pb.ShouldPruneResp, error) {
	panic("implement me")
}

func (s *RockServer) ChildrenShouldPrune(ctx context.Context, in *pb.ChildrenShouldPruneReq) (*pb.ChildrenShouldPruneResp, error) {
	panic("implement me")
}

func (s *RockServer) CalNotSatisfyNode(ctx context.Context, in *pb.CalNotSatisfyNodeReq) (*pb.CalNotSatisfyNodeResp, error) {
	req := &request.CalNotSatisfyNodeReq{}
	err := json.Unmarshal(in.GetReq(), req)
	if err != nil {
		logger.Error("json.Unmarshal failed, err=", err, string(in.GetReq()))
		return nil, err
	}
	resp := trees.CalNoeSatisfyNode(req)
	return &pb.CalNotSatisfyNodeResp{
		LhsCandidateGiniIndex:      resp.LhsCandidateGiniIndex,
		LhsCrossCandidateGiniIndex: resp.LhsCrossCandidateGiniIndex,
		RuleSize:                   int32(resp.RuleSize),
	}, nil
}

// GetCubeSlotLenOfNode deprecated
func (s *RockServer) GetCubeSlotLenOfNode(ctx context.Context, in *pb.GetCubeSlotLenReq) (*pb.GetCubeSlotLenResp, error) {
	panic("implement me")
}
func (s *RockServer) MoveBucket(ctx context.Context, in *pb.MoveBucketReq) (*pb.MoveBucketResp, error) {
	panic("implement me")
}

func (s *RockServer) GetCreateBucketsInfo(ctx context.Context, in *pb.CreateBucketsReq) (*pb.CreateBucketsResp, error) {
	panic("implement me")
}

func (s *RockServer) GetOtherNodeCubeKeys(svr pb.Rock_GetOtherNodeCubeKeysServer) error {
	panic("implement me")
}

func (s *RockServer) GetBucketAlloc(ctx context.Context, in *pb.GetBucketAllocReq) (*pb.GetBucketAllocResp, error) {
	panic("implement me")
}

func (s *RockServer) ShouldPruneStream(svr pb.Rock_ShouldPruneStreamServer) error {
	return nil
}

func (s *RockServer) UniversalCall(ctx context.Context, req *pb.UniversalReq) (*pb.UniversalResp, error) {
	fun, ok := rpc_universal_call.Router[req.Kind]
	if !ok {
		return &pb.UniversalResp{Err: fmt.Sprintf("Unknown call kind %d", req.Kind)}, nil
	}
	ret, err := fun(req.Data)
	if err == nil {
		return &pb.UniversalResp{Data: ret}, nil
	} else {
		return &pb.UniversalResp{Err: err.Error()}, nil
	}
}

func (s *RockServer) CheckError(ctx context.Context, in *pb.CheckErrorReq) (*pb.CheckErrorResp, error) {
	panic("implement me")
}

func (s *RockServer) CheckErrorEnter(ctx context.Context, in *pb.CheckErrorEnterReq) (*pb.CheckErrorEnterResp, error) {
	panic("implement me")
}

func (s *RockServer) MoveBucketStream(svr pb.Rock_MoveBucketStreamServer) error {
	panic("implement me")
}

func (s *RockServer) ExecuteSingleRowDecisionTree(ctx context.Context, in *pb.SingleRowDecisionTreeReq) (*pb.SingleRowDecisionTreeResp, error) {
	req := &request.SingleRowDecisionTreeReq{}
	err := json.Unmarshal(in.GetReq(), req)
	if err != nil {
		logger.Error("json.Unmarshal failed, err=", err, string(in.GetReq()))
		return nil, err
	}
	resp := decision_tree.ExecuteSingleRowDecisionTree(req)
	return &pb.SingleRowDecisionTreeResp{
		RuleSize: int32(resp.RuleSize),
		Success:  resp.Success,
	}, nil
}

func (s *RockServer) GetTableSampleData(in *pb.GetTableSampleDataReq, svr pb.Rock_GetTableSampleDataServer) error {
	logger.Infof("enter RockServer GetTableSampleData")
	defer func() {
		logger.Infof("exit RockServer GetTableSampleData")
	}()
	req := &request.GetTableSampleDataReq{}
	err := json.Unmarshal(in.GetReq(), req)
	if err != nil {
		logger.Error("json.Unmarshal failed, err=", err, string(in.GetReq()))
		return err
	}
	resp := decision_tree.GetTableSampleData(req)
	respBytes, err := json.Marshal(resp)
	if err != nil {
		logger.Error("json.Marshal failed, err=", err, string(respBytes))
		return err
	}
	respBytesLen := len(respBytes)
	for len(respBytes) > 0 {
		end := sharegrpc.KMaxRecvSendMsgSize - 1024
		if end > len(respBytes) {
			end = len(respBytes)
		}
		err = svr.SendMsg(&pb.ServerStreamResp{
			Resp: respBytes[:end],
		})
		if err != nil {
			logger.Error("stream.SendMsg failed, err=", err)
			return err
		}
		respBytes = respBytes[end:]
	}
	//svr.Send(&pb.ServerStreamResp{Resp: respBytes})
	logger.Debugf("svr send resp:%d", respBytesLen)
	return nil
}

func (s *RockServer) RdsEnter(ctx context.Context, in *pb.RdsEnterReq) (*pb.RdsEnterResp, error) {
	panic("implement me")
}

func (s *RockServer) CorrectStream(in *pb.CorrectReq, server pb.Rock_CorrectStreamServer) error {
	panic("implement me")
}

func (s *RockServer) StartBucketSampleForRds(server pb.Rock_StartBucketSampleForRdsServer) error {
	panic("implement me")
}

func (s *RockServer) GetBucketSampleForRds(server pb.Rock_GetBucketSampleForRdsServer) error {
	panic("implement me")
}

func (s *RockServer) DropCacheDataBase(ctx context.Context, req *pb.DropCacheDBReq) (*pb.DropCacheDBResp, error) {
	panic("implement me")
}

func (s *RockServer) CorrectEnterStream(server pb.Rock_CorrectEnterStreamServer) error {
	panic("implement me")
}

func Registry(gRPCServer *grpc.Server) {
	pb.RegisterRockServer(gRPCServer, NewRockServer())
}
