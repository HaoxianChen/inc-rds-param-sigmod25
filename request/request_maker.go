package request

import (
	"gitlab.grandhoo.com/rock/storage/storage2/utils/rpc/serializer"
	"time"
)

func makeMulti2Request() {
	r := MultiRDSRequest{
		DataSources: []DataSource{{
			Database:     "postgres",
			Host:         "127.0.0.1",
			Port:         5432,
			DatabaseName: "xxxx",
			TableName:    "xxxx",
			TableId:      "",
			User:         "xxxx",
			Password:     "xxxx",
			TableType:    0,
			DataLimit:    1000,
		}},
		TaskID:           time.Now().UnixMilli(),
		JoinKeys:         nil,
		OtherMappingKeys: nil,
		Rhs:              nil,
		UDFTabCols:       nil,
		ColumnsRole:      nil,
		SkipColumns:      nil,
		SkipYColumns:     nil,
		Eids:             nil,
		MutexGroup:       nil,
		RdsConf: RdsConfRequest{
			Confidence:                        0.9,
			Support:                           0.01,
			FPSupport:                         0.01,
			MultiRuleDig:                      true,
			FilterArr:                         nil,
			SampleCount:                       100,
			PolyMaxLength:                     0,
			PolyAllowableError:                0,
			MultiPolySampleGroup:              0,
			DecisionTreeSampleThreshold2Ratio: nil,
			ChanSize:                          0,
			PredicateSupportLimit:             0,
			TreeLevel:                         10,
			TopKLayer:                         10,
			TopKSize:                          10,
			DecisionTreeMaxDepth:              3,
			CrossTablePredicateSupp:           0.5,
			CubeSize:                          10,
			NoMultiRule:                       false,
			TreeChanSize:                      2,
			IsNeedRE:                          false,
			REParam:                           RuleExecuteParam{},
			FrequentSize:                      10,
			IsLocal:                           false,
			NumericalConf:                     "",
			DecisionTreeMaxRowSize:            100,
			EnumSize:                          10,
			SimilarThreshold:                  0.9,
			TableRuleLimit:                    1000,
			RdsSize:                           1000,
			EnableErRule:                      true,
			EnableTimeRule:                    true,
			EnableDecisionTree:                true,
			EnableEmbedding:                   true,
			EnableSimilar:                     true,
			EnableEnum:                        true,
		},
	}

	println(serializer.Jsonify(r))
}
