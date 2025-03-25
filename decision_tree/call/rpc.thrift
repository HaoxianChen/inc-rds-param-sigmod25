struct SplitRecord {
    // 这个只是测试用的，之后会给具体的
    1: i32 feature
    2: double splitValue
    3: double leftWeight
    4: double rightWeight
    5: double leftImpurity
    6: double rightImpurity
    7: double improvementProxy
}

/**
* AttrBasic 一些关于属性的简单统计信息
**/
struct AttrBasic {
    1: bool empty // 是否有有效值，非empty才考虑后续的一些值
    2: double NaNWeights // nan的计数放到这里来
    3: double min
    4: double max
    5: list<double> values // 属性的一些取值。对于非数值类型保存所有值，对数值类型属性只选取部分取值，如果属性取值不多的话，不用划分区间，有最大长度限制，如果超过最大长度限制，会给个空，要与本身没有这个属性的取值进行区分(如都是NaN)
    6: map<double, double> validClassWeightCount // 该属性上非NaN取值的实例中，label的权重统计
}

/**
* InsBasic 关于实例的一些简单统计信息
**/
struct PartitionInsBasic {
    1: i64 singleCount // fixme:这里的singleCount要能累加的话，需要各机器上的pivot没有重叠，不然就不能直接加
    2: i64 multiCount
    3: double weights // 实例权重累和
    4: map<double, i64> classSingleCount // 实例中各label的统计
    5: map<double, i64> classMultiCount
}

struct Interval {
    1: double left
    2: double right
}

struct AVC {
    1: i8 attrT
	2: i32 attr
	3: double nanCount
	4: list<double> attrVs
	5: list<map<double, double>> labelCount
}

struct PartitionRef {
    1: i64 splitId // 根据这两项确定要进行统计的分区
    2: bool isLeft
}

service ServeOnMaster {
    void UpdateSplitInfo(1: SplitRecord record) // 更新用于结点划分的信息，一个属性会调用一次这个，选出最佳属性

    // 终止
    void Stop()

}

service ServeOnWorker {
    void DataInit() // 这个函数主要是针对dataframe不变的情况下，多次执行
    map<i32, AttrBasic> CollectAttrBasicInfo(1: PartitionRef partition) // 收集本地数据中，各属性的一些基本信息
    PartitionInsBasic CollectInsBasicInfo(1: PartitionRef partition, 2: list<i32> relatedFeatures) // 收集本地数据中，实例的简单统计信息

    void BeforeGenAVC(1: map<i32, map<double, double>> nonNaNlabelWeights) // 除了一些初始化之外，额外传一些之后要使用的参数
    void AfterSplit() // 做一些收尾清理工作

    // 下面的AVC生成都是对一批数据来说的，一次只传一个属性可能有点浪费
    // 这里不在master上进行汇总，也就是说不用进行同步等待，到时候异步更新
    void GenGeneralAVC(1: PartitionRef partition, 2: map<i32, list<double>> smallAVCTasks, 3: map<i32, list<double>> conciseAVCTasks) // 生成small-avc和concise-avc
    list<AVC> GenPartialAVC(1: PartitionRef partition, 2: i32 featureId, 3: list<Interval> tasks) // 为各区间内部生成AVC直方图，这里同步等吧，返回的列表与区间的列表一一对应
    void Split(1: PartitionRef partition, 2: i64 newPartitionId, 3: i32 splitAttr, 4: double splitValue, 5: bool hasNaN) // 对实例进行划分

    // worker间的一些调用
    void MergeGeneralAVC(1: PartitionRef partition, 2: AVC avc) // 在一个worker上收集满一个属性的各AVC，一次只会做一个分区的统计，但还是指定一下分区，做验证

    void Clear() // 清理一下内存，这里同步等，不再异步了
    // 终止
    void Stop()
}
