import math
import time

from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import array, expr
from pyspark.sql import functions as F

key_experiment_name = 'experiment_name'
key_dataset_name = 'dataset_name'
key_thresholds = 'thresholds'
key_vary_node_num = 'vary node num'
key_vary_dataset_size = 'vary dataset size'
key_dataset_names = 'dataset_names'
key_sample_recalls = 'sample recalls'


def init_spark_session():
    # 初始化 Spark 会话
    spark = SparkSession.builder \
        .appName("IApriori-FPGrowth") \
        .getOrCreate()
    return spark


def load_data(spark, dataset_name):
    # 读取 CSV 数据
    csv_path = "file:///data/Apriori/dataset/" + str(dataset_name) + ".csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    df.show()
    # 获取所有列名
    columns = df.columns

    # 假设第一列是 ID 或其他不需要参与项集生成的列，移除这些列
    # 如果你的数据中有 ID 列、时间戳等可以忽略的列，应该根据实际情况调整
    # columns_to_include = columns[1:]  # 假设第一列为 id，跳过它

    # 使用剩余的列创建 'items' 列
    df = df.withColumn("features", array(*columns))
    df.show(truncate=False)
    df_transformed = df.withColumn("unique_features", F.expr("array_distinct(features)"))
    # df_transformed.show(truncate=False)

    # 去掉 'items' 列中的空值
    df_transformed = df_transformed.withColumn(
        "unique_features", expr("filter(unique_features, x -> x is not null)")
    )
    return df_transformed


def fp_growth(df, src_min_support, min_confidence, experiment_name, dataset, num_partitions):
    # support confidence需要开平方
    min_support = math.sqrt(src_min_support)
    # min_confidence = math.sqrt(src_min_confidence)
    # FP-growth 算法
    # FPGrowth 需要的数据格式是 DataFrame，其中一列是交易 ID（在这里是 "id"），
    # 另一列是包含项集的列表（在这里是 "items"）。
    # 这里使用了 Transaction（事务）列表示每个交易中的商品项。
    fpgrowth = FPGrowth(itemsCol="unique_features", minSupport=min_support, minConfidence=min_confidence,
                        numPartitions=num_partitions)

    # 训练模型
    model = fpgrowth.fit(df)

    # 获取频繁项集
    print(f"****** start get frequent itemSets...")
    fre_itemsets_start = time.time()
    model.freqItemsets.show()
    fre_itemsets_time = time.time() - fre_itemsets_start

    # 获取关联规则
    print(f"****** start get association rules...")
    rule_gen_start = time.time()
    model.associationRules.show()
    rule_gen_time = time.time() - rule_gen_start

    print(
        f"****** finish experiment_name:{experiment_name} dataset:{dataset} src_min_support:{src_min_support} min_support:{min_support} min_confidence:{min_confidence} frequent itemSets time:{fre_itemsets_time} gen rule time:{rule_gen_time}")


def vary_dataset_fp_growth(df, src_min_support, min_confidence, experiment_name, dataset, num_partitions, sample):
    # 按设置的抽样比抽样
    sample_df = df.sample(False, sample, seed=42)

    # support confidence需要开平方
    min_support = math.sqrt(src_min_support)
    # min_confidence = math.sqrt(src_min_confidence)
    # FP-growth 算法
    # FPGrowth 需要的数据格式是 DataFrame，其中一列是交易 ID（在这里是 "id"），
    # 另一列是包含项集的列表（在这里是 "items"）。
    # 这里使用了 Transaction（事务）列表示每个交易中的商品项。
    fpgrowth = FPGrowth(itemsCol="unique_features", minSupport=min_support, minConfidence=min_confidence,
                        numPartitions=num_partitions)

    # 训练模型
    model = fpgrowth.fit(sample_df)

    # 获取频繁项集
    print(f"****** start get frequent itemSets...")
    fre_itemsets_start = time.time()
    model.freqItemsets.show()
    fre_itemsets_time = time.time() - fre_itemsets_start

    # 获取关联规则
    print(f"****** start get association rules...")
    rule_gen_start = time.time()
    association_rules = model.associationRules
    rule_gen_time = time.time() - rule_gen_start

    association_rules = association_rules.withColumn("antecedent_str", F.concat_ws(" ^ ", "antecedent"))
    association_rules = association_rules.withColumn("consequent_str", F.concat_ws(" ^ ", "consequent"))

    select_output = association_rules.select("antecedent_str", "consequent_str", "support", "confidence")

    output_file_name = "association_rules_" + str(sample) + ".csv"
    output_path = "/data/Apriori/path_to_output_csv/" + output_file_name
    select_output.write.option("header", "true").csv(output_path)

    print(f"Filtered association rules have been saved to {output_path}")


    print(
        f"****** finish sample:{sample} experiment_name:{experiment_name} dataset:{dataset} src_min_support:{src_min_support} min_support:{min_support} min_confidence:{min_confidence} frequent itemSets time:{fre_itemsets_time} gen rule time:{rule_gen_time}")
    return fre_itemsets_time, rule_gen_time


def main():
    spark_session = init_spark_session()

    experiment_params = [
        # {
        #     key_experiment_name: 'support increase',
        #     key_dataset_name: 'adult',
        #     key_thresholds: [(0.00001, 0.75), (0.0001, 0.75), (0.001, 0.75), (0.01, 0.75)]
        # },
        # {
        #     key_experiment_name: 'support decrease',
        #     key_dataset_name: 'inspection',
        #     key_thresholds: [(0.001, 0.75), (0.0001, 0.75), (0.00001, 0.75), (0.000001, 0.75)]
        # },
        # {
        #     key_experiment_name: 'confidence increase',
        #     key_dataset_name: 'inspection',
        #     key_thresholds: [(0.000001, 0.75), (0.000001, 0.8), (0.000001, 0.85), (0.000001, 0.9), (0.000001, 0.95)]
        # },
        # {
        #     key_experiment_name: 'confidence decrease',
        #     key_dataset_name: 'inspection',
        #     key_thresholds: [(0.000001, 0.9), (0.000001, 0.85), (0.000001, 0.8), (0.000001, 0.75), (0.000001, 0.7)]
        # },
        # {
        #     key_experiment_name: 'supp inc | conf inc',
        #     key_dataset_name: 'inspection',
        #     key_thresholds: [(0.00001, 0.75), (0.0001, 0.8), (0.001, 0.85), (0.01, 0.9)]
        # },
        # {
        #     key_experiment_name: 'supp inc | conf dec',
        #     key_dataset_name: 'adult',
        #     key_thresholds: [(0.00001, 0.9), (0.0001, 0.85), (0.001, 0.8), (0.01, 0.75)]
        # },
        # {
        #     key_experiment_name: 'supp dec | conf inc',
        #     key_dataset_name: 'inspection',
        #     key_thresholds: [(0.001, 0.75), (0.0001, 0.8), (0.00001, 0.85), (0.000001, 0.9)]
        # },
        # {
        #     key_experiment_name: 'supp dec | conf dec',
        #     key_dataset_name: 'adult',
        #     key_thresholds: [(0.001, 0.9), (0.0001, 0.85), (0.00001, 0.8), (0.000001, 0.75)]
        # },
        # {
        #     key_experiment_name: '(old change) supp inc|conf inc',
        #     key_dataset_name: 'inspection',
        #     key_thresholds: [(0.00001, 0.8), (0.0001, 0.85), (0.001, 0.9), (0.01, 0.95)]
        # },
        # {
        #     key_experiment_name: '(old change) supp dec|conf dec',
        #     key_dataset_name: 'adult',
        #     key_thresholds: [(0.001, 0.9), (0.0001, 0.85), (0.00001, 0.8), (0.000001, 0.75)]
        # },
        # {
        #     key_experiment_name: 'vary n',
        #     key_dataset_name: 'inspection',
        #     key_thresholds: [(0.000001, 0.7)],
        #     key_vary_node_num: [4, 8, 12, 16, 20]
        # },
        {
            key_experiment_name: 'vary |D| size',
            # key_dataset_names: ['AMiner_Author', 'AMiner_Author2Paper', 'AMiner_Paper', 'dblp'],
            key_dataset_names: ['parksong'],
            key_sample_recalls: [0.2, 0.4, 0.6, 0.8, 1.0],
            key_thresholds: [(0.000001, 0.7)]
        }
    ]

    # experiment_params = [
    #     {
    #         key_experiment_name: 'vary n',
    #         key_dataset_name: 'inspection',
    #         key_thresholds: [(0.000001, 0.7)],
    #         key_vary_node_num: [4, 8, 12, 16, 20]
    #     },
    #     {
    #         key_experiment_name: 'vary |D| size',
    #         key_dataset_names: ['AMiner_Author', 'AMiner_Author2Paper', 'AMiner_Paper', 'dblp'],
    #         key_sample_recalls: [0.2, 0.4, 0.6, 0.8, 1.0],
    #         key_thresholds: [(0.000001, 0.7)]
    #     }
    # ]

    for experiment_param in experiment_params:
        experiment_name = experiment_param[key_experiment_name]
        if experiment_name == 'vary n':
            dataset_name = experiment_param[key_dataset_name]
            df = load_data(spark_session, dataset_name)
            experiment_threshold = experiment_param[key_thresholds][0]
            node_nums = experiment_param[key_vary_node_num]
            for node_num in node_nums:
                print(
                    f"experiment_name:{experiment_name} node_num:{node_num} dataset_name:{dataset_name} support:{experiment_threshold[0]} confidence:{experiment_threshold[1]}")
                fp_growth(df, experiment_threshold[0], experiment_threshold[1], experiment_name, dataset_name, node_num)
        elif experiment_name == 'vary |D| size':
            dataset_names = experiment_param[key_dataset_names]
            samples = experiment_param[key_sample_recalls]
            experiment_threshold = experiment_param[key_thresholds][0]
            for sample in samples:
                fre_itemsets_total_time = 0
                rule_gen_total_time = 0
                for dataset_name in dataset_names:
                    print(
                        f"experiment_name:{experiment_name} dataset_name:{dataset_name} sample:{sample}, support:{experiment_threshold[0]} confidence:{experiment_threshold[1]}")
                    df = load_data(spark_session, dataset_name)
                    fre_itemsets_time, rule_gen_time = vary_dataset_fp_growth(df, experiment_threshold[0], experiment_threshold[1], experiment_name, dataset_name, 20, sample)
                    fre_itemsets_total_time += fre_itemsets_time
                    rule_gen_total_time += rule_gen_time
                print(f"***** finish sample:{sample} vary |D| size, fre_total_time:{fre_itemsets_total_time}, rule_gen_total_time:{rule_gen_total_time}")

        else:
            dataset_name = experiment_param[key_dataset_name]
            df = load_data(spark_session, dataset_name)
            experiment_thresholds = experiment_param[key_thresholds]
            for experiment_threshold in experiment_thresholds:
                print(
                    f"experiment_name:{experiment_name} dataset_name:{dataset_name} support:{experiment_threshold[0]} confidence:{experiment_threshold[1]}")
                fp_growth(df, experiment_threshold[0], experiment_threshold[1], experiment_name, dataset_name, 20)

    # 停止 Spark 会话
    spark_session.stop()


if __name__ == '__main__':
    main()
