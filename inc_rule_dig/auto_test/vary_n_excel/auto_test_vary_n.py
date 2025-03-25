import json
import time

import pandas as pd
import requests
import numpy as np

# import argparse

# parser = argparse.ArgumentParser()
# parser.add_argument('i', type=str, help='数据输入路径')
# parser.add_argument('o', type=str, help='结果输出路径')
# parser.add_argument('s', type=float, help='support')
# parser.add_argument('c', type=float, help='confidence')

retry_count = 3

COLUMN_DATASET_NAME = 'dataset name'
COLUMN_OLD_SUPPORT = 'old support'
COLUMN_OLD_CONFIDENCE = 'old confidence'
COLUMN_NEW_SUPPORT = 'new support'
COLUMN_NEW_CONFIDENCE = 'new confidence'
COLUMN_BASELINE = 'Baseline'
COLUMN_K = 'K'
COLUMN_BETA = 'beta'
COLUMN_WORKER_INSTANCES = 'worker instances'
COLUMN_MINING_TIME = 'Mining time'
COLUMN_MIN_REE_NUM = 'minimal REE number'
COLUMN_PRUNE_REE_NUM = 'prune REE number'
COLUMN_SAMPLE_NUM = 'sample number'
COLUMN_TASK_ID = 'taskId'
COLUMN_RECALL_RATE = 'recall rate'
COLUMN_DATASET_SIZE = '|D| size'
COLUMN_OUTPUT_SIZE = 'output size'
COLUMN_AUXILIARY_SIZE = 'auxiliary size'
COLUMN_USE_NEIGHBOR_CONFS = 'use neighbor confs'
COLUMN_MINIMAL_SIZE = 'minimal size'
COLUMN_DECISION_TREE_REE_NUM = 'decision tree REE number'
COLUMN_DECISION_TREE_SIZE = 'decision tree size'

RSP_KEY_PRE = 'data'
RSP_KEY_TOTAL_TIME = 'TotalTime'
RSP_KEY_UPDATED_TASK_ID = 'UpdatedTaskId'
RSP_KEY_MIN_REE_SIZE = 'MinimalREEsSize'
RSP_KEY_PRUNE_REE_SIZE = 'PruneREEsSize'
RSP_KEY_SAMPLE_SIZE = 'SampleSize'
RSP_KEY_RECALL_RATE = 'RecallRate'
RSP_KEY_OUTPUT_SIZE = 'OutputSize'
RSP_KEY_AUXILIARY_SIZE = 'AuxiliarySize'
RSP_KEY_MINIMAL_SIZE = 'MinimalSize'
RSP_KEY_DECISION_TREE_REE_NUM = 'DecisionTreeREEsSize'
RSP_KEY_DECISION_TREE_SIZE = 'DecisionTreeSize'

# DATASET_SIZE_20_PERCENT = '20%'
# DATASET_SIZE_40_PERCENT = '40%'
# DATASET_SIZE_60_PERCENT = '60%'
# DATASET_SIZE_80_PERCENT = '80%'
# DATASET_SIZE_100_PERCENT = '100%'
DATASET_SIZE_20_PERCENT = '0.2'
DATASET_SIZE_40_PERCENT = '0.4'
DATASET_SIZE_60_PERCENT = '0.6'
DATASET_SIZE_80_PERCENT = '0.8'
DATASET_SIZE_100_PERCENT = '1.0'

# data_limit_20_percent_list = [68498, 207720, 83695, 8320]
# data_limit_40_percent_list = [136995, 415440, 167389, 16640]
# data_limit_60_percent_list = [205492, 623160, 251083, 24960]
# data_limit_80_percent_list = [273989, 830880, 334777, 33280]
# data_limit_100_percent_list = [342486, 1038599, 418471, 41599]

data_limit_20_percent_list = [1000000]
data_limit_40_percent_list = [2000000]
data_limit_60_percent_list = [3000000]
data_limit_80_percent_list = [4000000]
data_limit_100_percent_list = [5000000]

data_table_name_20_percent_list = ['parksong_100w']
data_table_name_40_percent_list = ['parksong_200w']
data_table_name_60_percent_list = ['parksong_300w']
data_table_name_80_percent_list = ['parksong_400w']
data_table_name_100_percent_list = ['parksong']


table_2_dt_max_row_size = {
    'inc_rds.inspection': 200000,
}


def get_vary_dataset_size_old_task_id(experiment_df, old_support, old_confidence, dataset_size):
    for _, row in experiment_df.iterrows():
        support = row[COLUMN_NEW_SUPPORT]
        confidence = row[COLUMN_NEW_CONFIDENCE]
        vary_dataset_size = str(row[COLUMN_DATASET_SIZE])
        if support == old_support and confidence == old_confidence and vary_dataset_size == str(dataset_size):
            return row[COLUMN_TASK_ID]
    return None


def get_recall_batch_task_id(experiment_df, new_support, new_confidence):
    for _, row in experiment_df.iterrows():
        support = row[COLUMN_NEW_SUPPORT]
        confidence = row[COLUMN_NEW_CONFIDENCE]
        baseline = row[COLUMN_BASELINE]
        if support == new_support and confidence == new_confidence and baseline == 'batch':
            return row[COLUMN_TASK_ID]
    return np.nan


def get_old_task_id(experiment_df, old_support, old_confidence, baseline):
    old_task_tuples = []
    for _, row in experiment_df.iterrows():
        if row[COLUMN_NEW_SUPPORT] == old_support and row[COLUMN_NEW_CONFIDENCE] == old_confidence:
            old_task_tuple = (row[COLUMN_TASK_ID], row[COLUMN_BASELINE])
            old_task_tuples.append(old_task_tuple)

    if len(old_task_tuples) == 0:
        return None
    if len(old_task_tuples) < 2:
        return old_task_tuples[0][0]
    else:
        for old_task_tuple in old_task_tuples:
            if old_task_tuple[1] == baseline:
                return old_task_tuple[0]

    return None


def get_vary_k_old_task_id(experiment_df, old_support, old_confidence, k):
    for _, row in experiment_df.iterrows():
        support = row[COLUMN_NEW_SUPPORT]
        confidence = row[COLUMN_NEW_CONFIDENCE]
        vary_k = row[COLUMN_K]
        if support == old_support and confidence == old_confidence and vary_k == k:
            return row[COLUMN_TASK_ID]
    return None


def send_http_to_rock(url, req_json):
    # 发送post请求
    response = requests.post(url, json=req_json)
    if response.status_code == 200:
        return response.json()
        # total_time = data.get(RSP_KEY_TOTAL_TIME)
        # updated_task_id = data.get(RSP_KEY_UPDATED_TASK_ID)
        # min_ree_size = data.get(RSP_KEY_MIN_REE_SIZE)
        # prune_ree_size = data.get(RSP_KEY_PRUNE_REE_SIZE)
        # sample_size = data.get(RSP_KEY_SAMPLE_SIZE)
        # recall_rate = data.get(RSP_KEY_RECALL_RATE)
        # print(
        #     f"total_time:{total_time}, updated_task_id:{updated_task_id}, min_ree_size:{min_ree_size}, prune_ree_size:{prune_ree_size}, sample_size:{sample_size}, recall_rate:{recall_rate}")
    else:
        raise Exception(f"post request failed, status code:{response.status_code}")


def execute_task(row, url, experiment_df, req_json_data, experiment_name, golden_rules, retries=3):
    try:
        if len(golden_rules) > 0:
            req_json_data["goldenRules"] = golden_rules
        if row[COLUMN_USE_NEIGHBOR_CONFS] == 'Y':
            req_json_data["useNeighborConfs"] = True
        else:
            req_json_data["useNeighborConfs"] = False
        if experiment_name == 'vary |D| size':
            dataset_size = str(row[COLUMN_DATASET_SIZE])
            if dataset_size == DATASET_SIZE_20_PERCENT:
                for index, table_name in enumerate(data_table_name_20_percent_list):
                    req_json_data["dataSources"][index]["tableName"] = table_name
                    req_json_data["decisionTreeMaxRowSize"] = 10000
            elif dataset_size == DATASET_SIZE_40_PERCENT:
                for index, table_name in enumerate(data_table_name_40_percent_list):
                    req_json_data["dataSources"][index]["tableName"] = table_name
                    req_json_data["decisionTreeMaxRowSize"] = 20000
            elif dataset_size == DATASET_SIZE_60_PERCENT:
                for index, table_name in enumerate(data_table_name_60_percent_list):
                    req_json_data["dataSources"][index]["tableName"] = table_name
                    req_json_data["decisionTreeMaxRowSize"] = 30000
            elif dataset_size == DATASET_SIZE_80_PERCENT:
                for index, table_name in enumerate(data_table_name_80_percent_list):
                    req_json_data["dataSources"][index]["tableName"] = table_name
                    req_json_data["decisionTreeMaxRowSize"] = 40000
            else:
                for index, table_name in enumerate(data_table_name_100_percent_list):
                    req_json_data["dataSources"][index]["tableName"] = table_name
                    req_json_data["decisionTreeMaxRowSize"] = 50000
            # if dataset_size == DATASET_SIZE_20_PERCENT:
            #     for index, size in enumerate(data_limit_20_percent_list):
            #         req_json_data["dataSources"][index]["dataLimit"] = size
            # elif dataset_size == DATASET_SIZE_40_PERCENT:
            #     for index, size in enumerate(data_limit_40_percent_list):
            #         req_json_data["dataSources"][index]["dataLimit"] = size
            # elif dataset_size == DATASET_SIZE_60_PERCENT:
            #     for index, size in enumerate(data_limit_60_percent_list):
            #         req_json_data["dataSources"][index]["dataLimit"] = size
            # elif dataset_size == DATASET_SIZE_80_PERCENT:
            #     for index, size in enumerate(data_limit_80_percent_list):
            #         req_json_data["dataSources"][index]["dataLimit"] = size
            # else:
            #     for index, size in enumerate(data_limit_100_percent_list):
            #         req_json_data["dataSources"][index]["dataLimit"] = size
        else:
            dataset_name = row[COLUMN_DATASET_NAME]
            req_json_data["dataSources"][0]["tableName"] = dataset_name
            decision_tree_max_row_size = table_2_dt_max_row_size.get(dataset_name, -1)
            req_json_data["decisionTreeMaxRowSize"] = decision_tree_max_row_size
        baseline = row[COLUMN_BASELINE]
        # for index, row in experiment_df.iterrows():
        if baseline == 'batch':
            req_json_data["preTaskID"] = int(-1)
            req_json_data["preSupport"] = int(-1)
            req_json_data["preConfidence"] = int(-1)
            req_json_data["withoutSampling"] = False
        elif baseline == 'IncMiner':
            old_support = row[COLUMN_OLD_SUPPORT]
            old_confidence = row[COLUMN_OLD_CONFIDENCE]
            if experiment_name == 'vary K':
                pre_task_id = get_vary_k_old_task_id(experiment_df, old_support, old_confidence, row[COLUMN_K])
            elif experiment_name == 'vary |D| size':
                pre_task_id = get_vary_dataset_size_old_task_id(experiment_df, old_support, old_confidence,
                                                                row[COLUMN_DATASET_SIZE])
            else:
                pre_task_id = get_old_task_id(experiment_df, old_support, old_confidence, baseline)
            req_json_data["preTaskID"] = int(pre_task_id)
            req_json_data["preSupport"] = int(0)
            req_json_data["preConfidence"] = int(0)
            req_json_data["withoutSampling"] = False
        elif baseline == 'IncMinerNS':
            old_support = row[COLUMN_OLD_SUPPORT]
            old_confidence = row[COLUMN_OLD_CONFIDENCE]
            if experiment_name == 'vary K':
                pre_task_id = get_vary_k_old_task_id(experiment_df, old_support, old_confidence, row[COLUMN_K])
            elif experiment_name == 'vary |D| size':
                pre_task_id = get_vary_dataset_size_old_task_id(experiment_df, old_support, old_confidence,
                                                                row[COLUMN_DATASET_SIZE])
            else:
                pre_task_id = get_old_task_id(experiment_df, old_support, old_confidence, baseline)
            req_json_data["preTaskID"] = int(pre_task_id)
            req_json_data["preSupport"] = int(0)
            req_json_data["preConfidence"] = int(0)
            req_json_data["withoutSampling"] = True
        new_support = row[COLUMN_NEW_SUPPORT]
        req_json_data["updatedSupport"] = float(new_support)
        new_confidence = row[COLUMN_NEW_CONFIDENCE]
        req_json_data["updatedConfidence"] = float(new_confidence)
        K = row[COLUMN_K]
        req_json_data["coverRadius"] = int(K)
        beta = row[COLUMN_BETA]
        if np.isnan(beta):
            req_json_data["useCDF"] = False
        else:
            req_json_data["useCDF"] = True
            req_json_data["recallBound"] = float(beta)
        recall_batch_task_id = get_recall_batch_task_id(experiment_df, row[COLUMN_NEW_SUPPORT],
                                                        row[COLUMN_NEW_CONFIDENCE])
        if np.isnan(recall_batch_task_id):
            recall_batch_task_id = int(-1)
        req_json_data["recallRateBatchMinerTaskId"] = int(recall_batch_task_id)

        print(f"req_json_data:{req_json_data}")
        # if retries > 1:
        #     raise ValueError("cannot divide by zero")

        # total_time = 1000
        # updated_task_id = 1000
        # min_ree_size = 1000
        # prune_ree_size = 1000
        # sample_size = 1000
        # recall_rate = 1000
        # output_size = 1000
        # auxiliary_size = 1000
        # minimal_size = 1000
        # decision_tree_ree_num = 1000
        # decision_tree_size = 1000

        task_result = send_http_to_rock(url, req_json_data)
        print(f"task_result:{task_result}")
        total_time = task_result[RSP_KEY_PRE][RSP_KEY_TOTAL_TIME]
        updated_task_id = task_result[RSP_KEY_PRE][RSP_KEY_UPDATED_TASK_ID]
        min_ree_size = task_result[RSP_KEY_PRE][RSP_KEY_MIN_REE_SIZE]
        prune_ree_size = task_result[RSP_KEY_PRE][RSP_KEY_PRUNE_REE_SIZE]
        sample_size = task_result[RSP_KEY_PRE][RSP_KEY_SAMPLE_SIZE]
        recall_rate = task_result[RSP_KEY_PRE][RSP_KEY_RECALL_RATE]
        output_size = task_result[RSP_KEY_PRE][RSP_KEY_OUTPUT_SIZE]
        auxiliary_size = task_result[RSP_KEY_PRE][RSP_KEY_AUXILIARY_SIZE]
        minimal_size = task_result[RSP_KEY_PRE][RSP_KEY_MINIMAL_SIZE]
        decision_tree_ree_num = task_result[RSP_KEY_PRE][RSP_KEY_DECISION_TREE_REE_NUM]
        decision_tree_size = task_result[RSP_KEY_PRE][RSP_KEY_DECISION_TREE_SIZE]

        return {
            COLUMN_MINING_TIME: total_time,
            COLUMN_TASK_ID: updated_task_id,
            COLUMN_MIN_REE_NUM: min_ree_size,
            COLUMN_PRUNE_REE_NUM: prune_ree_size,
            COLUMN_SAMPLE_NUM: sample_size,
            COLUMN_RECALL_RATE: recall_rate,
            COLUMN_OUTPUT_SIZE: output_size,
            COLUMN_AUXILIARY_SIZE: auxiliary_size,
            COLUMN_MINIMAL_SIZE: minimal_size,
            COLUMN_DECISION_TREE_REE_NUM: decision_tree_ree_num,
            COLUMN_DECISION_TREE_SIZE: decision_tree_size
        }

        # experiment_df.loc[i, COLUMN_MINING_TIME] = total_time
        # experiment_df.loc[i, COLUMN_TASK_ID] = updated_task_id
        # experiment_df.loc[i, COLUMN_MIN_REE_NUM] = min_ree_size
        # experiment_df.loc[i, COLUMN_PRUNE_REE_NUM] = prune_ree_size
        # experiment_df.loc[i, COLUMN_SAMPLE_NUM] = sample_size
        # experiment_df.loc[i, COLUMN_RECALL_RATE] = recall_rate
        # experiment_df.loc[i, COLUMN_OUTPUT_SIZE] = output_size
        # experiment_df.loc[i, COLUMN_AUXILIARY_SIZE] = auxiliary_size
        # experiment_df.loc[i, COLUMN_MINIMAL_SIZE] = minimal_size
        # experiment_df.loc[i, COLUMN_DECISION_TREE_REE_NUM] = decision_tree_ree_num
        # experiment_df.loc[i, COLUMN_DECISION_TREE_SIZE] = decision_tree_size
    except Exception as e:
        # sleep 5min 等待服务重启好后继续执行
        time.sleep(300)
        retries -= 1
        if retries > 0:
            print(f"Exception occurred: {e}. Retrying...({retries} retries left)")
            return execute_task(row, url, experiment_df, req_json_data, experiment_name, golden_rules, retries)
        else:
            print(f"Task failed after 3 retries")
            raise Exception(e)


def execute_experiment(url, experiment_df, req_json_data, experiment_name, golden_rules):
    row_num = len(experiment_df)
    for i in range(row_num):
        # if i < 5:
        #     continue
        row = experiment_df.iloc[i]
        try:
            task_resp = execute_task(row, url, experiment_df, req_json_data, experiment_name, golden_rules)
            experiment_df.loc[i, COLUMN_MINING_TIME] = task_resp[COLUMN_MINING_TIME]
            experiment_df.loc[i, COLUMN_TASK_ID] = task_resp[COLUMN_TASK_ID]
            experiment_df.loc[i, COLUMN_MIN_REE_NUM] = task_resp[COLUMN_MIN_REE_NUM]
            experiment_df.loc[i, COLUMN_PRUNE_REE_NUM] = task_resp[COLUMN_PRUNE_REE_NUM]
            experiment_df.loc[i, COLUMN_SAMPLE_NUM] = task_resp[COLUMN_SAMPLE_NUM]
            experiment_df.loc[i, COLUMN_RECALL_RATE] = task_resp[COLUMN_RECALL_RATE]
            experiment_df.loc[i, COLUMN_OUTPUT_SIZE] = task_resp[COLUMN_OUTPUT_SIZE]
            experiment_df.loc[i, COLUMN_AUXILIARY_SIZE] = task_resp[COLUMN_AUXILIARY_SIZE]
            experiment_df.loc[i, COLUMN_MINIMAL_SIZE] = task_resp[COLUMN_MINIMAL_SIZE]
            experiment_df.loc[i, COLUMN_DECISION_TREE_REE_NUM] = task_resp[COLUMN_DECISION_TREE_REE_NUM]
            experiment_df.loc[i, COLUMN_DECISION_TREE_SIZE] = task_resp[COLUMN_DECISION_TREE_SIZE]
        except Exception as e:
            print(f"[execute_experiment] execute task failed, exception:{e}")
            break
    return experiment_df


def load_golden_rules_file():
    with open('golden_rules.txt', 'r') as golden_file:
        lines_list = [line.strip() for line in golden_file]
    return lines_list


if __name__ == '__main__':
    ip = '127.0.0.1'
    port = '19124'
    excel_file = 'auto_test_vary_n.xlsx'

    xls = pd.ExcelFile(excel_file)

    # 组装URL
    url = 'http://' + ip + ":" + port + "/inc-rds"
    print(f"URL:{url}")

    # 遍历每个工作表并处理
    with pd.ExcelWriter('output.xlsx', engine='openpyxl') as writer:
        for sheet_name in xls.sheet_names:
            print(f"execute experiment:{sheet_name}")
            if sheet_name == 'vary |D| size':
                with open('vary_dataset_size_req_json_new.json', 'r', encoding='utf-8') as file:
                    json_data = json.load(file)
                    golden_lines = []
            else:
                with open('request_json.json', 'r') as file:
                    json_data = json.load(file)
                golden_lines = []
                if 'Guidelines'.lower() in sheet_name.lower():
                    golden_lines = load_golden_rules_file()
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=0, index_col=0)
            df = execute_experiment(url, df, json_data, sheet_name, golden_lines)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
