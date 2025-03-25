import itertools
import json
import math
import threading
import time

import pandas as pd
import requests
import numpy as np

COLUMN_DATASET_NAME = 'dataset name'
COLUMN_NEW_SUPPORT = 'new support'
COLUMN_NEW_CONFIDENCE = 'new confidence'
COLUMN_WORKER_INSTANCES = 'worker instances'
COLUMN_DATASET_SIZE = '|D| size'

# 创建一个从100000开始的自增生成器
task_id_generator = itertools.count(1001130)
task_id_lock = threading.Lock()

# 定位任务最大并发数
MAX_CONCURRENT = 3

# 请求超时时间10小时
timeout_seconds = 36000

semaphore = threading.Semaphore(MAX_CONCURRENT)


def get_fix_support(support, sample_ratio):
    fix_support = support
    if sample_ratio < 1.0:
        fix_support = fix_support / math.pow(sample_ratio, 2)
    return fix_support


def generate_task_id():
    with task_id_lock:
        return next(task_id_generator)


def get_result_store_path(task_id):
    return "/user/hive/warehouse/db_rule_discover_result.db/rule_result_" + str(task_id)


def send_http_to_rock(url, req_json):
    # 发送post请求
    try:
        response = requests.post(url, json=req_json, timeout=timeout_seconds)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"post request failed, status code:{response.status_code}")
    except requests.exceptions.Timeout:
        response = {'code': '0', 'msg': f"the request time out after {timeout_seconds} seconds"}
        return json.dumps(response)


def get_req_json(dataset_name):
    req_json_path = 'req_json/' + str(dataset_name) + "_req.json"
    with open(req_json_path, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
    return json_data


def execute_task(task_id, row, url, experiment_df, experiment_name, retries=3):
    try:
        # task_id = generate_task_id()
        dataset_name = row[COLUMN_DATASET_NAME]
        new_support = row[COLUMN_NEW_SUPPORT]
        new_confidence = row[COLUMN_NEW_CONFIDENCE]
        req_json_data = get_req_json(dataset_name)
        # 修改support
        if experiment_name == 'vary large |D| size':
            # fix support
            dataset_size = row[COLUMN_DATASET_SIZE]
            new_fix_support = get_fix_support(new_support, dataset_size)
            req_json_data["configurations"]["cr"] = float(new_fix_support)
        else:
            req_json_data["configurations"]["cr"] = new_support
        # 修改confidence
        req_json_data["configurations"]["ftr"] = new_confidence
        # 修改task_id
        req_json_data["taskId"] = task_id
        # 修改resultStorePath
        result_store_path = get_result_store_path(task_id)
        req_json_data["resultStorePath"] = result_store_path

        if experiment_name == 'vary |D| size':
            dataset_size = row[COLUMN_DATASET_SIZE]
            req_json_data["configurations"]["dataSampleRatio"] = float(dataset_size)

        # 执行任务
        print(
            f"start experiment_name:{experiment_name}, taskId:{task_id}, support:{new_support}, confidence:{new_confidence}, req_json_data:{req_json_data}")
        start_time = time.time()
        response_json = send_http_to_rock(url, req_json_data)
        cost_time = time.time() - start_time
        # response_json = "{}"
        return response_json, cost_time
    except Exception as e:
        time.sleep(30)
        retries -= 1
        if retries > 0:
            print(f"Exception occurred:{e}. Retrying...({retries} retries left)")
            return execute_task(task_id, row, url, experiment_df, experiment_name, retries)
        else:
            print(f"Task failed after 3 retries")
            raise Exception(e)


def concurrent_execute_task(row, url, experiment_df, experiment_name):
    with semaphore:
        task_id = generate_task_id()
        print(f"任务{task_id} 获取信号量")
        try:
            task_resp, cost_time = execute_task(task_id, row, url, experiment_df, experiment_name)
            print(
                f"finish experiment_name:{experiment_name}, taskId:{task_id}, support:{row[COLUMN_NEW_SUPPORT]}, confidence:{row[COLUMN_NEW_CONFIDENCE]}, costTime:{cost_time}, response:{task_resp}")
        except Exception as e:
            print(f" execute task {task_id} failed, exception:{e}")
        print(f"任务{task_id} 释放信号量")


def execute_experiment(url, experiment_df, experiment_name):
    row_num = len(experiment_df)
    threads = []
    for i in range(row_num):
        row = experiment_df.iloc[i]
        thread = threading.Thread(target=concurrent_execute_task, args=(row, url, experiment_df, experiment_name))
        threads.append(thread)
        thread.start()
        # concurrent_execute_task(row, url, experiment_df, experiment_name)
        # try:
        #     task_id, task_resp, cost_time = execute_task(row, url, experiment_df, experiment_name)
        #     print(
        #         f"finish experiment_name:{experiment_name}, taskId:{task_id}, costTime:{cost_time}, response:{task_resp}")
        # except Exception as e:
        #     print(f"[execute_experiment] execute task failed, exception:{e}")
        #     break

    # 等待所有线程完成
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    excel_file = 'auto_test_0118.xlsx'
    xls = pd.ExcelFile(excel_file)

    # 组装URL
    url = "http://127.0.0.1:19500/api/v1/mls/rulediscover/execute"
    print(f"URL:{url}")

    # 遍历每个工作表并处理
    for sheet_name in xls.sheet_names:
        print(f"execute experiment:{sheet_name}")
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=0, index_col=0)
        execute_experiment(url, df, sheet_name)
