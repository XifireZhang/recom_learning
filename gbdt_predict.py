import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import numpy as np
import os
from odps import ODPS
from odps.df import DataFrame


def batch_predict_lightgbm_and_eval(
        model_file,
        brand_id,
        output_file,
        label_col='label',
        topK_list=[1000, 3000, 5000, 10000, 50000],
        
    ):
    """
    使用给定的 LightGBM 模型，对大规模测试集分批做预测，并在全部数据上统计
    topK 范围内的召回率（即前 K 条中正例数占总正例数的比例）。
    
    参数：
    --------
    model_file : str
        已训练好的 LightGBM 模型文件路径（.txt 或 .bin 等）

    label_col  : str
        标签列的名称，值为 0/1

    topK_list  : list of int
        希望评估的若干个 topK 取值
    """
    
    # 1. 加载 LightGBM 模型
    model = lgb.Booster(model_file=model_file)
    
    # 2. 分批读取测试数据，并逐批预测
    #    注意：如果有非特征列（比如 user_id 等），可在这里或外部进行过滤或 drop
    predictions_list = []  # 用来收集每批的预测结果
    
    for num in range(10):
        sql = '''
        SELECT  
            *
        FROM
            recom_learning.user_pay_sample_feature_join_eval
        WHERE ds= '20130923'  and brand_id='{brand}' and rnd > {start} and rnd <= {end}

        ;
        '''.format(brand=brand_id, start=num/10.0, end=(num + 1)/10.0)
        print(sql)
        
        query_job = o.execute_sql(sql)
        result = query_job.open_reader(tunnel=True)
        df = result.to_pandas(n_process=4) 
        chunk_df = df.drop(columns=['user_id', 'brand_id', 'bizdate', 'ds', 'rnd'])
        
        y_chunk = chunk_df[label_col].values
        X_chunk = chunk_df.drop(columns=label_col)
        
        # 对该批次进行预测
        pred_probs = model.predict(X_chunk)
        
        # 将结果保存到一个小 DataFrame，以便后续拼接
        chunk_result = pd.DataFrame({
            label_col: y_chunk,
            'pred_prob': pred_probs
        })
        predictions_list.append(chunk_result)
    
    # 3. 将全部批次的预测结果合并
    #    如果数据非常大，也可以考虑将每批结果写入一个临时文件，再用外部排序工具处理
    predictions_df = pd.concat(predictions_list, ignore_index=True)
    
    # 4. 按预测概率从高到低排序
    predictions_df.sort_values(by='pred_prob', ascending=False, inplace=True)
    
    # 5. 计算测试集中所有正例的总数
    total_positives = predictions_df[label_col].sum()
    if total_positives == 0:
        print("测试集里没有 label=1 的样本，无法计算召回率。")
        return
    
    # 6. 针对每个 topK，计算召回率
    results = []
    for k in topK_list:
        # 如果数据总量小于 k，就用数据总长度
        actual_k = min(k, len(predictions_df))
        
        topk_df = predictions_df.iloc[:actual_k]
        topk_positives = topk_df[label_col].sum()
        
        recall_k = topk_positives / total_positives
        results.append((k, recall_k))
        print(f"Top {k} 的召回率: {recall_k:.4f} "
              f"(占全部正例的 {recall_k * 100:.2f}%)")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("TopK\tRecall\n")  # 写表头
        for k, recall_k in results:
            f.write(f"{k}\t{recall_k:.6f}\n")

    print(f"\n以上结果已经写入文件: {output_file}")

# -------------------------
# 使用示例
# -------------------------
if __name__ == "__main__":
    
     # 建立链接。
    o = ODPS(
        'LTAI5tDADMFPw1WQaXoFmN7v',
        'password',
        project='recom_learning',
        endpoint='https://service.cn-beijing.maxcompute.aliyun.com/api',
    )
    
    #  'b47686'  韩都衣舍 70；   --b56508 三星手机 35 --b62063 诺基亚 20 --b78739 LILY  20
    brand_id = 'b47686'
    brand_id_2 = 'b78739'
    tree_num = '70'
    
    # 假设你的 LightGBM 模型文件是 'lightgbm_model.txt'
    model_path = f'./models_{brand_id}_{brand_id_2}/lightgbm_{tree_num}.txt'
    output_file_path = f'./models_{brand_id_2}/topK_recall_results_{brand_id}_{brand_id_2}.txt'
    
    batch_predict_lightgbm_and_eval(
        model_file=model_path,
        brand_id=brand_id_2,
        output_file = output_file_path,
        label_col='label',
        topK_list=[1000, 3000, 5000, 10000, 50000 ]
    )
