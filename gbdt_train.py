import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import numpy as np
import os
from odps import ODPS
from odps.df import DataFrame


def train_lightgbm_and_save_models(df, output_file,
                                   label_col='label', 
                                   non_feature_cols=['user_id', 'brand_id', 'bizdate', 'rnd', 'ds'],
                                   n_estimators_start=5,
                                   n_estimators_end=100,
                                   n_estimators_step=5,
                                   test_size=0.2,
                                   random_state=42,
                                   ):
    """
    使用 LightGBM 对给定 DataFrame 进行二分类训练，每隔一定棵树保存模型和记录 AUC。
    
    参数：
    ---------
    df : pd.DataFrame
        原始数据，包含特征、标签、以及非特征列
    label_col : str
        标签列的列名
    non_feature_cols : list[str]
        非特征列，比如 user_id、brand_id 等
    n_estimators_start : int
        训练轮数（树数）起始值
    n_estimators_end : int
        训练轮数（树数）结束值
    n_estimators_step : int
        训练轮数每次的步长
    test_size : float
        训练集与测试集划分的比例
    random_state : int
        随机种子
    auc_output_file : str
        存放 AUC 结果的文件名
    """
    
    # 1. 准备特征与标签
    y = df[label_col]
    X = df.drop(non_feature_cols + [label_col], axis=1)
    
    # 2. 切分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=test_size,
        random_state=random_state
    )
    
    # 3. 准备 LightGBM 数据集
    train_data = lgb.Dataset(X_train, label=y_train)
    valid_data = lgb.Dataset(X_test, label=y_test, reference=train_data)
    
    # 打开结果文件准备写入
    with open(output_file, 'w') as f:
        # 写入标题行
        f.write("n_estimators,train_auc,test_auc\n")
        
        # 4. 迭代训练
        for n_est in range(n_estimators_start, n_estimators_end + 1, n_estimators_step):
            
            # 训练参数，可根据需要进行调整
            params = {
                'objective': 'binary',
                'boosting_type': 'gbdt',
                # 同时计算 auc 和 loss
                'metric': ['binary_logloss', 'auc' ],
                'learning_rate': 0.1,
                'num_leaves': 31,          # 可根据数据规模和特征复杂度调参
                'feature_fraction': 0.9,   # 每次迭代使用 90% 的特征
                'bagging_fraction': 0.8,   # 每次迭代使用 80% 的数据进行训练
                'bagging_freq': 5,         # 每 5 次迭代进行一次重采样
            }
            
            # 进行训练
            model = lgb.train(
                params,
                train_data,
                num_boost_round=n_est,     # 注意这里指定的树数量
                valid_sets=[train_data, valid_data],
                valid_names=['train','valid']
            )
            
            # 5. 计算训练集和测试集 AUC
            # -- 这里不再使用 best_iteration，直接使用该 n_est 轮模型
            train_pred = model.predict(X_train)
            test_pred = model.predict(X_test)
            
            train_auc = roc_auc_score(y_train, train_pred)
            test_auc = roc_auc_score(y_test, test_pred)
            
            # 6. 保存当前模型
            model_file_name = f"./models_all/lightgbm_{n_est}.txt"
            model.save_model(model_file_name)
            
            # 7. 将 AUC 写入 txt 文件
            f.write(f"{n_est},{train_auc:.6f},{test_auc:.6f}\n")
            
            print(f"n_estimators={n_est} => train_auc: {train_auc:.6f}, test_auc: {test_auc:.6f}, model saved: {model_file_name}")


if __name__ == "__main__":
    
    # 建立链接。
    o = ODPS(
        'LTAI5tDADMFPw1WQaXoFmN7v',
        'password',
        project='recom_learning',
        endpoint='https://service.cn-beijing.maxcompute.aliyun.com/api',
    )

    # 读取数据。
    #  'b47686'  韩都衣舍；   --b56508 三星手机  --b62063 诺基亚  --b78739 LILY
    brand_id = 'b47686'  
    brand_id_2 = 'b78739'
    sql = '''
    SELECT  
        *
    FROM (
        SELECT *
        FROM  recom_learning.user_pay_sample_feature_join 
        WHERE ds>='20130701' and ds <= '20130916'
) t1 join(
        select 'b47686' as brand_id
        union all
        select 'b56508' as brand_id
        union all
        select 'b62063' as brand_id
        union all
        select 'b78739' as brand_id
) t2 on t1.brand_id = t2.brand_id
GROUP BY t1.brand_id;
    ;
    '''
    
    print(sql)
    
    query_job = o.execute_sql(sql)
    result = query_job.open_reader(tunnel=True)
    df = result.to_pandas(n_process=4) 
    
    output_file_name = f'./models_all/auc_results.txt'
    train_lightgbm_and_save_models(df, output_file=output_file_name)
