
from config import config
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter
from dataset.dnn_dataset import MyDataset, MyPriorDataset
from getdata.get_data import get_data, my_collate_fn, get_data_test, calculate_top_k_ratio
from sklearn.metrics import roc_auc_score



class MyModel(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.embedding = nn.Embedding(num_embeddings=config['num_embedding'], embedding_dim=config['embedding_dim'], padding_idx=0)   # 20000, 32    32因为补齐中有0，不应当参与梯度的计算
        self.fc1 = nn.Linear(config["embedding_dim"] * len(config['feature_col']), 512)   # 特征数量： 32 * 14
        self.fc2 = nn.Linear(512, 128)
        self.fc3 = nn.Linear(128, 1)
    
    def forward(self, features):
        embedding_dict = {}
        for ff in self.config["feature_col"]:
            embedding_dict[ff] = torch.sum(self.embedding(features[ff]), dim=1)   ## 3d 序列进行求和后 为2d   原先：batch_size , 补齐的序列长度 , emb_dim  之后：batch_size , emb_dim
        x = torch.cat([embedding_dict[ff] for ff in self.config["feature_col"]], dim=1)   # 一个feature是 batch_size , emb_dim ---> batch_size , embdim * len(feature)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x
    
    
# 数据集加载
brands = ['b47686','b56508','b62063','b78739']

for brand_id in brands:
    #brand_id='b56508'
    model_path = './models/dnn_focal/model_epoch_1_31999.pth'
    # 需要计算top数量
    top_k_list = [1000, 3000, 5000, 10000, 50000]

    test_feature_numpy, test_label = get_data_test(brand_id)
    dataset_test = MyPriorDataset(test_feature_numpy, test_label, config)

    dataloader_test = DataLoader(dataset_test, batch_size=config["batch_size"], shuffle=False, collate_fn=my_collate_fn)

    model = MyModel(config).to('cpu')
    model.load_state_dict(torch.load(model_path))
    model.to('cpu')
    model.eval()
    test_preds = []
    test_targets = []
    for data, target in dataloader_test:
        output = model(data)
        test_preds.extend(output.sigmoid().squeeze().tolist())
        test_targets.extend(target.squeeze().tolist())

    # 计算top k的正例比例
    ratios = calculate_top_k_ratio(test_preds, test_targets, top_k_list)
    # 输出结果
    for k, ratio in ratios.items():
        print(f"Top {k} ratio of positive labels: {ratio:.4f}")
    # 如果需要保存结果到文件
    with open(f'./dnn/models_{brand_id}_top_results.txt', 'w') as f:
        for k, ratio in ratios.items():
            f.write(f"Top {k} ratio of positive labels: {ratio:.4f}\n")