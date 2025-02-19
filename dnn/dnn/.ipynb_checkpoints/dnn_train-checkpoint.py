
from config import config
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter
from dataset.dnn_dataset import MyDataset, MyPriorDataset
from getdata.get_data import get_data, my_collate_fn
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
train_feature_numpy,test_feature_numpy,train_label,test_label = get_data()

dataset_train = MyDataset(train_feature_numpy, train_label, config)
dataloader_train = DataLoader(dataset_train, 
                              batch_size=config['batch_size'], 
                              shuffle=False, 
                              collate_fn=my_collate_fn)     #  features shape:  batch_size, feature_num, padding_length

dataset_test = MyDataset(test_feature_numpy, test_label, config)
dataloader_test = DataLoader(dataset_test, 
                             batch_size=config['batch_size'], 
                             shuffle=False, 
                             collate_fn=my_collate_fn)

model = MyModel(config)
criterion = nn.BCEWithLogitsLoss()
optimizer = torch.optim.Adam(model.parameters(), config['lr'])
writer = SummaryWriter()


def train_model(train_loader, test_loader, model, criterion, optimizer, num_epochs=3):
    total_step = 0
    for epoch in range(num_epochs):
        model.train()
        for features, labels in train_loader:
            
            optimizer.zero_grad()
            outputs = model(features)
            labels = torch.unsqueeze(labels, dim=1)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            total_step += 1
            
            if (total_step + 1) %10 == 0:
                writer.add_scalar('Train Loss', loss.item(), total_step)
            if (total_step + 1) %100 == 0:
                print(f'Epoch {epoch} Total Step {total_step}: Loss={loss.item(): .4f}')
            if (total_step+1) % 2000 == 0:
                with torch.no_grad():
                    model.eval()
                    test_preds = []
                    test_targets = []
                    for data, target in test_loader:
                        output  = model(data)
                        test_preds.extend(output.to('cpu').sigmoid().squeeze().tolist())
                        test_targets.extend(target.squeeze().tolist())
                    test_auc = roc_auc_score(test_targets, test_preds)
                    writer.add_scalar('AUC/train', test_auc, total_step)
                torch.save(model.state_dict(), f'models/dnn/model_epoch_{epoch}_{total_step}.pth')
                model.train()
        torch.save(model.state_dict(), f'models/dnn/model_epoch_{epoch}.pth')    
    
    with torch.no_grad():
        model.eval()
        test_preds = []
        test_targets = []
        for data, target in test_loader:
            output = model(data)
            test_preds.extend(output.to('cpu').sigmoid().squeeze().tolist())
            test_targets.extend(target.squeeze().tolist())
        test_auc = roc_auc_score(test_targets, test_preds)
        print(test_auc)
        writer.add_scalar('AUC/Train', test_auc, total_step)

train_model(dataloader_train, dataloader_test, model, criterion, optimizer)
writer.close()