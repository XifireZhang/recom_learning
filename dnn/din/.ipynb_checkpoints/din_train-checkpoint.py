
from config import config
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader
from torch.utils.tensorboard import SummaryWriter
from dataset.dnn_dataset import MyDataset, MyPriorDataset
from getdata.get_data import get_data, my_collate_fn, calculate_top_k_ratio, seq_collate_fn, get_data_test
from sklearn.metrics import roc_auc_score
from loss.focal_loss import FocalLoss


class LocalActivationUnit(nn.Module):
    def __init__(self, hidden_units):
        super().__init__()
        self.fc1 = nn.Linear(hidden_units * 4, hidden_units)
        self.fc2 = nn.Linear(hidden_units, 1)

    def forward(self, user_behaviors, target_item, mask):
        # user_behaviors shape: (batch_size, seq_len, hidden_units)
        # target_item shape: (batch_size, hidden_units)
        # mask shape: (batch_size, seq_len)
        
        seq_len = user_behaviors.size(1)
        target_item = target_item.unsqueeze(1).expand(-1, seq_len, -1)
        
        # Concatenate user behavior embeddings with target item embeddings
        interactions = torch.cat([user_behaviors, target_item, user_behaviors - target_item, user_behaviors * target_item], dim=-1)
        
        # Forward through two dense layers with activation
        x = torch.relu(self.fc1(interactions))   # batch_size, seq_len, hidden_units
        attention_logits = self.fc2(x).squeeze(-1)   
        
        # Apply mask to remove padding influence
        attention_logits = attention_logits.masked_fill(mask == 0, float('-inf'))
        
        # Apply softmax to compute attention weights
        attention_weights = F.softmax(attention_logits, dim=1).unsqueeze(-1)
        
        # Compute weighted sum of user behavior embeddings to get user interests
        user_interests = torch.sum(attention_weights * user_behaviors, dim=1)
        return user_interests

    
class Din(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.embedding = nn.Embedding(num_embeddings=self.config["num_embedding"], embedding_dim =self.config["embedding_dim"], padding_idx=0)
        self.fc1 = nn.Linear(self.config["embedding_dim"]*len(self.config["feature_col"]), 512)
        self.fc2 = nn.Linear(512,128)
        self.fc3 = nn.Linear(128,1)
        self.att = LocalActivationUnit(self.config["embedding_dim"])
        
    def forward(self, features, mask):
        embedding_dict = {}
        for ff in self.config["feature_col"]:
            if ff != 'pay_brand_seq':
                embedding_dict[ff] = torch.sum(self.embedding(features[ff]), dim=1)
        att_emb = self.att(self.embedding(features['pay_brand_seq']), embedding_dict['target_brand_id'], mask['pay_brand_seq'])
        x = torch.cat([embedding_dict[ff] for ff in self.config["feature_col"] if ff != 'pay_brand_seq'], dim=1)
        x = torch.cat([x, att_emb], dim=1)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x


train_feature_numpy,test_feature_numpy,train_label,test_label = get_data()
dataset_train = MyPriorDataset(train_feature_numpy, train_label,config)
print('train dataset finish')
dataloader_train = DataLoader(dataset_train, batch_size=config["batch_size"], shuffle=False, collate_fn=seq_collate_fn)
print('train dataloader finish')

brands = ['b47686','b56508','b62063','b78739']
dataset_test_dict = {}
dataloader_test_dict = {}
for brand_id in brands: 
    test_feature_numpy,test_label = get_data_test(brand_id)
    dataset_test_dict[brand_id] = MyPriorDataset(test_feature_numpy, test_label, config)
    dataloader_test_dict[brand_id] = DataLoader(dataset_test_dict[brand_id], batch_size=2048, shuffle=False, collate_fn=seq_collate_fn)
print('test data finish')

device = torch.device( "cpu")
model = Din(config).to(device) 
criterion = nn.BCEWithLogitsLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.004)
log_dir = './runs_din'
writer = SummaryWriter(log_dir)


def train_model(train_loader, test_loader_dict, model, criterion, optimizer, num_epochs=3):
    total_step = 0
    for epoch in range(num_epochs):
        model.train()
        for features, mask, labels in train_loader:
            for ff in config["feature_col"]:
                features[ff] = features[ff].to(device)
                mask[ff] = mask[ff].to(device)
            labels = labels.to(device) 
            optimizer.zero_grad()
            outputs = model(features,mask)
            labels = torch.unsqueeze(labels,dim=1)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            total_step += 1
            if (total_step+1)%10 == 0:
                writer.add_scalar('Training Loss', loss.item(), total_step)
            if (total_step+1)%100 == 0:
                print(f'Epoch {epoch}, Step {total_step}: Loss={loss.item(): .4f}')
            if (total_step+1)%7000 == 0:
                with torch.no_grad():
                    model.eval()
                    for brand_id in brands:
                        #brand_id='b56508'
                        # 需要计算top数量
                        top_k_list = [1000, 3000, 5000, 10000, 50000]
                        test_preds = []
                        test_targets = []
                        for data,mask, target in test_loader_dict[brand_id]:
                            output  = model(data,mask)
                            test_preds.extend(output.sigmoid().squeeze().tolist())
                            test_targets.extend(target.squeeze().tolist())

                        # 计算top k的正例比例
                        ratios = calculate_top_k_ratio(test_preds, test_targets, top_k_list)
                        # 输出结果
                        for k, ratio in ratios.items():
                            print(f"{brand_id} Top {k} ratio of positive labels: {ratio:.4f}")
                        # 如果需要保存结果到文件
                        with open(f'{log_dir}/models_{brand_id}_top_results_din_{total_step}.txt', 'w') as f:
                            for k, ratio in ratios.items():
                                f.write(f"Top {k} ratio of positive labels: {ratio:.4f}\n")
                torch.save(model.state_dict(), f'models/din/model_epoch_{epoch}_{total_step}.pth')
                model.train()
        torch.save(model.state_dict(), f'models/din/model_epoch_{epoch}.pth')
    with torch.no_grad():
        model.eval()
        for brand_id in brands:
            #brand_id='b56508'
            # 需要计算top数量
            top_k_list = [1000, 3000, 5000, 10000, 50000]
            test_preds = []
            test_targets = []
            for data,mask, target in test_loader_dict[brand_id]:
                output  = model(data,mask)
                test_preds.extend(output.sigmoid().squeeze().tolist())
                test_targets.extend(target.squeeze().tolist())

            # 计算top k的正例比例
            ratios = calculate_top_k_ratio(test_preds, test_targets, top_k_list)
            # 输出结果
            for k, ratio in ratios.items():
                print(f"{brand_id} Top {k} ratio of positive labels: {ratio:.4f}")
            # 如果需要保存结果到文件
            with open(f'{log_dir}/models_{brand_id}_top_results_din_{total_step}.txt', 'w') as f:
                for k, ratio in ratios.items():
                    f.write(f"Top {k} ratio of positive labels: {ratio:.4f}\n")
        
train_model(dataloader_train, dataloader_test_dict, model, criterion, optimizer)
writer.close()