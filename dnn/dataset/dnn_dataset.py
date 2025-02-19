import torch
from torch.utils.data import Dataset


# dataset必须实现__getitem__ __len__两种方法
class MyDataset(Dataset):
    def __init__(self, features, labels, config):
        self.features = {}
        self.config = config
        for ff in self.config['feature_col']:
            self.features[ff] = [torch.tensor([int(id) for id in seq.split(',')], dtype=torch.long) for seq in features[ff]]

        self.labels = torch.tensor(labels, dtype=torch.float32)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        res_features = {}
        for ff in self.config['feature_col']:
            res_features[ff] = self.features[ff][idx]
        res_features['labels'] = self.labels[idx]
        return res_features

    
class MyPriorDataset(Dataset):
    def __init__(self, features, labels, config):
        self.config = config
        self.features = features
        self.labels = labels
        
    def __len__(self):
        return len(self.labels)
        
    def __getitem__(self, idx):
        res_features = {}
        for ff in self.config["feature_col"]:
            res_features[ff] = torch.tensor([int(id) for id in self.features[ff][idx].split(',')], dtype=torch.long)
        res_features['labels'] = torch.tensor(self.labels[idx], dtype=torch.float32)
        return res_features