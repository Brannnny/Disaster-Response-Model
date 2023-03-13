import torch
from torch.utils.data import DataLoader, Dataset
import glob, cv2
from glob import glob

device = 'cuda' if torch.cuda.is_available() else 'cpu'


class Data(Dataset):
    def __init__(self, folder):
        """

        :param folder:
        """
        class1 = glob(folder+'/class1/*.jpg')
        class2 = glob(folder+'/class2/*.jpg')
        self.fpaths = class1 + class2

        from random import shuffle, seed
        seed(10)
        shuffle(self.fpaths)
        self.targets = [fpath.split('/')[-1].startswith('class1') \
                        for fpath in self.fpaths]

    def __len__(self):
        """

        :return:
        """
        return len(self.fpaths)

    def __getitem__(self, item):
        """

        :param item:
        :return:
        """
        f = self.fpaths[item]
        target = self.targets[item]
        im = (cv2.imread(f)[:,:,::-1])
        im = cv2.resize(im, (224, 224))
        return torch.tensor(im/255).permute(2, 0, 1).to(device).float(),\
                torch.tensor([target]).float().to(device)


def get_data(train_data_dir, test_data_dir, batch_size=32):
    """

    :param train_data_dir:
    :param test_data_dir:
    :param batch_size:
    :return:
    """
    train = data(train_data_dir)
    trn_dl = DataLoader(train, batch_size=batch_size, shuffle=True, drop_last=True)
    val = data(test_data_dir)
    val_dl = DataLoader(val, batch_size=batch_size, shuffle=True, drop_last=True)
    return trn_dl, val_dl
