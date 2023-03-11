import torch
import torch.nn as nn

device = 'cuda' if torch.cuda.is_available() else 'cpu'


class Model(nn.Module):

  def conv_layer(self, ni, no, kernel_size, stride):
    """

    :param ni:
    :param no:
    :param kernel_size:
    :param stride:
    :return:
    """
    return nn.Sequential(
      nn.Conv2d(ni, no, kernel_size, stride),
      nn.ReLU(),
      nn.BatchNorm2d(no),
      nn.MaxPool2d(2)
    )

  def get_model(self):
    """

    :return:
    """
    model = nn.Sequential(
      self.conv_layer(3, 64, 3),
      self.conv_layer(64, 512, 3),
      self.conv_layer(512, 512, 3),
      self.conv_layer(512, 512, 3),
      self.conv_layer(512, 512, 3),
      self.conv_layer(512, 512, 3),
      nn.Flatten(),
      nn.Linear(512, 1),
      nn.Sigmoid(),
    ).to(device)
    loss_fn = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)
    return model, loss_fn, optimizer


def train_batch(x, y, model, opt, loss_fn):
  """

  :param x:
  :param y:
  :param model:
  :param opt:
  :param loss_fn:
  :return:
  """
  model.train()
  prediction = model(x)
  batch_loss = loss_fn(prediction, y)
  batch_loss.backward()
  opt.step()
  opt.zero_grad()
  return batch_loss.item()
