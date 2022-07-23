from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import os

import torch
import torch.nn as nn
import torch.functional as F 
import torch.optim as optim


load_dotenv()

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
    .format(user=os.environ['DB_USER'],
            host=os.environ['DB_HOST'],
            pw=os.environ['DB_PASSWORD'],
            db=os.environ['DB_DATABASE']))
# Primeiro carregar os dados relativos as entradas que vamos treinar a ia

# Nesse software foi escolhido para treinar a ia usando ações do setor elétrico



acoes = [
    "AESB3",
    "ALUP11",
    "COCE5",
    "CMIG4",
    "CPLE6",
    "CPFE3",
    "ELET3",
    "ENBR3",
    "ENGI11",
    "ENEV3",
    "EGIE3",
    "EQTL3",
    "LIGT3",
    "NEOE3",
    "MEGA3",
    "TAEE11",
    "TRPL4",
]

dados_acoes = pd.read_sql('SELECT * FROM acao_historico_inputs WHERE stock_name IN (\''+'\',\''.join(acoes)+'\')',con=engine)

dados_acoes.drop('id',inplace=True,axis=1)
dados_acoes.drop('stock_name',inplace=True,axis=1)
dados_acoes.drop('trading_date',inplace=True,axis=1)
dados_acoes.drop('period_variation_open',inplace=True,axis=1)
dados_acoes.drop('period_variation_close',inplace=True,axis=1)

Y = dados_acoes['period_variation_avg'].tolist()
X = dados_acoes.drop('period_variation_avg',axis=1).values.tolist()


'''
    0 - se for menor de -3%
    1 - se for menor que -1.5%
    2 - se for menor que -0.5%
    3 - se for menor que 0.5%
    4 - se for menor que 1.5%
    5 - se for menor que 3%
'''

separator = [-3,-1.5,-0.5,0.5,1.5,3,float('inf')]
a = np.array([0,1,2,3,4,5,6])
for idx,y_t in enumerate(Y):
    index = next(x[0] for x in enumerate(separator) if x[1] > y_t)
    Y[idx]= (a==index).astype('float')

for idx,x_t in enumerate(X):
    X[idx] = np.reshape(x_t,(10,5))
X = np.array(X)
Y = np.array(Y)

def split_and_shuffle(X, Y, perc = 0.1):
  ''' Esta função embaralha os pares de entradas
      e saídas desejadas, e separa os dados de
      treinamento e validação
  '''
  # Total de amostras
  tot = len(X)
  # Emabaralhamento dos índices
  indexes = np.arange(tot)
  np.random.shuffle(indexes)
  # Calculo da quantidade de amostras de
  # treinamento
  n = int((1 - perc)*tot)
  Xt = X[indexes[:n]]
  Yt = Y[indexes[:n]]
  Xv = X[indexes[n:]]
  Yv = Y[indexes[n:]]
  return Xt, Yt, Xv, Yv

Xt,Yt,Xv,Yv = split_and_shuffle(X,Y,0.15)

Xt = torch.from_numpy(Xt)
Yt = torch.from_numpy(Yt)
Xv = torch.from_numpy(Xv)
Yv = torch.from_numpy(Yv)

Xt = Xt.unsqueeze(1)
Xv = Xv.unsqueeze(1)

print('Dados de treinamento:')
print('Xt', Xt.size(), 'Yt', Yt.size())
print()
print('Dados de validação:')
print('Xv', Xv.size(), 'Yv', Yv.size())

class ConvNet(nn.Module):
    
    def __init__(self):
        super(ConvNet, self).__init__()
        self.conv1 = nn.Conv2d(1, 2, kernel_size=(3,1),stride=1) 
        
        self.pool1 = nn.MaxPool2d((3,1), stride=1)
        
        self.lin1 = nn.Linear(60, 80)
        self.lin2 = nn.Linear(80, 70)
        self.lin3 = nn.Linear(70, 7)
  
    def forward(self, x):
        x = self.conv1(x)
        x = torch.relu(x)
        x = self.pool1(x)
        x = x.view(-1, 60)
        x = self.lin1(x)
        x = self.lin2(x)
        x = self.lin3(x)
        return x

cnn = ConvNet()
print(cnn)

def evaluate(neural,x, y_hat):
  y = neural(x).argmax(dim=1)
  y_hat = y_hat.argmax(dim=1)
  return 100*float((y == y_hat).sum()) / len(y)

opt = optim.Adam(cnn.parameters(), lr=0.0001)
loss = nn.CrossEntropyLoss()

gpu = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
cnn = cnn.to(gpu)
Xt = Xt.to(gpu, dtype=torch.float)
Yt = Yt.to(gpu, dtype=torch.long)
Xv = Xv.to(gpu, dtype=torch.float)
Yv = Yv.to(gpu, dtype=torch.long)

for j in range(200001):

  # Faremos o treinamento em lotes de
  # tamanho igual a 128 amostras
  for i in range(0,len(Yt),128):

    # Separa o lote de entradas
    x = Xt[i:i+128,:,:,:]

    # Separa o lote de saídas desejadas
    # já transformando de one-hot para
    # índice das colunas.
    y_hat = Yt[i:i+128,:].argmax(dim=1)

    # Zera o gradiente do otimizador
    opt.zero_grad()

    # Calcula a saída da rede neural
    y = cnn(x)

    # Calcula o erro
    e = loss(y, y_hat)

    # Calcula o gradiente usando
    # backpropagation
    e.backward()

    # Realiza um passo de atualização
    # dos parâmetros da rede neural
    # usando o otimizador.
    opt.step()

  # A cada 200 épocas imprimimos o
  # erro do último lote e a acurácia
  # nos dados de treinamento
  if not (j % 200):
    print(float(e), evaluate(cnn,Xt, Yt))