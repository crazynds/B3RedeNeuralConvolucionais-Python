import pandas as pd
from sqlalchemy import create_engine
import gc
from dotenv import load_dotenv
import os

load_dotenv()


engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
    .format(user=os.environ['DB_USER'],
            host=os.environ['DB_HOST'],
            pw=os.environ['DB_PASSWORD'],
            db=os.environ['DB_DATABASE']))

arquivos_bovespa = [
    './dados/COTAHIST_A2022.TXT',
    './dados/COTAHIST_A2021.TXT',
    './dados/COTAHIST_A2020.TXT',
    './dados/COTAHIST_A2019.TXT',
    './dados/COTAHIST_A2018.TXT',
    './dados/COTAHIST_A2017.TXT',
    './dados/COTAHIST_A2016.TXT',
]



tamanho_campos = [2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]
columns = [
    "tipo_registro",
    "data_pregao",
    "cod_bdi",
    "cod_negociacao",
    "tipo_mercado",
    "nome_empresa",
    "especificacao_papel",
    "prazo_dias_merc_termo",
    "moeda_referencia",
    "preco_abertura",
    "preco_maximo",
    "preco_minimo",
    "preco_medio",
    "preco_ultimo_negocio",
    "preco_melhor_oferta_compra",
    "preco_melhor_oferta_venda",
    "numero_negocios",
    "quantidade_papeis_negociados",
    "volume_total_negociado",
    "preco_exercicio",
    "indicador_correcao_precos",
    "data_vencimento",
    "fator_cotacao",
    "preco_exercicio_pontos",
    "codigo_isin",
    "num_distribuicao_papel",
]
databaseColumns = [
    "trading_date",
    "bdi_code",
    "stock_name",
    "market_type",
    "price_open",
    "price_max",
    "price_min",
    "price_avg",
    "price_last_deal",
    "price_best_buy_offer",
    "price_best_sell_offer",
    "quantity_papers_negotiated",
    "number_trades"
]
listaNumber = [
    "preco_abertura",
    "preco_maximo",
    "preco_minimo",
    "preco_medio",
    "preco_ultimo_negocio",
    "preco_melhor_oferta_compra",
    "preco_melhor_oferta_venda",
]
listaInt = [
    "quantidade_papeis_negociados",
    "numero_negocios",
    "cod_bdi",
    "tipo_mercado",
]


for arquivo_bovespa in arquivos_bovespa:
    
    # Le o arquivos em capos pre definidos
    print("========================================================")
    print("Começando a ler os dados do arquivo "+arquivo_bovespa)
    dados_acoes = pd.read_fwf(arquivo_bovespa,widths=tamanho_campos)
    dados_acoes.columns = columns


    print("Lido "+str(len(dados_acoes.index))+" linhas de dados")
    # Filtra registros que não sejam do tipo 1
    dados_acoes = dados_acoes[dados_acoes["tipo_registro"]==1]
    dados_acoes = dados_acoes[(dados_acoes["preco_melhor_oferta_compra"]!=0) & (dados_acoes["preco_melhor_oferta_venda"]!=0)]

    print("Separado "+str(len(dados_acoes.index))+" linhas de dados válidos")
    '''
        Deleta coluna não necessárias
    '''
    dados_acoes.drop('num_distribuicao_papel',inplace=True,axis=1)
    dados_acoes.drop('preco_exercicio_pontos',inplace=True,axis=1)
    dados_acoes.drop('volume_total_negociado',inplace=True,axis=1)
    dados_acoes.drop('fator_cotacao',inplace=True,axis=1)
    dados_acoes.drop('data_vencimento',inplace=True,axis=1)
    dados_acoes.drop('indicador_correcao_precos',inplace=True,axis=1)
    dados_acoes.drop('preco_exercicio',inplace=True,axis=1)
    dados_acoes.drop('tipo_registro',inplace=True,axis=1)
    dados_acoes.drop('prazo_dias_merc_termo',inplace=True,axis=1)
    dados_acoes.drop('moeda_referencia',inplace=True,axis=1)
    dados_acoes.drop('codigo_isin',inplace=True,axis=1)
    dados_acoes.drop('nome_empresa',inplace=True,axis=1)
    dados_acoes.drop('especificacao_papel',inplace=True,axis=1)

    # Formata as colunas necessárias
    for column in listaNumber:
        dados_acoes[column]=[i/100. for i in dados_acoes[column]]
    for column in listaInt:
        dados_acoes[column]=[int(i) for i in dados_acoes[column]]

    # Atualiza o nome das colunas de acordo com o da database
    dados_acoes.columns = databaseColumns

    
    print("Enviando dados para o banco...")
    # Salva no banco
    dados_acoes.to_sql('acao_historico',con=engine,if_exists ='append',chunksize =10000,index=False)
    print("Dados enviados!")
    
    del dados_acoes
    gc.collect()

