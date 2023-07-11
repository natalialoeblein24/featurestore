# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

import urllib.request
from multiprocessing import Pool

from tqdm import tqdm

def get_data_uf_ano_mes(uf, ano, mes):
    url = f"ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/200801_/Dados/RD{uf}{ano}{mes}.dbc"

    file_path = f"/dbfs/mnt/datalake/datasus/rd/dbc/RD{uf}{ano}{mes}.dbc" 

    resp = urllib.request.urlretrieve(url, file_path)

def get_data_uf(uf, datas):
    for i in tqdm(datas):
        ano, mes, dia = i.split("-")
        ano = ano[-2:]
        get_data_uf_ano_mes(uf, ano, mes)

ufs = ["RO", "AC", "AM", "RR","PA",
       "AP", "TO", "MA", "PI", "CE",
       "RN", "PB", "PE", "AL", "SE",
       "BA", "MG", "ES", "RJ", "SP",
       "PR", "SC", "RS", "MS", "MT",
       "GO","DF"]

datas = ['2023-01-01', '2023-02-01']

to_download = [(uf, datas) for uf in ufs]

# COMMAND ----------

with Pool(8) as pool:
    pool.starmap(get_data_uf, to_download)
