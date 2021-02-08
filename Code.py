#!/usr/bin/env python
# coding: utf-8

# In[ ]:


### CASE ###
#Processo de extração de dados de logs de servidores e sistemas
import numpy as np
import pandas as pd
import glob
import os
from functools import reduce
from datetime import datetime
import requests
import json
import urllib3
from urllib3 import request
import certifi
from pandas.io.json import json_normalize


# In[ ]:


#LISTA DE USUARIOS
list_of_files = glob.glob('C:\\Users\\ps014986\\Documents\\Case Ze Delivery\\*Usuarios*')
usuarios = max(list_of_files, key=os.path.getctime)

log_usuarios = pd.read_csv(usuarios, sep = ';')

df2 = (( log_usuarios['DT_INCLUSAO_REG'].str.contains("28/01/2021"))
        & (log_usuarios.CIDADE_ESTABELECIMENTO.isin(['SAO PAULO','CARUARU'])))

log_usuarios= log_usuarios.loc[df2]

log_usuarios = log_usuarios[["MATR","NOME","CPF"]]
log_usuarios = log_usuarios.astype(str)


# In[ ]:


#########
# ANALISA LOG Sistema 1
################################
list_of_files = glob.glob('C:\\Users\\ps014986\\Documents\\Case Ze Delivery\\Sistema1\\*Sistema1*') # * means all if need specific format then *.csv
usu_sis1 = max(list_of_files, key=os.path.getctime)

usu_sis1 = pd.read_table(usu_sis1,sep=',', encoding='latin-1',header = 1)

usu_sis1 = usu_sis1.rename(columns={"UserName": "MATR","AcctDisabled" : "Sistema1"})
usu_sis1 = usu_sis1[["MATR","Sistema1"]]
usu_sis1 = usu_sis1.astype(str)


# In[ ]:


#########
# ANALISA LOG Sistema 1
################################
list_of_files = glob.glob('C:\\Users\\ps014986\\Documents\\Case Ze Delivery\\Sistema1\\*Sistema1*') # * means all if need specific format then *.csv
usu_sis1 = max(list_of_files, key=os.path.getctime)

usu_sis1 = pd.read_table(usu_sis1,sep=',', encoding='latin-1',header = 1)

usu_sis1 = usu_sis1.rename(columns={"UserName": "MATR","AcctDisabled" : "Sistema1"})
usu_sis1 = usu_sis1[["MATR","Sistema1"]]    
    
usu_sis1 = usu_sis1.astype(str)


# In[ ]:


#########
# ANALISA LOG Sistema 2
################################
list_of_files = glob.glob('C:\\Users\\ps014986\\Documents\\Case Ze Delivery\\Sistema1\\*Sistema2*') # * means all if need specific format then *.csv
usu_sis2 = max(list_of_files, key=os.path.getctime)

usu_sis2 = pd.read_excel(usu_sis2,sheet_name='Relatório de Usuários', usecols = "B,C")
usu_sis2 = usu_sis2.rename(columns={"USERNAME": "MATR","STATUS": "Sistema2"})
usu_sis2 = usu_sis2.astype(str)


# In[ ]:


#########
# Ex via API: ANALISA LOG Sistema 3
################################
api_token = 'Senh@Teste2021'
headers = {'Content-Type': 'application/json',
           'Authorization': 'Bearer {0}'.format(api_token)}

# Lista de usuario
list_usu = usu_Ativos['MATR']
list_usu = list_usu.values.tolist()
MATRs = list_usu

channels_list = []

# para cada usuario, é feita uma consulta via API
for MATR in MATRs:
    JSONContent = requests.get("http://localhost:8091/sistema3/user/"+MATR+"?field=name,agent_key,matriculation,id,user_status,user_ident,user_date_inactive_status)", headers=headers)
    data = json.loads(JSONContent.content.decode('utf-8'))

    if not (data.get('data') is None):
        channels_list.append(data)
        df = pd.json_normalize(channels_list, 'data')
        usu_sistema3 = pd.DataFrame(df)

usu_sistema3 = usu_sistema3.rename(columns={"matriculation": "MATR","user_status": "Sistema3"})        
usu_sistema3 = usu_sistema3[["MATR","Sistema3"]]
usu_sistema3 = usu_sistema3.astype(str)


# In[ ]:


#########
#Correlaciona os dados dos sistemas com os dos Usuarios
################################
df1 = pd.DataFrame(log_usuarios)
df2 = pd.DataFrame(usu_sis1)
df3 = pd.DataFrame(usu_sis2)

#--------------------------------------------------------------------
merge_AdAssai = pd.merge(df1, df2, on="MATR",how ='left')
merge_AdBarc = pd.merge(df1, df3, on="MATR",how ='left')
#--------------------------------------------------------------------
data_frames = [df1,df2,df3]
#data_frames = [df1,df2]
nan_value = 'NULL'
#--------------------------------------------------------------------
df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['MATR'],
                                            how='left'), data_frames).fillna(nan_value)

#-----------------------------------------------------------------------
##Tira matriculas duplicadas Consinco
df_merged = df_merged.drop_duplicates(keep='first')


# In[ ]:


df_merged.loc[df_merged['Sistema1'] == 'No ', 'Sistema1'] = 'ATIVO'
df_merged.loc[df_merged['Sistema1'] == 'NULL', 'Sistema1'] = 'BLOQUEADO'
df_merged.loc[df_merged['Sistema2'] == 'NULL', 'Sistema2'] = 'BLOQUEADO'


# In[ ]:


# Exportando para Excel 
today = datetime.now()
os.chdir("C:/Users/ps014986/Documents/Case Ze Delivery/Sistema1")
datatoexcel = pd.ExcelWriter(today.strftime('%Y%m%d') + '_Logs_Acessos.xlsx') 
  
df_merged.to_excel(datatoexcel) 

datatoexcel.save() 
print('Exportação para Excel concluída.')


# In[45]:


df_merged.head(10)


# In[ ]:




