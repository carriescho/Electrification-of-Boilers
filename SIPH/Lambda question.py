#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import pyarrow
import tarfile
from matplotlib import pyplot as plt

with tarfile.open('mfg_eu_temps_20191003.tar.gz', mode='r:gz') as tf:
    tf.extractall('C:Users/zhang/Box/Python/SIPH')
    mfg_eu_temps = pd.read_parquet('C:Users/zhang/Box/Python/SIPH/mfg_eu_temps_20191003/', engine='pyarrow') 


# In[2]:


fraction=pd.read_csv('WHP fraction.csv')


# In[3]:


whp_frac = fraction.loc[:,('naics','end use','Overall_fraction')]


# In[4]:


COP=pd.read_csv('COP.csv')


# In[5]:


COP_v = COP.loc[:,('naics','end use','COP')]


# In[13]:


#fraction with different end uses
whp_frac_bchp=whp_frac[whp_frac['end use'] == "Boiler/CHP"].set_index('naics')['Overall_fraction'].to_dict()
whp_frac_ph=whp_frac[whp_frac['end use']=="Process heating"].set_index('naics')['Overall_fraction'].to_dict()
COP_v_bchp=COP_v[COP_v['end use']=="Boiler/CHP"]
COP_v_ph=COP_v[COP_v['end use']=="Process heating"]
whp_frac_bchp


# In[7]:


#filter for different end uses
boiler_chp=mfg_eu_temps.loc[(mfg_eu_temps['End_use']=='Conventional Boiler Use')|(mfg_eu_temps['End_use']=="CHP and/or Cogeneration Process")].copy()
ph=mfg_eu_temps.loc[(mfg_eu_temps['End_use']=='Process Heating')].copy()  


# In[14]:


#filter useful information
boiler_chp_f=boiler_chp.loc[:,('COUNTY_FIPS','MECS_FT','naics','End_use','MMBtu')]
ph_f=ph.loc[:,('COUNTY_FIPS','MECS_FT','naics','End_use','MMBtu')]
boiler_chp_f


# In[9]:


#group data by county, fuel types, and naics code
boiler_chp_group = boiler_chp_f.groupby(['MECS_FT','COUNTY_FIPS','naics']).sum().reset_index()
ph_group=ph_f.groupby(['MECS_FT','COUNTY_FIPS','naics']).sum().reset_index()


# In[15]:


boiler_chp_f.loc[:,'recovered_heat_MMBtu'] = boiler_chp_f.apply(lambda x: x['MMBtu'])*whp_frac_bchp[x['naics']]


# In[ ]:




