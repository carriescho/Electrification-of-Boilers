#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import pyarrow
import tarfile

with tarfile.open('mfg_eu_temps_20191003.tar.gz', mode='r:gz') as tf:
    tf.extractall('c:/users/carrie schoeneberger/Gitlocal')
    mfg_eu_temps = pd.read_parquet('c:/users/carrie schoeneberger/Gitlocal/mfg_eu_temps_20191003/', engine='pyarrow') 


# In[ ]:


wh_frac = pd.read_csv(r'C:\Users\Carrie Schoeneberger\Downloads\WHP fraction updated.csv')

cop = pd.read_csv(r'C:\Users\Carrie Schoeneberger\Downloads\COP updated.csv')


#create dictionaries that match naics code to WHP fraction and to COP
wh_frac = wh_frac.loc[:,('naics', 'end use','Overall_fraction')]
wh_frac['naics'] = wh_frac['naics'].astype(str)

wh_frac_bchp_dict = wh_frac[wh_frac['end use']=="Boiler/CHP"].set_index('naics')['Overall_fraction'].to_dict()
wh_frac_ph_dict = wh_frac[wh_frac['end use']=="Process heating"].set_index('naics')['Overall_fraction'].to_dict()



cop = cop.loc[:,('naics', 'end use','COP')]
cop['naics'] = cop['naics'].astype(str)

cop_bchp_dict = cop[cop['end use']=="Boiler/CHP"].set_index('naics')['COP'].to_dict()
cop_ph_dict = cop[cop['end use']=="Process heating"].set_index('naics')['COP'].to_dict()


# In[ ]:


#filter for the boiler and chp end use
boiler_chp = mfg_eu_temps.loc[(mfg_eu_temps['End_use'] =='Conventional Boiler Use') | 
                      (mfg_eu_temps['End_use']=='CHP and/or Cogeneration Process')].copy()


#filter for the process heating end use
ph = mfg_eu_temps.loc[(mfg_eu_temps['End_use'] =='Process Heating')].copy()



boiler_chp_group = boiler_chp.groupby(['MECS_FT','COUNTY_FIPS','naics','End_use']).sum().reset_index()
ph_group = ph.groupby(['MECS_FT','COUNTY_FIPS','naics','End_use']).sum().reset_index()

boiler_chp_group.loc[:,'naics_sub'] = boiler_chp_group['naics'].astype(str).str[:3]
boiler_chp_group.loc[:,'naics'] = boiler_chp_group['naics'].astype(str)

ph_group.loc[:,'naics_sub'] = ph_group['naics'].astype(str).str[:3]
ph_group.loc[:,'naics'] = ph_group['naics'].astype(str)


# In[ ]:


#calculate technical potential
boiler_chp_group.loc[:,'tech_pot_MMBtu'] = boiler_chp_group.apply(
    lambda x: x['MMBtu']*(wh_frac_bchp_dict.get(x['naics_sub'],0)*cop_bchp_dict.get(x['naics_sub'],0) +\
                        wh_frac_bchp_dict.get(x['naics'],0)*cop_bchp_dict.get(x['naics'],0)),axis=1)


ph_group.loc[:,'tech_pot_MMBtu'] = ph_group.apply(
    lambda x: x['MMBtu']*(wh_frac_ph_dict.get(x['naics_sub'],0)*cop_ph_dict.get(x['naics_sub'],0) +\
                        wh_frac_ph_dict.get(x['naics'],0)*cop_ph_dict.get(x['naics'],0)),axis=1)


# In[ ]:


#combine into one file and drop rows where technical potential is = 0
whr_tech_pot_MMBtu = pd.concat([boiler_chp_group, ph_group], ignore_index=True).drop(
    whr_tech_pot_MMBtu[whr_tech_pot_MMBtu['tech_pot_MMBtu']==0 ].index) 


whr_tech_pot_MMBtu.to_csv('whr_tech_pot_MMBtu.csv')

