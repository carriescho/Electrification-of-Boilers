#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np


#read in data
tier1_df = pd.read_csv('Tier1_CO2_CS.csv',dtype=str)

part75_df = pd.read_csv('Part75_CO2.csv', dtype=str)

cap_df = pd.read_csv('capacity.csv',dtype=str)

naics_county_df = pd.read_csv('naics.csv',dtype=str)



#combine tier1/part75 data with capacity data by matching on facility ID, year, and unit name 
m=tier1_df.merge(cap_df,how="inner",on=['FACILITY_ID','REPORTING_YEAR','UNIT_NAME'])

m_75_cap=part75_df.merge(cap_df,how="inner",on=['FACILITY_ID','REPORTING_YEAR','UNIT_NAME'])

#combine merged data with naics data by matching on facility ID
tier1_all = m.merge(naics_county_df,how="inner",on=['FACILITY_ID'])

part75_all = m_75_cap.merge(naics_county_df,how="inner",on=['FACILITY_ID'])

#save to file 
tier1_all.to_csv(r'C:\Users\Carrie Schoeneberger\Box\Boiler electrification potential\Tier1_CO2_emissions.csv', index = False)

part75_all.to_csv(r'C:\Users\Carrie Schoeneberger\Box\Boiler electrification potential\Part75_CO2_emissions.csv', index = False)


