#!/usr/bin/env python
# coding: utf-8


#-------------------

# Collect industrial boiler data from GHGRP subpart C (stationary fuel combustion sources)
# (Data is organized by different tiers, representing different methods for estimating emissions from fuel sources)

#-------------------


import pandas as pd



tier_1 = pd.read_excel(r'C:\Users\zhang\Box\Boiler electrification potential\Tier1_final.xlsx')
tier_2 = pd.read_excel(r'C:\Users\zhang\Box\Boiler electrification potential\Tier_2_cleaned.xlsx')
tier_3 = pd.read_excel(r'C:\Users\zhang\Box\Boiler electrification potential\Tier_3_cleaned.xlsx')
tier_4 = pd.read_excel(r'C:\Users\zhang\Box\Boiler electrification potential\Tier_4_cleaned.xlsx')




cols1 = tier_1.columns.tolist()
cols2 = tier_2.columns.tolist()
cols3 = tier_3.columns.tolist()
cols4 = tier_4.columns.tolist()




df_1 = tier_1[['FACILITY_ID','FACILITY_NAME','COUNTY_FIPS','COUNTY','NAICS',
               'REPORTING_YEAR','UNIT_NAME','UNIT_TYPE','FUEL_TYPE','TIER','INPUT_HEAT_CAPACITY',
               'INPUT_HEAT_CAPACITY_UNIT','AGGR_HIGH_HEAT_CAPACITY','AGGR_HIGH_HEAT_CAPACITY_UOM',
               'FUEL_COM','FUEL_UNIT','ENERGY_COM','ENERGY_MMBtu_hr']]
               
df_2 = tier_2[['FACILITY_ID','FACILITY_NAME','COUNTY_FIPS','COUNTY','NAICS',
               'REPORTING_YEAR','UNIT_NAME','UNIT_TYPE','FUEL_TYPE','TIER_2_METHOD_EQUATION',
               'INPUT_HEAT_CAPACITY','INPUT_HEAT_CAPACITY_UNIT','AGGR_HIGH_HEAT_CAPACITY',
               'AGGR_HIGH_HEAT_CAPACITY_UOM','FUEL_COM','FUEL_UNIT','ENERGY_COM','ENERGY_MMBtu_hr']]              
               
               

# separte tier_3 into three seperate files 
tier_3_c3 = tier_3[tier_3['TIER_3_METHOD_EQUATION']=='Tier 3 (Equation C-3  solid fuel)']
tier_3_c4 = tier_3[tier_3['TIER_3_METHOD_EQUATION']=='Tier 3 (Equation C-4  liquid fuel)']
tier_3_c5 = tier_3[tier_3['TIER_3_METHOD_EQUATION']=='Tier 3 (Equation C-5  gaseous fuel)']
tier_3_blank = tier_3[tier_3['TIER_3_METHOD_EQUATION']=='']



df_3_c3 = tier_3_c3[['FACILITY_ID','FACILITY_NAME','COUNTY_FIPS','COUNTY','NAICS',
                     'REPORTING_YEAR','UNIT_NAME','UNIT_TYPE','FUEL_TYPE','TIER_3_METHOD_EQUATION',
                     'INPUT_HEAT_CAPACITY','INPUT_HEAT_CAPACITY_UNIT','AGGR_HIGH_HEAT_CAPACITY',
                     'AGGR_HIGH_HEAT_CAPACITY_UOM','TIER3_C3_FUEL_QTY_1','TIER3_C3_FUEL_UNIT_1',                  
                     'Energy_consumption_MMBtu','Energy_comsumption_MMBtu_per_hr']]                       
                
                     


df_3_c4 = tier_3_c4[['FACILITY_ID','FACILITY_NAME','COUNTY_FIPS','COUNTY','NAICS',
                     'REPORTING_YEAR','UNIT_NAME','UNIT_TYPE','FUEL_TYPE','TIER_3_METHOD_EQUATION',
                     'INPUT_HEAT_CAPACITY','INPUT_HEAT_CAPACITY_UNIT','AGGR_HIGH_HEAT_CAPACITY',
                     'AGGR_HIGH_HEAT_CAPACITY_UOM','TIER3_C4_FUEL_QTY_1','TIER3_C4_FUEL_UNIT_1',
                     'Energy_consumption_MMBtu','Energy_comsumption_MMBtu_per_hr']]

df_3_c5 = tier_3_c5[['FACILITY_ID','FACILITY_NAME','COUNTY_FIPS','COUNTY','NAICS',
                     'REPORTING_YEAR','UNIT_NAME','UNIT_TYPE','FUEL_TYPE','TIER_3_METHOD_EQUATION',
                     'INPUT_HEAT_CAPACITY','INPUT_HEAT_CAPACITY_UNIT','AGGR_HIGH_HEAT_CAPACITY',
                     'AGGR_HIGH_HEAT_CAPACITY_UOM','TIER3_C5_FUEL_QTY_1','TIER3_C5_FUEL_UNIT_1',
                     'Energy_consumption_MMBtu','Energy_comsumption_MMBtu_per_hr']]

df_3_blank = tier_3_blank[['FACILITY_ID','FACILITY_NAME','COUNTY_FIPS','COUNTY','NAICS',
                           'REPORTING_YEAR','UNIT_NAME','UNIT_TYPE','FUEL_TYPE','TIER_3_METHOD_EQUATION',
                           'INPUT_HEAT_CAPACITY','INPUT_HEAT_CAPACITY_UNIT','AGGR_HIGH_HEAT_CAPACITY',                           
                           'AGGR_HIGH_HEAT_CAPACITY_UOM','Energy_consumption_MMBtu','Energy_comsumption_MMBtu_per_hr']]
                                                    
df_3_blank['FUEL_COM'] = ''
df_3_blank['FUEL_UNIT'] = ''



df_4 = tier_4[['FACILITY_ID','FACILITY_NAME','COUNTY_FIPS','COUNTY','NAICS',
               'REPORTING_YEAR','UNIT_NAME','UNIT_TYPE','FUEL_TYPE','INPUT_HEAT_CAPACITY',
               'INPUT_HEAT_CAPACITY_UNIT','AGGR_HIGH_HEAT_CAPACITY','AGGR_HIGH_HEAT_CAPACITY_UOM',               
               'FUEL_COM','FUEL_UNIT','Energy_consumption_MMBtu','ENERGY_MMBtu_hr']]
               
df_4.loc[:,('TIER')] = 'TIER 4'



# make the names the same for each tier
df_1 = df_1.rename(columns={'ENERGY_COM': 'ENERGY_COM_MMBtu'})
df_2 = df_2.rename(columns = {'TIER_2_METHOD_EQUATION': 'TIER','ENERGY_COM': 'ENERGY_COM_MMBtu'})





df_3_c3 = df_3_c3.rename(columns = {'TIER_3_METHOD_EQUATION':'TIER', 'TIER3_C3_FUEL_QTY_1':'FUEL_COM',
                                    'TIER3_C3_FUEL_UNIT_1':'FUEL_UNIT','Energy_consumption_MMBtu':'ENERGY_COM_MMBtu',
                                    'Energy_comsumption_MMBtu_per_hr':'ENERGY_MMBtu_hr'})                                   
                                    
                                    
df_3_c4 = df_3_c4.rename(columns = {'TIER_3_METHOD_EQUATION':'TIER', 'TIER3_C4_FUEL_QTY_1':'FUEL_COM',
                                    'TIER3_C4_FUEL_UNIT_1':'FUEL_UNIT','Energy_consumption_MMBtu':'ENERGY_COM_MMBtu',
                                    'Energy_comsumption_MMBtu_per_hr':'ENERGY_MMBtu_hr'})

df_3_c5 = df_3_c5.rename(columns = {'TIER_3_METHOD_EQUATION':'TIER', 'TIER3_C5_FUEL_QTY_1':'FUEL_COM',
                                    'TIER3_C5_FUEL_UNIT_1':'FUEL_UNIT','Energy_consumption_MMBtu':'ENERGY_COM_MMBtu',
                                    'Energy_comsumption_MMBtu_per_hr':'ENERGY_MMBtu_hr'})

df_3_blank = df_3_blank.rename(columns = {'TIER_3_METHOD_EQUATION':'TIER','Energy_consumption_MMBtu':'ENERGY_COM_MMBtu',
                                          'Energy_comsumption_MMBtu_per_hr':'ENERGY_MMBtu_hr'})


df_4 = df_4.rename(columns = {'Energy_consumption_MMBtu':'ENERGY_COM_MMBtu',
                              'Energy_comsumption_MMBtu_per_hr':'ENERGY_MMBtu_hr'})



name1 = df_1.columns.tolist()
name2 = df_2.columns.tolist()
name3_c3 = df_3_c3.columns.tolist()
name3_c4 = df_3_c4.columns.tolist()
name3_c5 = df_3_c5.columns.tolist()
name4 = df_4.columns.tolist()



df_overall = pd.concat([df_1,df_2,df_3_c3,df_3_c4,df_3_c5,df_4])




df_overall = df_overall[df_overall['INPUT_HEAT_CAPACITY'].notnull() | df_overall['AGGR_HIGH_HEAT_CAPACITY'].notnull()]



df_overall.to_excel(r'C:\Users\zhang\Box\Boiler electrification potential\Tier_overall_2.xlsx', index = False)






