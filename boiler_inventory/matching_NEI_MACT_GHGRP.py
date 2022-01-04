#!/usr/bin/env python
# coding: utf-8

#------------------------

# Add the boiler data from the NEI databse with the combined GHGRP and MACT data
# Create dataset of total reported boiler inventory

#------------------------

import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re
from pandas import DataFrame



nei = pd.read_csv(r'..\NEI\NEI_boilers.csv')



ghgrp_mact = pd.read_csv(r'updated_MACT_EPA\total_tier_mact_cap2.csv').drop(columns='Unnamed: 0')

ghgrp_mact.loc[:,'gmID'] = ghgrp_mact.index + 1

# keep data where NAICS code is known and record the 3 digit NAICS code
ghgrp_mact.loc[(~ghgrp_mact.NAICS.isnull())&
               ghgrp_mact.NAICS_sub.isnull(),'NAICS_sub'] = ghgrp_mact['NAICS'].astype(str).str[:3]

# delete OCS entries that don't have boiler in the unit name - there's no validation that they're boilers
ghgrp_mact.drop(ghgrp_mact[(ghgrp_mact.UNIT_TYPE=='OCS (Other combustion source)')&
                             (~ghgrp_mact.UNIT_NAME.str.contains('Boil|boil|BLR|BOIL'))].index, inplace=True)



#--- check that boilers in the GHGRP are not CHP units by comparing facility data to EIA form 923

ghgrp = ghgrp_mact[~ghgrp_mact.tierID.isnull()].copy()

# crosswalk from Industrial Energy Tool (https://github.com/NREL/Industry-Energy-Tool), data from 2015
xwalk = pd.read_csv(r'C:\Users\Carrie Schoeneberger\Downloads\eia_epa_xwalk_923.csv')

# EIA form 923 provides data on CHP electricity generation...check if any of these units are in GHGRP
eia923_2019 = pd.read_excel(r'C:\Users\Carrie Schoeneberger\Box\NU_Research\ElectrificationofBoilers\923data_2019.xlsx')
eia923_2019.rename(columns={'Plant_ID':'EIA_PLANT_ID'},inplace=True)
eia923_2019 = eia923_2019[eia923_2019.CHP_Plant=='Y']

#---
# check which data from the 2019 form is not in the current xwalk, then check if in the ghgrp data
# done separately, identified several facilities new in the 2019 data, using code below but don't need to re-run

#eia923_matches = eia923_2019.merge(xwalk,on='EIA_PLANT_ID',how='left')
#eia923_matches[eia923_matches.FACILITY_NAME_x.isnull()]
#---


# add the several facilities from 2019 data that are in ghgrp into the crosswalk key
ghgrp_name = ['WESTERN SUGAR COOPERATIVE','TORAY PLASTICS (AMERICA) INC.','Ingredion Incorporated',
              'ERVING PAPER MILLS INC','CHEVRON PRODUCTS EL SEGUNDO REFINERY',
              'White Springs Agricultural Chemical dba Nutrien','VERSO PAPER CORP. - SARTELL MILL']

ghgrp_id = [1001783,1000338,1012516,1005037,1007978,1006467,1003164]

eia_name = ['Western Sugar Cooperative - Billings','Toray Plastics America','Ingredion Inc - Winston Salem',
            'Erving Paper Mills Inc','Chevron USA Inc','White Springs Agrici Chem Inc','Verso Corporation']

eia_id =[62319,61857,54618,54228,52076,50474,50424]

eia923_2019_add = pd.DataFrame(list(zip(ghgrp_name,ghgrp_id,eia_name,eia_id)),
                               columns=['FACILITY_NAME_x','FACILITY_ID','EIA_name','EIA_PLANT_ID'])

xwalk_2019 = xwalk.append(eia923_2019_add,ignore_index=True)

ghg_eia_matches = ghgrp.merge(xwalk_2019,on='FACILITY_ID',how='left')

ghg_eia_ls = list(ghg_eia_matches[~ghg_eia_matches.EIA_name.isnull()].drop_duplicates(
    subset=['tierID'],keep='first').gmID.unique())


ghgrp_mact.drop(ghgrp_mact[ghgrp_mact.gmID.isin(ghg_eia_ls)].index, inplace=True)

#---


# SKIP if 'nei_ghgrp_mact_sites.csv' is already up to date
# creates dataframe that matches the names of NEI sites and GHGRP-MACT facilities, then saves as file; takes >2 hr
"""    
def find_facility_matches(ghgrp_mact_df, nei_df):
    
    # list of unique facility names in GHGRP-MACT dataset
    ghgrp_mact_fac_ls = ghgrp_mact_df['FACILITY_NAME'].unique()

    # make dataframe of NEI site names
    nei_fac_ls = nei_df['site_name'].unique()
    nei_fac_df = DataFrame(nei_fac_ls,columns=['site_name'])
    
    # compare NEI site names to GHGRP-MACT names and find matches 
    nei_fac_df.loc[:,'FACILITY_NAME'] = nei_fac_df.apply(
        lambda x: process.extractOne(x['site_name'],ghgrp_mact_fac_ls),axis=1)
    
    return nei_fac_df

nei_gm_fac = find_facility_matches(ghgrp_mact,nei) 
nei_gm_fac.to_csv('nei_ghgrp_mact_sites.csv')
"""



# Check the rest of GHGRP-MACT data against NEI data based on matched facilities

nei_gm_fac = pd.read_csv('nei_ghgrp_mact_sites.csv')

# delete excess parentheses and matching score
nei_gm_fac['FACILITY_NAME'] = nei_gm_fac['FACILITY_NAME'].astype(str).str[2:-6]

# create dictionary between NEI site names and GHGRP-MACT facility names
fac_dict = nei_gm_fac.set_index('site_name')['FACILITY_NAME'].to_dict()


# add column with the GHGRP-MACT facility name that was the closest match to NEI site name
nei.loc[:,'FACILITY_NAME'] = nei.apply(lambda x: fac_dict[x['site_name']],axis=1)


# merge based on facility name and fips
ghgrp_mact.rename(columns={'FIPS':'fips'},inplace=True)
nei_ghgrp_mact = nei.merge(ghgrp_mact, how='inner',on=['FACILITY_NAME','fips'])


# compare capacitiy values, and keep only matching capacity values  CHANGE BACK TO CAP_MMBTUHR
ngm_matches = nei_ghgrp_mact.loc[(abs(nei_ghgrp_mact['design_capacity']-nei_ghgrp_mact['Capacity (mmBtu/hr)']))<=1].copy()


# remove duplicate NEI entries by keeping only the first GHGRP-MACT entry that matched to it
ngm_matches.drop_duplicates(subset=['eis_unit_id'],keep='first', inplace=True) 

# identify entries that were not matched and create combined dataframe
nei_matches = ngm_matches['eis_unit_id'].unique()
gm_matches = ngm_matches['gmID'].unique()

nei_nm = nei.loc[~nei['eis_unit_id'].isin(nei_matches)]
gm_nm = ghgrp_mact.loc[~ghgrp_mact['gmID'].isin(gm_matches)]

ngm_nonmatch = pd.concat([nei_nm,gm_nm],ignore_index=True,sort=False)



total_units = pd.concat([ngm_matches, ngm_nonmatch])




# assign common names among the combined NEI, GHGRP, and MACT data for the following data categories

total_units['county'].fillna(value=total_units['COUNTY'],inplace=True)
total_units['site_name'].fillna(value=total_units['FACILITY_NAME'],inplace=True)
total_units['naics_code'].fillna(value=total_units['NAICS'],inplace=True)
total_units['naics_sub'].fillna(value=total_units['NAICS_sub'],inplace=True)
total_units['fuel_type'].fillna(value=total_units['FUEL_TYPE'],inplace=True)
total_units['cap_mmbtuhr'].fillna(value=total_units['Capacity (mmBtu/hr)'],inplace=True)
total_units['REPORTING_YEAR'].fillna(value=total_units['calc_data_year'],inplace=True)

total_units.loc[:,'naics_sub'] = total_units.naics_sub.astype(float)

# remove outliers where capacity (mmbtu/hr) >1800 (which is the max industrial boiler size in GHGRP or MACT)
# and where capacity is NA

total_units = total_units.reset_index(drop=True)
total_units.drop(total_units[(total_units.cap_mmbtuhr > 1800)|
                             (total_units.cap_mmbtuhr.isnull())].index, inplace=True)


total_units.drop(columns=['COUNTY','FACILITY_NAME','FACILITY_ID','NAICS','NAICS_sub','UNIT_NAME',
                          'FUEL_TYPE','Unit Count','Capacity (mmBtu/hr)','Fuel Category for Unit',
                          'Temperature','design_capacity','design_capacity_uom','eis_process_id',
                          'eis_facility_id','unit_description','process_description','unit_type',
                          'calc_data_year'],inplace=True)


# label the data source each boiler entry came from

total_units.loc[(total_units.eis_unit_id > 0) &
                (total_units.tierID > 0) &
                (total_units.mactID > 0),'data_source'] = 'NEI, GHGRP, MACT'

total_units.loc[(total_units.eis_unit_id > 0) &
                (total_units.tierID > 0) &
                (total_units.mactID.isnull()), 'data_source'] = 'NEI, GHGRP'
                 
total_units.loc[(total_units.eis_unit_id > 0) &
                (total_units.tierID.isnull()) &
                (total_units.mactID > 0), 'data_source'] = 'NEI, MACT'

total_units.loc[(total_units.eis_unit_id.isnull()) &
                (total_units.tierID > 0) &
                (total_units.mactID > 0), 'data_source'] = 'GHGRP, MACT'
                 
total_units.loc[(total_units.eis_unit_id.isnull()) &
                (total_units.tierID > 0) &
                (total_units.mactID.isnull()), 'data_source'] = 'GHGRP'

total_units.loc[(total_units.eis_unit_id.isnull()) &
                (total_units.tierID.isnull()) &
                (total_units.mactID > 0), 'data_source'] = 'MACT'

total_units.drop(columns=['tierID','mactID','gmID'],inplace=True)



# group fuel types under common names

def classify_fuel_types(all_units):
    
    # classify to Coal, Natural Gas, Biomass, Biomass - gas, Other fuels-solid, Other fuels-gas,
    all_units.fuel_type.fillna(value='nan',inplace=True)
    
    all_units.loc[all_units.fuel_type.str.contains('Coal|Bituminous|Subbituminous|Lignite|Coke|Anthracite|Mixed|coke'),
                  'fuel_type'] = 'coal'
    all_units.loc[all_units.fuel_type.str.contains('Gas 1|Natural Gas'),
                  'fuel_type'] = 'natural gas'
    all_units.loc[all_units.fuel_type.str.contains('Fuel Gas|Blast Furnace|Process Gas|Coke Oven Gas|Propane'),
                  'fuel_type'] = 'other fuels' #gas
    all_units.loc[all_units.fuel_type.str.contains('Solid Byproducts|Plastics|Tires'),
                  'fuel_type'] = 'other fuels' #solid
    all_units.loc[all_units.fuel_type.str.contains('Wet Biomass|Bagasse|Dry Biomass|Wood|Agricultural'),
                  'fuel_type'] = 'biomass'
    all_units.loc[all_units.fuel_type.str.contains('Biomass Gases|Biogas|Landfill|Biodiesel|Animal|Vegetable'),
                  'fuel_type'] = 'biomass' #gas and liquids
    
    all_units.loc[all_units.fuel_type.str.contains(
        'Fuel Oil|fuel oil|Liquid|diesel|Lubricants|LPG|Used|401|Gasoline|Kerosene|Unfinished|Butane|lene|Oils|Iso'),
                  'fuel_type'] = 'oil products'
    
    return

classify_fuel_types(total_units)



#save to file
total_units.to_csv('total_reported_boilers.csv',index=False)




# ------get average of operating hours by naics subsector

opHrs = ghgrp_mact[~ghgrp_mact['Op Hours Per Year'].isnull()]

opHrs = opHrs.groupby('NAICS_sub')['Op Hours Per Year'].mean().reset_index()

opHrs.rename(columns={'NAICS_sub':'naics_sub','Op Hours Per Year':'op_hrs'},inplace=True)

opHrs.to_csv('operating_hours_avg.csv',index=False)

# -----get weighted average operating hours
opHrs = opHrs.groupby('NAICS_sub').apply(
    lambda x: (x['Op Hours Per Year']*x['Capacity (mmBtu/hr)']).sum() / (x['Capacity (mmBtu/hr)']).sum()).reset_index()

opHrs.rename(columns={'NAICS_sub':'naics_sub',0:'op_hrs'},inplace=True)

opHrs.to_csv('operating_hours_wt_avg.csv',index=False)

