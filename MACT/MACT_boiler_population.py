#!/usr/bin/env python
# coding: utf-8

#---------------------------

# Collect industrial manfuacturing boiler data from the MACT database

#--------------------------

import pandas as pd
import numpy as np



# filter data for characteristics relevant to industrial boilers

inv_df = pd.read_csv('MACT_inventory.csv')


inv_df = inv_df.loc[:,('FacilityID','UnitID','Unit Count','Classification','Capacity_Numeric',
                       'Op Hours Per Year','Fuel Category for Unit','Temperature','NAICS','Industrial/Commercial')]

inv_df = inv_df.loc[inv_df['Classification']=="Boiler"].drop(columns=['Classification'])

inv_df = inv_df.loc[inv_df['Industrial/Commercial']=="Industrial"].drop(columns=['Industrial/Commercial'])

inv_df = inv_df.rename(columns={'Capacity_Numeric':'Capacity (mmBtu/hr)'})


# get facility and NAICS data

facility = pd.read_csv('MACT_facility.csv')

facility = facility.loc[:,('FacilityID','2d_Phys_Zip','7a2_NumberFacilityEmployees','9_NAICS')].rename(
    columns={'2d_Phys_Zip':'Zip','7a2_NumberFacilityEmployees':'Num_Empl','9_NAICS' :'NAICS'})

facility = facility.loc[(facility['NAICS']<=339) & (facility['NAICS']>=311)].dropna(subset=['Zip'])

facility['Zip'] = facility.Zip.astype(str).str[:5].str.zfill(5)


# merge technical characteristics with facility and NAICS data
inv_fac = inv_df.merge(facility,how="inner",on=['FacilityID','NAICS'])


# convert zip codes to FIPS codes

zip_to_fips = pd.read_csv('ZIP_COUNTY_032020.csv')

zip_to_fips = zip_to_fips.rename(columns={'COUNTY':'FIPS','ZIP':'Zip'}).drop(
    columns=['RES_RATIO','BUS_RATIO','OTH_RATIO','TOT_RATIO'])

zip_to_fips['FIPS'] = zip_to_fips.FIPS.astype(str).str[:5].str.zfill(5)
zip_to_fips['Zip'] = zip_to_fips.Zip.astype(str).str[:5].str.zfill(5)



mact_loc = inv_fac.merge(zip_to_fips,how="inner",on=['Zip']).drop_duplicates(subset=['FacilityID', 'UnitID'], keep='first')


# save MACT boilers data to file
mact_loc.to_csv('MACT_location_capacity.csv')



