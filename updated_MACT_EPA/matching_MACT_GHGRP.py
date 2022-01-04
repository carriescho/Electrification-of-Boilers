#!/usr/bin/env python
# coding: utf-8

#------------------------------

# Check for duplicate boiler units in GHGRP and MACT databases
# Find duplicate units via matches in facility name/ID, capacity values, and FIPS codes

#------------------------------


import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re
from pandas import DataFrame



#read in MACT boiler data with facility information
mact = pd.read_csv('..\MACT\MACT_location_capacity.csv').drop(columns='Unnamed: 0')

mact.loc[:,'State'] = mact.FacilityID.str[0:2]

mact.loc[:,'FacilityID'] = mact.FacilityID.str[2:]

mact.loc[:,'FacilityID'] = mact.apply(lambda x: re.sub(r"(\w)([A-Z])", r"\1 \2", x['FacilityID']),axis=1)

mact.loc[:,'mactID'] = mact.index + 1


#list of unique FacilityIDs in MACT data set
mact_fac = mact['FacilityID'].unique()


#read in GHGRP boiler data, filter for NAICS 311-339
tier = pd.read_csv('..\GHGRP\Tier_overall_2.csv').rename(columns={'COUNTY_FIPS':'FIPS'})

tier = tier[(tier['NAICS']>=311111)&
            (tier['NAICS']<=339999)].drop_duplicates(subset=tier.columns.difference(['COUNTY']))

tier.loc[:,'tierID'] = tier.index + 1


#create dictionary of unique GHGRP FACILITY_NAMEs
tier_fac = tier['FACILITY_NAME'].unique()

tier_fac_df = DataFrame(tier_fac,columns=['FACILITY_NAME'])

#find the closest match between GHGRP FACILITY_NAME and MACT FacilityIDs
tier_fac_df.loc[:,'Facility_ID'] = tier_fac_df.apply(lambda x: process.extractOne(x['FACILITY_NAME'],mact_fac),axis=1)

tier_fac_df['Facility_ID'] = tier_fac_df.Facility_ID.astype(str).str[2:-6]

#dictionary matching GHGRP FACILITY_NAME and MACT FacilityIDs
fac_dict = tier_fac_df.set_index('FACILITY_NAME')['Facility_ID'].to_dict()



#add column with the MACT FacilityID that was the closest match
tier.loc[:,'FacilityID'] = tier.apply(lambda x: fac_dict[x['FACILITY_NAME']],axis=1)

#merge based on FacilityID and FIPS
tier_mact = tier.merge(mact,how="inner",on=['FacilityID','FIPS'])



#compare capacitiy values, and keep only matching capacity values
tier_mact_matches = tier_mact.loc[(tier_mact['INPUT_HEAT_CAPACITY']==tier_mact['Capacity (mmBtu/hr)']) | 
                          (tier_mact['AGGR_HIGH_HEAT_CAPACITY']==tier_mact['Capacity (mmBtu/hr)'])].copy()



#remove duplicate ghgrp_tier entries by keeping only the first mact entry that matched to it
tier_mact_matches.drop_duplicates(subset='tierID',
                                  keep="first", inplace=True)



#identify entries that were not matched and create combined dataframe

tierID_matches = tier_mact_matches['tierID'].unique()
mactID_matches = tier_mact_matches['mactID'].unique()

tier_nm = tier.loc[~tier['tierID'].isin(tierID_matches)].drop(columns='FacilityID')
mact_nm = mact.loc[~mact['mactID'].isin(mactID_matches)].rename(columns={'FIPS':'FIPS_m','NAICS':'NAICS_sub'})
                   
tier_mact_nonmatch = pd.concat([tier_nm, mact_nm], ignore_index=True,sort=False)


#save to files
tier_mact_matches.to_csv('tier_mact_matches2.csv',index=False)
tier_mact_nonmatch.to_csv('tier_mact_nonmatch2.csv',index=False)


