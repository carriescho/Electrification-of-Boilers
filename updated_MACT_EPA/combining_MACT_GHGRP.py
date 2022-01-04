#!/usr/bin/env python
# coding: utf-8

#-----------------------------

# Combine the unique boiler data from GHGRP and MACT databases and remove duplicate units
# Assign common names for the categories: capacity, fuel type, FIPS, facility name, unit name, reporting year

#-----------------------------


import pandas as pd
import numpy as np

match = pd.read_csv('tier_mact_matches2.csv')
nonmatch = pd.read_csv('tier_mact_nonmatch2.csv')


match.rename(columns={'NAICS_x':'NAICS',
                      'NAICS_y':'NAICS_sub'}, inplace=True)


combined = pd.concat([match, nonmatch])



def use_recent_reporting_year(combined_df,analysis_type):
    
    #put capacity values all under one column, Capacity (mmBtu/hr)
    combined_df['Capacity (mmBtu/hr)'].fillna(value=combined_df['AGGR_HIGH_HEAT_CAPACITY'],inplace=True)

    combined_df['Capacity (mmBtu/hr)'].fillna(value=combined_df['INPUT_HEAT_CAPACITY'],inplace=True)

    combined_df = combined_df[(combined_df['Capacity (mmBtu/hr)']!=0)&
                              (combined_df['Capacity (mmBtu/hr)']!=18644810)&
                              (combined_df['Capacity (mmBtu/hr)']!=18614040)].reset_index(drop=True)


    #put facility name/id, unit name/id, FIPS all under one column 
    combined_df['FACILITY_NAME'].fillna(value=combined_df['FacilityID'],inplace=True)

    combined_df['UNIT_NAME'].fillna(value=combined_df['UnitID'],inplace=True)

    combined_df['FIPS'].fillna(value=combined_df['FIPS_m'],inplace=True)
    
    combined_df['FUEL_TYPE'].fillna(value=combined_df['Fuel Category for Unit'],inplace=True)

    #drop unnecessary columns
    combined_df.drop(columns=['AGGR_HIGH_HEAT_CAPACITY','AGGR_HIGH_HEAT_CAPACITY_UOM',
                              'INPUT_HEAT_CAPACITY','INPUT_HEAT_CAPACITY_UNIT',
                              'FacilityID','UnitID', 'FIPS_m', 'Zip',
                              'State','Num_Empl'],inplace=True)
    
    
    #take data from only most recent reporting year
    #MACT data doesn't list reporting year, but data was last updated in 2012
    combined_df['REPORTING_YEAR'].fillna(value=2012,inplace=True)
    
    
    if analysis_type=='energy':
        
        combined_df = combined_df[(combined_df['ENERGY_COM_MMBtu'].notna()) &
                                  (combined_df['ENERGY_COM_MMBtu']!='na')]

        combined_df = combined_df[combined_df.ENERGY_COM_MMBtu.astype(float)>0]

        
    combined_df = combined_df.groupby(['FACILITY_NAME','UNIT_NAME','FUEL_TYPE'],
                                      group_keys=False).apply(lambda x: x.sort_values('REPORTING_YEAR'))
    
    combined_df = combined_df.drop_duplicates(subset=['FACILITY_ID','FACILITY_NAME',
                                                      'UNIT_NAME','FUEL_TYPE','NAICS'], 
                                              keep="last").reset_index(drop=True)
    
    
    #replicate an entry based on its unit count
    combined_df['Unit Count'].fillna(value=1, inplace=True)

    total_count = pd.DataFrame(combined_df.values.repeat(combined_df['Unit Count'], axis=0),
                               columns=combined_df.columns)

    
    return total_count


total_cap = use_recent_reporting_year(combined,'capacity')

total_energy = use_recent_reporting_year(combined,'energy')




#save to file
total_cap.to_csv('total_tier_mact_cap2.csv')
total_energy.to_csv('total_tier_mact_energy2.csv')





# check that energy consumption calculations make sense by checking if energy consumption
# divided by operating hours is less than capacity; if greater, then check energy cons. calculations 

total_energy['Op Hours Per Year'].fillna(value=8760,inplace=True)

energy_check = total_energy[(total_energy['ENERGY_COM_MMBtu'].notna()) &
                            (total_energy['ENERGY_COM_MMBtu']!='na')]

cap_lessthan_mmbtuhr = energy_check[((
    (energy_check['ENERGY_COM_MMBtu'].astype(float)) / energy_check['Op Hours Per Year']) > \
    energy_check['Capacity (mmBtu/hr)'])].copy()

cap_lessthan_mmbtuhr.loc[:,'ENERGY_MMBtu_hr_check'] =     (cap_lessthan_mmbtuhr['ENERGY_COM_MMBtu'].astype(float))/cap_lessthan_mmbtuhr['Op Hours Per Year']
    
cap_lessthan_mmbtuhr





