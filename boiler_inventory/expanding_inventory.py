#!/usr/bin/env python
# coding: utf-8

#--------------

# Estimate the count, capacity, and fuel type of non-reported boilers (not in the GHGRP, MACT, or NEI) 
#    from county-level boiler fuel use data (NREL thermal energy use in manufacturing dataset)
# Add these non-reported boilers to the boiler inventory for a final industrial boiler dataset

#--------------


import pandas as pd
import numpy as np
from pandas import DataFrame



# Read in inventory dataset of reported boilers from the three EPA databases and group capacity by county, naics

inv = pd.read_csv('total_reported_boilers.csv')

inv.drop(inv[inv['cap_mmbtuhr']==0].index, inplace = True)

# fips > 57000 (not in united states)
inv.drop(inv[inv.fips>57000].index,inplace=True)

inv_cty = inv.groupby(['fips','naics_sub'])['cap_mmbtuhr'].agg(['sum','count','mean']).reset_index()

inv_cty.rename(columns={'sum':'inv_sum','count':'inv_count','mean':'inv_mean'},inplace=True)




# Read in Industrial Thermal Energy Use data and group energy totals by county, naics subsector

mfg_eu_temps = pd.read_parquet('mfg_eu_temps_20200826_2224.parquet.gzip', engine='pyarrow') 

mfg_eu_temps.loc[:,'naics_sub'] = mfg_eu_temps.naics.astype(str).str[:3].astype(int)

mfg_blr = mfg_eu_temps[mfg_eu_temps.End_use=='Conventional Boiler Use'].copy()

#mfg_blr_cty = mfg_blr.groupby(['COUNTY_FIPS','naics_sub','MECS_FT'])['MMBtu'].sum().reset_index()
mfg_blr_cty = mfg_blr.groupby(['COUNTY_FIPS','naics_sub'])['MMBtu'].sum().reset_index()

mfg_blr_cty.rename(columns={'COUNTY_FIPS':'fips'},inplace=True)




# Read in average operating hours by naics subsector, calculated from GHGRP-MACT data in "matching_NEI_MACT_GHGRP.py"

op_hrs = pd.read_csv('operating_hours_avg.csv')

hrs_315 = pd.DataFrame({'naics_sub':[315],'op_hrs':[5472]},columns=['naics_sub','op_hrs'])

op_hrs = op_hrs.append(hrs_315,ignore_index=True)




# Determine median boiler capacity by naics subsector

med_capacity = inv.groupby('naics_sub')['cap_mmbtuhr'].median().reset_index()
#med_capacity = inv.groupby('naics_sub')['cap_mmbtuhr'].mean().reset_index()

med_capacity.rename(columns={'cap_mmbtuhr':'med_cap'},inplace=True)




# Determine breakdown of capacity ranges by naics subsector from inventory of reported units...
naics_inv=inv.copy()

cut_labels = ['<10', "10-50", '50-100', '100-250', '>250']
cut_bins =[-1, 10, 50, 100, 250,1800]

naics_inv['cap_bins'] = pd.cut(naics_inv['cap_mmbtuhr'], bins=cut_bins, labels = cut_labels)
naics_inv = naics_inv.groupby(['naics_sub','cap_bins'])['cap_mmbtuhr'].count()
naics_inv = naics_inv.reset_index().rename(columns={'cap_mmbtuhr':'count'})
naics_inv.naics_sub = naics_inv.naics_sub.astype(int)
naics_inv = naics_inv.pivot(index='naics_sub',columns='cap_bins',values='count')



capacity_dict ={'<10':5,'10-50':30,'50-100':75,'100-250':175,'>250':251}

# create dictionary of ratio between 
ratio = list(naics_inv['<10'].div(naics_inv['10-50']).to_frame(name='ratio').to_dict().values())[0]




def meanderingArray(unsortedArray):
    #sort array in ascending order
    ascendingSort = sorted(unsortedArray)
    #create empty list to sort items in meandering order
    meanderedArray = []
    
    #Giving that the array is not empty or with only one element
    while len(ascendingSort) > 1:
        #take the biggest and smallest from the ascendingSort list and add the to the meandering list
        meanderedArray += [ascendingSort[-1], ascendingSort[0]]
        #remove the biggest and smallest from the ascending sort list to allow for new biggest and new smallest
        ascendingSort = ascendingSort[1:-1]
  
      #add whatever is left of ascending sort list to the meanderedArray (i.e. empty or 1 item)
    meanderedArray += ascendingSort
    
    return meanderedArray



# Combine county thermal energy use data with inventory of reported boilers 
#   to find where/how many boiler units are missing

# Where thermal energy use is negligent in single counties and subsectors, do not include
mfg_blr_cty.drop(mfg_blr_cty[mfg_blr_cty['MMBtu'] < 50].index, inplace = True)

comb = mfg_blr_cty.merge(inv_cty,on=['fips','naics_sub'],how='left')

comb = comb.merge(op_hrs,on='naics_sub',how='left')

comb = comb.merge(med_capacity, on='naics_sub', how='left')



# Case 1: for counties where the fuel use exists but no boiler in inventory, 
# add boilers based on fuel use, med cap, op hours

comb.loc[comb['inv_count'].isnull(),'est_count'] = comb['MMBtu']/comb['op_hrs']/comb['med_cap']


# assigning est capacity to small capacities based on breakdown by subsector
for i in list(comb['naics_sub'].values):
    
    naics_index = list(comb[(comb.naics_sub==i) & (comb['inv_mean'].isnull())].index)
    
    index_rearr = meanderingArray(naics_index)
    
    r = ratio.get(i)
    
    first_half = round(len(index_rearr) * r/(r+1))
    
    less_ten,ten_fifty = index_rearr[:first_half], index_rearr[first_half:]
    
    comb.loc[less_ten,'est_cap'] = 5
    comb.loc[ten_fifty,'est_cap'] = 30


    
# Case 2: for counties where the fuel use is more than current boilers, find out how many boilers to add

comb.loc[comb['inv_sum']*comb['op_hrs'] < comb['MMBtu'],
         'cap_diff'] = (comb['MMBtu']-comb['inv_sum']*comb['op_hrs'])/comb['op_hrs']

comb.loc[~comb.cap_diff.isnull(),'est_count'] = comb['cap_diff']/comb['inv_mean']


# assigning est capacity to small capacities based on breakdown by subsector
for i in list(comb['naics_sub'].values):
    
    naics_index = list(comb[(comb.naics_sub==i) & (~comb['inv_mean'].isnull())].index)
    
    index_rearr = meanderingArray(naics_index)
    
    r = ratio.get(i)
    
    first_half = round(len(index_rearr) * r/(r+1))
    
    less_ten,ten_fifty = index_rearr[:first_half], index_rearr[first_half:]
    
    comb.loc[less_ten,'est_cap'] = 5
    comb.loc[ten_fifty,'est_cap'] = 30


# Case 3: for counties where fuel use is less than current boilers, no new boilers to add

comb.loc[comb.est_count.isnull(),'est_count'] = 0


# --------------------
# Limit estimated count by setting additional boilers equal to no more than the current count
comb.loc[comb['est_count']>(comb['inv_count']*1),'est_count'] = comb['inv_count']*1


# if est_count is b/t 0 and 1, round up to 1
comb.loc[comb.est_count < 1, 'est_count'] = comb['est_count'].apply(np.ceil)
# otherwise, round normally
comb.loc[comb.est_count > 1, 'est_count'] = round(comb['est_count'])





# create a line for each additional boiler, accounting for multiples
addtl_blrs = pd.DataFrame(comb.values.repeat(comb['est_count'], axis=0),
                               columns=comb.columns)

est_blrs = addtl_blrs[['fips','naics_sub','est_cap','est_count']].copy()

est_blrs.rename(columns={'est_cap':'cap_mmbtuhr'},inplace=True)

est_blrs.loc[:,'data_source'] = 'Estimate'





# assign fuel type to estimated boilers

mfg_f = mfg_blr.groupby(
    ['COUNTY_FIPS','naics_sub','MECS_FT'])['MMBtu'].sum().reset_index().rename(
    columns={'COUNTY_FIPS':'fips'})

mfg_f.loc[((mfg_f.naics_sub==311)|(mfg_f.naics_sub==312)|
          (mfg_f.naics_sub==321)|(mfg_f.naics_sub==322))&
          (mfg_f.MECS_FT=='Other'),'MECS_FT']= 'biomass'

fuel_type_dict = {'Diesel':'oil products','LPG_NGL':'oil products','Natural_gas':'natural gas',
                  'Residual_fuel_oil':'oil products','Other':'other fuels','Coal':'coal','Coke_and_breeze':'coal'}

mfg_f.replace({'MECS_FT': fuel_type_dict},inplace=True)

mfg_f = mfg_f.groupby(['fips','naics_sub','MECS_FT'])['MMBtu'].sum().reset_index()

mfg_f.rename(columns={'MECS_FT':'fuel_type'},inplace=True)


# determine maximum fuel type (by energy) in each county, naics subsector 
max_fuel = mfg_f[mfg_f.groupby(['fips','naics_sub'])['MMBtu'].transform(max)==mfg_f['MMBtu']].copy()

max_fuel.drop(columns=['MMBtu'],inplace=True)




# if est count==1, assign max fuel type from energy use
est_blrs_f = est_blrs.merge(max_fuel,on=['fips','naics_sub'],how='left')

# -------------------

# if est count>1, assign based on 1st 2nd 3rd etc fuel type
# first, nan all fuel types where count>1
est_blrs_f.loc[est_blrs_f.est_count>1,'fuel_type'] = np.nan


# only the rows where count>1 (the duplicate rows)
est_blrs_dup = est_blrs_f[est_blrs_f.duplicated(keep=False)]

#--------------------

# get percentages of fuel types by county, naics
fuel_perc=mfg_f.groupby(['fips','naics_sub','fuel_type'])['MMBtu'].sum()/mfg_f.groupby(['fips','naics_sub'])['MMBtu'].sum()

fuel_perc = fuel_perc.reset_index()

#fuel_perc.drop(fuel_perc[fuel_perc['MMBtu']==1].index,inplace=True)

fuel_perc.sort_values(by=['fips','naics_sub','MMBtu'], ascending=[True,True,False],inplace=True)

#--------------------

# match fuel types sorted and weighted by energy use to est boilers in county, naics
est_blrs_dup_f =est_blrs_dup.merge(fuel_perc,on=['fips','naics_sub'],how='left').drop_duplicates(
    subset=['fips','naics_sub','fuel_type_y'])


est_blrs_dup_f = est_blrs_dup_f.drop(columns=['fuel_type_x']).rename(columns={'fuel_type_y':'fuel_type'})

est_blrs_dup_f.loc[:,'ft_count'] = round(est_blrs_dup_f.est_count*est_blrs_dup_f.MMBtu)

est_blrs_dup_f.drop(est_blrs_dup_f[est_blrs_dup_f.ft_count==0].index,inplace=True)

#------------------ correcting for rounding errors
est_blrs_dup_f.loc[(est_blrs_dup_f.fips==19149)&
                   (est_blrs_dup_f.naics_sub==311)&
                   (est_blrs_dup_f.fuel_type=='natural gas'),'ft_count'] = 28

est_blrs_dup_f.loc[(est_blrs_dup_f.fips==31177)&
                   (est_blrs_dup_f.naics_sub==311)&
                   (est_blrs_dup_f.fuel_type=='natural gas'),'ft_count'] = 45

est_blrs_dup_f.loc[(est_blrs_dup_f.fips==40017)&
                   (est_blrs_dup_f.naics_sub==311)&
                   (est_blrs_dup_f.fuel_type=='natural gas'),'ft_count'] = 24

est_blrs_dup_f.loc[(est_blrs_dup_f.fips==19113)&
                   (est_blrs_dup_f.naics_sub==325)&
                   (est_blrs_dup_f.fuel_type=='natural gas'),'ft_count'] = 46

est_blrs_dup_f.loc[(est_blrs_dup_f.fips==49035)&
                   (est_blrs_dup_f.naics_sub==325)&
                   (est_blrs_dup_f.fuel_type=='coal'),'ft_count'] = 2
#---------------------

d = est_blrs_dup_f[est_blrs_dup_f.ft_count<est_blrs_dup_f.est_count].groupby(
    ['fips','naics_sub'])['ft_count'].sum().to_dict()

ft_total = []

for i in est_blrs_dup_f[est_blrs_dup_f.ft_count<est_blrs_dup_f.est_count].index.to_list():

    ft_total.append(d.get((est_blrs_dup_f.fips.loc[i],est_blrs_dup_f.naics_sub.loc[i])))
    

est_blrs_dup_f.loc[est_blrs_dup_f.ft_count<est_blrs_dup_f.est_count,'ft_total']=ft_total

est_blrs_dup_f.loc[est_blrs_dup_f.ft_total<est_blrs_dup_f.est_count,'ft_count'] = est_blrs_dup_f['est_count']


est_blrs_dup_total = pd.DataFrame(est_blrs_dup_f.values.repeat(est_blrs_dup_f['ft_count'], axis=0),
                               columns=est_blrs_dup_f.columns)

est_blrs_dup_total.drop(columns=['est_count','MMBtu','ft_count','ft_total'],inplace=True)


# remove duplicate counts, and re-add in duplicate counts with fuel types
est_blrs_fuel = est_blrs_f[~est_blrs_f.fuel_type.isnull()].copy()

est_blrs_fuel_total = est_blrs_fuel.append(est_blrs_dup_total).sort_values(by=['fips','naics_sub'])

est_blrs_fuel_total.drop(columns=['est_count'],inplace=True)





# combine inventory of reported units with estimated units for total expanded inventory
expanded_inv = inv.append(est_blrs_fuel_total,ignore_index=True)

# for columns with strings, fill null values with 'na' to standardize data type in columns
expanded_inv[['state','county','company_name','site_name',
              'zip_code','UNIT_TYPE','TIER','FUEL_UNIT',
              'ENERGY_COM_MMBtu','ENERGY_MMBtu_hr']] = expanded_inv[['state','county','company_name','site_name',
                                                                     'zip_code','UNIT_TYPE','TIER','FUEL_UNIT',
                                                                     'ENERGY_COM_MMBtu','ENERGY_MMBtu_hr']].fillna('na')

# save to file
expanded_inv.to_csv('total_boiler_inventory.csv',index=False)



