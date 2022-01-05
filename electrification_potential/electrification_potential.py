#!/usr/bin/env python
# coding: utf-8

#-------------------

# Calculate the electrification potential (electricity required to meet steam demand of boilers)
# Calculate the resulting fuel use for electricity and GHG emissions based on regional electric grid mixes

#-------------------


import pandas as pd
import numpy as np
import pyarrow
import matplotlib.pyplot as plt
import extra_fuels_for_elec




# read in manfacturing thermal energy use data

mfg_eu_temps = pd.read_parquet('mfg_eu_temps_20200826_2224.parquet.gzip', engine='pyarrow') 




# group only boiler fuel use by county, naics, and fuel type

mfg_eu_temps.loc[:,'naics_sub'] = mfg_eu_temps.naics.astype(str).str[:3].astype(int)

mfg_blr = mfg_eu_temps[mfg_eu_temps.End_use=='Conventional Boiler Use'].copy()

mfg_blr_cty = mfg_blr.groupby(['COUNTY_FIPS','naics_sub','MECS_FT'])['MMBtu'].sum().reset_index()


# calculate electrification potential based on combustion boiler & electric boiler efficiencies

blr_efficiency = {'Natural_gas': 0.75, 'LPG_NGL': 0.82, 'Diesel': 0.83, 'Coal': 0.81, 
                  'Residual_fuel_oil': 0.83, 'Coke_and_breeze': 0.70, 'Other': 0.70 }
                            #efficiency values based on Walker, M. et al. 2013. and IEA report:
                            #https://iea-etsap.org/E-TechDS/PDF/I01-ind_boilers-GS-AD-gct.pdf
eboiler_efficiency = 0.99

mfg_blr_cty.loc[:,'eboiler_MMBtu'] = mfg_blr_cty.apply(
            lambda x: x['MMBtu'] * blr_efficiency[x['MECS_FT']] / eboiler_efficiency, axis=1 )




# sum the fuel energy by naics and fuel type

boiler_naics_fuel = mfg_blr_cty.groupby(['naics_sub','MECS_FT']).agg(
    {'MMBtu':'sum','eboiler_MMBtu':'sum'}).reset_index()

# convert MMBtu to TBtu

boiler_naics_fuel['eboiler_MMBtu'] = boiler_naics_fuel.eboiler_MMBtu/(10**6)

boiler_naics_fuel['MMBtu'] = boiler_naics_fuel.MMBtu/(10**6)

boiler_naics_fuel.rename(columns={'MMBtu':'Boiler fuel demand (TBtu)',
                                        'eboiler_MMBtu':'Electrification potential (TBtu)'},inplace=True)

# rename naics codes with descriptions and reformat fuel types

boiler_naics_fuel['naics_sub'].replace({311:'311,Food',312:'312,Beverages',313:'313,Textile Mills',
                                       314:'313,Textile Products',315:'314,Apparel',316:'315,Leather Products',
                                       321:'321,Wood',322:'322,Paper',323:'323,Printing',
                                       324:'324,Refining',325:'325,Chemicals',326:'326,Plastics',
                                       327:'327,Minerals',331:'331,Primary Metals',
                                       332:'332,Fabricated Metals',333:'333,Machinery',
                                       334:'334,Electronic Products',335:'335,Electrical Components',
                                       336:'336,Transp. Eqmt',337:'337,Furniture',339:'339,Miscellaneous'},
                              inplace=True)
boiler_naics_fuel['MECS_FT'].replace({'Coke_and_breeze':'Coke & breeze','LPG_NGL':'LPG & NGL',
                                            'Natural_gas':'Natural gas','Residual_fuel_oil':'Residual fuel oil'},
                                          inplace=True)






# sum the electrification potential energy by naics and fuel type
eboiler_potential_naics = mfg_blr_cty.groupby(['naics_sub','MECS_FT'])['eboiler_MMBtu'].sum().reset_index()

# convert to TBtu
eboiler_potential_naics['eboiler_MMBtu'] = eboiler_potential_naics.eboiler_MMBtu/(10**6)

eboiler_potential_naics.rename(columns={'eboiler_MMBtu':'elec_pot_tbtu'},inplace=True)

# rename naics codes with descriptions
eboiler_potential_naics['naics_sub'].replace({311:'311,Food',312:'312,Beverages',313:'313,Textile Mills',
                                       314:'313,Textile Products',315:'314,Apparel',316:'315,Leather Products',
                                       321:'321,Wood',322:'322,Paper',323:'323,Printing',
                                       324:'324,Refining',325:'325,Chemicals',326:'326,Plastics',
                                       327:'327,Minerals',331:'331,Primary Metals',
                                       332:'332,Fabricated Metals',333:'333,Machinery',
                                       334:'334,Electronic Products',335:'335,Electrical Components',
                                       336:'336,Transp. Eqmt',337:'337,Furniture',339:'339,Miscellaneous'},
                              inplace=True)




# determine byproduct fuel use for relevant subsectors and subtract from electrification potential
# (assume that conventional boilers that use byproduct fuels wouldn't be replaced with electric boilers)

# Percentages of Other out of total fuel use -- Percentages of byproduct fuels from MECS Carbon Footprints 2014
#321, 86% is Other -- 40% black liquor, 17% biomass
#322, 57% is Other -- 40% black liquor, 17% biomass
#324, 92% is Other -- 43% still gas, 18% pet coke, (4% of Other is Other fuels) 
#325, 13% is Other -- 13% waste gas
#331, 67% is Other -- 27% blast furnace/coke oven gas, 17% coke and breeze
#336, 4% is Other -- 4% waste gas


byproduct_dict = {'321,Wood':{'Black liquor':0.40,'Biomass':0.17},
                  '322,Paper':{'Black liquor':0.40,'Biomass':0.17},
                  '324,Refining':{'Still gas':0.43,'Pet coke':0.18},
                  '325,Chemicals':{'Waste gas':0.13},
                  '331,Primary Metals':{'Blast furnace/coke oven gas':0.27},
                  '336,Transp. Eqmt':{'Waste gas':0.04}}

bypr_df = pd.DataFrame.from_dict(byproduct_dict,orient='index').reset_index().rename(columns={'index':'naics_sub'})

ep_with_bypr = eboiler_potential_naics.groupby('naics_sub')['elec_pot_tbtu'].sum().reset_index().merge(
    bypr_df,on='naics_sub',how='left')

ep_with_bypr.fillna(0,inplace=True)

# multiply percent of byproduct fuel use in boilers by boiler fuel consumption
ep_with_bypr.iloc[:,2:] = ep_with_bypr.iloc[:,2:].mul(ep_with_bypr['elec_pot_tbtu'],axis=0)

# subtract byproduct fuel use from calculated electrification potential
ep_with_bypr.loc[:,'elec_pot_tbtu'] = ep_with_bypr['elec_pot_tbtu'] - ep_with_bypr.iloc[:,2:].sum(axis=1)










#--- determine the additional fuel use required for boiler electrification


# get county-level electrification potential with byproduct fuels excluded
#   (assumes boilers using byproduct fuels are not electrified) 

mfg_blr_cty_excl_bypr = mfg_blr_cty.copy()

# Percentages of Other out of total fuel use -- Percentages of byproduct fuels from MECS Carbon Footprints 2014
#321, 86% is Other -- 40% black liquor, 17% biomass 
#322, 59% is Other -- 40% black liquor, 17% biomass
#324, 92% is Other -- 43% still gas, 18% pet coke, (4% of Other is Other fuels) 
#325, 13% is Other -- 13% waste gas
#331, 67% is Other -- 27% blast furnace/coke oven gas, 17% coke and breeze
#336, 4% is Other -- 4% waste gas

# take electrification potential and subtract the portion that is byproduct fuels from relevant subsectors
mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==321),
                          'eboiler_MMBtu'] = mfg_blr_cty_excl_bypr['eboiler_MMBtu']*(1-1/0.86*0.57)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==322),
                          'eboiler_MMBtu'] = mfg_blr_cty_excl_bypr['eboiler_MMBtu']*(1-1/0.59*0.57)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==324),
                          'eboiler_MMBtu'] = mfg_blr_cty_excl_bypr['eboiler_MMBtu']*(1-1/0.92*0.61)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==325),
                          'eboiler_MMBtu'] = mfg_blr_cty_excl_bypr['eboiler_MMBtu']*(1-1/0.13*0.13)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==331),
                          'eboiler_MMBtu'] = mfg_blr_cty_excl_bypr['eboiler_MMBtu']*(1-1/0.67*0.27)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==336),
                          'eboiler_MMBtu'] = mfg_blr_cty_excl_bypr['eboiler_MMBtu']*(1-1/0.04*0.04)

#---------------------------------------------------
mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==321),
                          'MMBtu'] = mfg_blr_cty_excl_bypr['MMBtu']*(1-1/0.86*0.57)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==322),
                          'MMBtu'] = mfg_blr_cty_excl_bypr['MMBtu']*(1-1/0.59*0.57)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==324),
                          'MMBtu'] = mfg_blr_cty_excl_bypr['MMBtu']*(1-1/0.92*0.61)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==325),
                          'MMBtu'] = mfg_blr_cty_excl_bypr['MMBtu']*(1-1/0.13*0.13)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==331),
                          'MMBtu'] = mfg_blr_cty_excl_bypr['MMBtu']*(1-1/0.67*0.27)

mfg_blr_cty_excl_bypr.loc[(mfg_blr_cty_excl_bypr['MECS_FT']=='Other')&
                          (mfg_blr_cty_excl_bypr['naics_sub']==336),
                          'MMBtu'] = mfg_blr_cty_excl_bypr['MMBtu']*(1-1/0.04*0.04)



# calculate the fuel required for electricity based on regional power plant heat rates (fuel input/elec output) and grid mixes

electricity_methods = extra_fuels_for_elec.electricity()



county_electricity_use = pd.read_csv(r'../Solar-for-Industry-Process-Heat/tech_opp_tests/county_elec_estimates.csv.gzip',
                                     compression='gzip')

resource_mix = electricity_methods.calc_egrid_emissions_resource_mix()


electricity_fuel = electricity_methods.calculate_elect_fuel(resource_mix, mfg_blr_cty)
                                                                        #mfg_blr_cty_excl_bypr/mfg_blr_cty



# get the breakdown of fuel for electricity by county and their heat rates (MMBtu,fuel/MWh,elec) - keep only carbon fuels
fuel_mix = resource_mix.drop(columns=['Nuclear_hr','Hydro','Geothermal','Wind_hr',
                                      'Solar_hr','Wind','Hydro_hr','Solar',
                                      'Nuclear','Other_hr','Biomass_hr','Coal_hr','Oil_hr',
                                     'Other_fossil_hr','Natural_gas_hr'])

fuel_mix.rename(columns={'Natural_gas':'Natural_gas_mix', 'Coal':'Coal_mix','Other':'Other_mix',
                         'Oil':'Oil_mix','Biomass':'Biomass_mix','Other_fossil':'Other_fossil_mix'},inplace=True)




def get_fuels_from_elec(elec_fuel_df, fuel_mix_df):
    
    fuels_by_cty = pd.merge(elec_fuel_df,fuel_mix_df[['COUNTY_FIPS','Natural_gas_mix','Oil_mix','Coal_mix',
                                                     'Other_mix','Other_fossil_mix','Biomass_mix']],
                            on='COUNTY_FIPS',how='left'
                           )
    
    fuels_by_cty.loc[:,'Biomass_MMBtu'] = fuels_by_cty.apply(
        lambda x: x['Biomass_MMBtu']*x['Biomass_mix'],axis=1)
    fuels_by_cty.loc[:,'Coal_MMBtu'] = fuels_by_cty.apply(
        lambda x: x['Coal_MMBtu']*x['Coal_mix'],axis=1)
    fuels_by_cty.loc[:,'Other_MMBtu'] = fuels_by_cty.apply(
        lambda x: x['Other_MMBtu']*x['Other_mix'],axis=1)
    fuels_by_cty.loc[:,'Other_fossil_MMBtu'] = fuels_by_cty.apply(
        lambda x: x['Other_fossil_MMBtu']*x['Other_fossil_mix'],axis=1)
    fuels_by_cty.loc[:,'Oil_MMBtu'] = fuels_by_cty.apply(
        lambda x: x['Oil_MMBtu']*x['Oil_mix'],axis=1)
    fuels_by_cty.loc[:,'Natural_gas_MMBtu'] = fuels_by_cty.apply(
        lambda x: x['Natural_gas_MMBtu']*x['Natural_gas_mix'],axis=1)
    
    fuels_by_cty.drop(fuels_by_cty[fuels_by_cty['MMBtu'].isnull()].index, inplace = True)
    fuels_by_cty = fuels_by_cty.reset_index(drop=True)
    
    return fuels_by_cty
   


elec_grid_fuel = get_fuels_from_elec(electricity_fuel, fuel_mix)



# electrication potential (TBtu)
elec_grid_fuel.eboiler_MMBtu.sum()/(10**6)

# fuels required for boiler electrification (TBtu)
( elec_grid_fuel['Natural_gas_MMBtu'].sum() +
 elec_grid_fuel['Oil_MMBtu'].sum() +
 elec_grid_fuel['Coal_MMBtu'].sum())/(10**6)


# fuels required for boiler electrification (TBtu) - future grid,  AEO reference scenario
(elec_grid_fuel['Natural_gas_MMBtu'].sum()*(1+0.22) +
 elec_grid_fuel['Oil_MMBtu'].sum()*(1-0.65) +
 elec_grid_fuel['Coal_MMBtu'].sum()*(1-0.25))/(10**6)


# fuels required for boiler electrification (TBtu) - future grid, high renewables scenario
(elec_grid_fuel['Natural_gas_MMBtu'].sum()*(1-0.40) +
 elec_grid_fuel['Oil_MMBtu'].sum()*(1-0.80) +
 elec_grid_fuel['Coal_MMBtu'].sum()*(1-0.30))/(10**6)





def get_ghg_emissions(elec_grid_fuels_df, grid_scenario):
    
    ghgEmissFactor = {'Biomass':5.60, 'Coal':99.98, 'Coke_and_breeze':124.16, 'Diesel':88.28, 'LPG_NGL':78.54 ,
                     'Natural_gas':63.48, 'Petroleum_coke':112.90, 'Residual_fuel_oil':82.98,
                     'Waste_gas':75.20, 'Waste_oils_tars_waste_materials':83.67,'Other':69.34}
    
                    # File "GREET_emissions_factors.xlsx" shows the combustion and upstream fuel cycle emissions factors
                    # combustion emissions from:
                    # https://www.epa.gov/sites/default/files/2021-04/documents/emission-factors_apr2021.pdf
                    # kg CO2e/MMBtu
                    # upstream fuel cycle emissions from:GREET
                    # Assumptions: Biomass = forest reisdues; Coal = Mixed (industrial sector);
                    # Residual fuel oil = No. 5; Waste_oils_tars_waste_materials = Used oil; Diesel = heavy gas oils
                    # Waste gas = Fuel gas; Other = Average of Waste_gas,Waste_oils_tars,Biomass (upstream only)
                    
        
    if grid_scenario == 'current':
        perc_chg= {'Coal':0,'Natural_gas':0,'Oil':0,'Biomass':0}
        
    elif grid_scenario == 'reference':
        perc_chg = {'Coal':-0.25,'Natural_gas':0.22,'Oil':-0.65}
    
    elif grid_scenario == 'high_renewables':
        perc_chg = {'Coal':-0.40,'Natural_gas':-0.3,'Oil':-0.8} #-0.40,-0.30,-0.80
    
    
    ghg_df = elec_grid_fuels_df.copy()
    
    for i in list(ghg_df.index.values):
        ghg_df.loc[i,'GHG_savings(kg CO2e)'] = ghg_df['MMBtu'][i]*ghgEmissFactor[ghg_df['MECS_FT'][i]]
        
    for i in list(ghg_df.index.values):
        ghg_df.loc[i,'GHG_elec_emissions'] = (ghg_df['Coal_MMBtu'][i]*ghgEmissFactor['Coal']*(1+perc_chg['Coal']) +
                                              ghg_df['Oil_MMBtu'][i]*ghgEmissFactor['Residual_fuel_oil']*(1+perc_chg['Oil']) +
                                              ghg_df['Natural_gas_MMBtu'][i]*ghgEmissFactor['Natural_gas']*(1+perc_chg['Natural_gas'])
                                              )
                                            #ghg_df['Biomass_MMBtu'][i]*ghgEmissFactor['Biomass']*(1+perc_chg['Biomass'])
    
    return ghg_df



# GHG emissions from boiler electrification

elec_grid_ghg_current = get_ghg_emissions(elec_grid_fuel,'current')
#elec_grid_ghg_ref = get_ghg_emissions(elec_grid_fuel,'reference')
#elec_grid_ghg_high_ren = get_ghg_emissions(elec_grid_fuel,'high_renewables')




def get_ghg_savings(elec_grid_ghg_df):
    
    # groupby COUNTY_FIPS or naics_sub
    ghg_savings = elec_grid_ghg_df.groupby('COUNTY_FIPS').agg({'GHG_savings(kg CO2e)':'sum','GHG_elec_emissions':'sum'})
    
    ghg_savings = ghg_savings.divide(10**3).rename(columns={'GHG_savings(kg CO2e)':'GHG_savings(mt CO2e)'})
    
    ghg_savings.loc[:,'Total_GHG_emissions'] = ghg_savings['GHG_elec_emissions'] - ghg_savings['GHG_savings(mt CO2e)']
    
    #ghg_savings['Total_GHG_savings'] = ghg_savings['Total_GHG_savings']/(10**6) #mt to MMmt
    
    return ghg_savings



# Net change in GHG emissions (GHG emissions from boiler electrification - GHG avoided from conventional boilers)

ghg_current = get_ghg_savings(elec_grid_ghg_current)
#ghg_ref = get_ghg_savings(elec_grid_ghg_ref)
#ghg_high_ren = get_ghg_savings(elec_grid_ghg_high_ren)

# Net change in GHG emissions (MMmt)
ghg_current.sum()/(10**6)




