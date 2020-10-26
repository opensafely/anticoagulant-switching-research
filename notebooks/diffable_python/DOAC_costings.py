# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: all
#     notebook_metadata_filter: all,-language_info
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.3
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Estimate of warfarin to DOAC switches

import pandas as pd
import numpy as np
import os
from ebmdatalab import bq, maps, charts

# ## Calculate DOAC annual cost

#create dataframe for DOAC costings
doacs = {'chemical': ['Edoxaban','Apixaban','Rivaroxaban','Dabigatran etexilate'],
         'cost_per_pack': [49,53.2,50.4,51],
         'pack_size': [28,56,28,60],
         'daily_doses': [1,2,1,2],
        }
df = pd.DataFrame(doacs) #create dataframe

#get proportion data from output from DOACs notebook
doac_df = pd.read_csv(os.path.join('..','output','doac_types.csv'))
#filter to use proportion of switching from March-May 2020
doac_df=doac_df.loc[(doac_df['year']==2020) & (doac_df['period']=='March-May')]
doac_df=doac_df.drop(['period', 'year'], axis=1)

doac_df.head()

doac_df = pd.merge(df,doac_df)

doac_df.head()

#calculate annualised cost
#first, calculate annual cost, including adjustment for actual cost (7.11% average reduction)
doac_df['annual_net_cost'] = 365 * 0.9289 *(doac_df['cost_per_pack']/(doac_df['pack_size']/doac_df['daily_doses']))
#create proportional cost for each DOAC, based on the proportion of uptake
doac_df['proportion_cost'] = doac_df['annual_net_cost']*(doac_df['%']/100)
#df.head()

#create DOAC cost by summing DOAC proportion weighting costs
doac_cost = doac_df['proportion_cost'].sum(axis=0)

print("Annual average actual cost of DOACs per patient: " + "£{:,.2f}".format(doac_cost))

# ## Annual costs per patient on warfarin (excluding liquids)

#Calculate overall cost for warfarin tablets for 3 months to end of Feb 2020 for TPP practices
sql='''
WITH #subquery, creating a list of TPP practices as per December 2018 (latest data available)
  tpp_practices AS (
  SELECT
    ODS
  FROM
    hscic.vendors
  WHERE
    Principal_Supplier = 'TPP'
    AND date = '2018-12-01')
SELECT
  SUM(actual_cost) AS actual_cost
FROM
  hscic.normalised_prescribing AS rx
INNER JOIN
  tpp_practices AS tpp
ON
  tpp.ODS = rx.practice
WHERE
  bnf_code LIKE '0208020V0%' #warfarin
  AND bnf_name LIKE '%tab%' #tablets only, excluding liquids
  AND month BETWEEN '2019-12-01'
  AND '2020-02-01'
'''
warf_df = bq.cached_read(sql, csv_path=os.path.join('..','output','warf_df.csv'))
warf_cost = warf_df['actual_cost'].sum(axis=0) # create single line variable
print("Cost of warfarin tablets Dec 2019 - Feb 2020: " + "£{:,.2f}".format(warf_cost))

# ### Calculate cost of switch

#get switch data from outputs from switch notebook
switch_df = pd.read_csv(os.path.join('..','output','doac_switchers.csv'))

switch_df.head()

switch_doac_df=switch_df.loc[(switch_df['year']==2020) & (switch_df['period']=='March-May')]

switch_doac_df.head()

print("Annual average actual cost of DOACs per patient: " + "£{:,.2f}".format(noac_cost))#cost_per_patient for warfarin
warfarin_pts = 1000* switch_doac_df['baseline warfarin patients (thousands)'].sum(axis=0) #from DOAC switch df
#calculate annual costs by multiplying by 4
warf_cost_per_patient = 4 * warf_cost/warfarin_pts
print("Annual cost per warfarin patient: " + "£{:,.2f}".format(warf_cost_per_patient))
#estimated cost difference between NOAC and warfarin
doac_diff =noac_cost - warf_cost_per_patient
print("Annual drug cost difference per patient from switch from warfarin to DOAC: ""£{:,.2f}".format(doac_diff))
#switch costs for TPP population
switched_pts = 1000* switch_doac_df['switched (thousands)'].sum(axis=0) # from DOAC switch df
print("Number of patients switched: " + str(switched_pts))
tpp_switch_costs = switched_pts * doac_diff
print("Estimated annual cost difference for switch from warfarin to DOAC in TPP practices: " + "£{:,.2f}".format(tpp_switch_costs))
#calculate proportion of patients at TPP practice (as per December 2018)
#use BQ to get list size (as per December 2018)
sql='''
select sum(total_list_size) as list_size, # calculate all practices list size
sum(case when vendors.Principal_Supplier = 'TPP' then total_list_size else 0 end) as tpp_list_size # calculate TPP practices list size
from hscic.practice_statistics as stats
inner join
hscic.vendors as vendors
on
date(stats.month) = vendors.Date
and
stats.practice = vendors.ODS
where
stats.month = '2018-12-01' # latest available date
'''
tpp_df = bq.cached_read(sql, csv_path=os.path.join('..','output','tpp_df.csv'))
#calculate TPP proportion
prop_tpp = tpp_df['tpp_list_size'].sum(axis=0)/tpp_df['list_size'].sum(axis=0)
print("Proportion of patients in England registered at TPP practice: " + "{:.2%}".format(prop_tpp))
#Using percentage TPP coverage, national costs:
national_switch_costs = (tpp_switch_costs/prop_tpp)
print("Estimated annual cost different for switch from warfarin to DOAC in England: " + "£{:,.2f}".format(national_switch_costs))


