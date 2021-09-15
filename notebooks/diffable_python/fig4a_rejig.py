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

# +
import pandas as pd
import os
import sys
from IPython.display import display, Markdown

# import custom functions from 'lib' folder
sys.path.append('../lib/')
from functions import plot_line_chart

# +
dfp = pd.read_csv(os.path.join("..","output","inr_testing.csv"))

dfp["INR_month"] = pd.to_datetime(dfp["INR_month"])
dfp = dfp.set_index("INR_month")

dfp["No of patients with INR"] = 1000*dfp["patient_count"]/dfp["denominator"]
dfp["No of INRs"] = 1000*dfp["test_count"]/dfp["denominator"]

titles = ["(a) Number of patients with an INR test per month, and\n number of INRs, per thousand patients on Warfarin"]

plot_line_chart([dfp[["No of patients with INR","No of INRs"]]], titles, 
                ylabels={0:"Rate per 1000"}, filename="fig4a")
