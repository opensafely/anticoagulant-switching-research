import pandas as pd
import os



summary = pd.Series(name="patient_count", dtype=int)

def count_missing(df, total, criteria):
    out =  total - df["patient_id"].count()
    total = df["patient_id"].count()
    summary[criteria] = ((out//5) * 5).astype(int)
    return total, summary

df = pd.read_csv('output/input_flow_chart.csv').fillna("0")

total = df["patient_id"].count()
summary["total registered patients"] = ((total//5) * 5).astype(total)

cols = df.drop("patient_id", axis=1).columns  

df = df.loc[(df['age_18_110']==1)]
total, _ = count_missing(df, total, "age >110 or <18")


df = df.loc[df['warfarin_last_three_months']!="0"]
total, _ = count_missing(df, total, "No warfarin")

df = df.loc[df['warfarin_6_months']!="0"]
total, _ = count_missing(df, total, "no warfarin 6 months ago")


df = df.loc[df['doac_last_three_months']!="0"]
total, _ = count_missing(df, total, "DOAC in last 3 months")


df = df.loc[(df['warfarin_next_three_months'] !="0") | (df['doac_next_three_months'] !="0")]
_ , summary = count_missing(df, total, "no warfarin or DOAC in follow up period")


#summary = 100*summary/summary["total registered patients"]
summary.to_csv(os.path.join("output","flow_chart_data.csv"))
print(summary)  