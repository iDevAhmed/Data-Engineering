import requests
import json

from airflow import DAG
import numpy as np
from datetime import datetime
from datetime import date
# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

import pandas as pd


# step 2 - define default args
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 13)
    }

# step 3 - instantiate DAG
dag = DAG(
    'happiness-DAG-test',    
    default_args=default_args,
    description='Fetch dataset',
    schedule_interval='@once',
)


# step 4 Define tasks
def load_data(**kwargs):
    df_happy_2015 = pd.read_csv("/root/airflow/dags/2015.csv")
    df_happy_2015.columns=df_happy_2015.columns.str.replace('[), (, ., \s]','')

    df_happy_2016 = pd.read_csv("/root/airflow/dags/2016.csv")
    df_happy_2016.columns=df_happy_2016.columns.str.replace('[), (, ., \s]','')

    df_happy_2017 = pd.read_csv("/root/airflow/dags/2017.csv")
    df_happy_2017.columns=df_happy_2017.columns.str.replace('[), (, ., \s]','')

    df_happy_2018 = pd.read_csv("/root/airflow/dags/2018.csv")
    df_happy_2018.columns=df_happy_2018.columns.str.replace('[), (, ., \s]','')
    df_happy_2018 = df_happy_2018.rename(columns={"Overallrank":"HappinessRank", "Countryorregion":"Region","Score":"HappinessScore", "GDPpercapita":"EconomyGDPperCapita", "Healthylifeexpectancy":"HealthLifeExpectancy", "Freedomtomakelifechoices":"Freedom", "Perceptionsofcorruption":"TrustGovernmentCorruption"})

    df_happy_2019 = pd.read_csv("/root/airflow/dags/2019.csv")
    df_happy_2019.columns=df_happy_2019.columns.str.replace('[), (, ., \s]','')
    df_happy_2019 = df_happy_2019.rename(columns={"Overallrank":"HappinessRank", "Countryorregion":"Region","Score":"HappinessScore", "GDPpercapita":"EconomyGDPperCapita", "Healthylifeexpectancy":"HealthLifeExpectancy", "Freedomtomakelifechoices":"Freedom", "Perceptionsofcorruption":"TrustGovernmentCorruption"})
    
    df_happy_2015["Socialsupport"] = (df_happy_2018["Socialsupport"] + df_happy_2019["Socialsupport"])/2
    df_happy_2016["Socialsupport"] = (df_happy_2018["Socialsupport"] + df_happy_2019["Socialsupport"])/2
    df_happy_2017["Socialsupport"] = (df_happy_2018["Socialsupport"] + df_happy_2019["Socialsupport"])/2
    df_happy_2015 = pd.concat([df_happy_2015, df_happy_2016, df_happy_2017, df_happy_2018, df_happy_2019]).groupby("Country").mean()

    df_happy_2015 = df_happy_2015.drop(columns=["HappinessRank","StandardError","LowerConfidenceInterval", "UpperConfidenceInterval", "Whiskerhigh", "Whiskerlow"])
    df_happy_2015.reset_index(level=0, inplace=True)
    df_happy_2015 = df_happy_2015.to_json()
    return df_happy_2015

def load_data_2(**kwargs):
    df_250_countries = pd.read_csv("/root/airflow/dags/250 Country Data.csv", index_col=0)
    df_250_countries.columns=df_250_countries.columns.str.replace('[., \s]','')
    df_250_countries = df_250_countries.to_json()

    return df_250_countries

def load_data_3(**kwargs):
    df_life_expectancy = pd.read_csv("/root/airflow/dags/Life Expectancy Data.csv")
    df_life_expectancy.columns=df_life_expectancy.columns.str.replace('[., \s]','')
    df_life_expectancy = df_life_expectancy.to_json()
    
    return df_life_expectancy

def integrate_data(**context):
    df = context['task_instance'].xcom_pull(task_ids='load_data')
    df_new = pd.DataFrame(json.loads(df))
    df_new.to_csv("/root/airflow/dags/mydataset.csv")    

def research_df(**context):
    df_happy_2015 = context['task_instance'].xcom_pull(task_ids='load_data')
    research_df = context['task_instance'].xcom_pull(task_ids='load_data_2')

    df_happy_2015 = pd.DataFrame(json.loads(df_happy_2015))
    research_df = pd.DataFrame(json.loads(research_df))

    research_df = research_df.drop(columns=["gini","RealGrowthRating(%)", "LiteracyRate(%)", "Inflation(%)"])
    research_df = research_df.rename(columns={"name":"Country", "region":"Region","population":"Population","area":"Area","subregion":"Subregion"})
    research_df["Unemployement(%)"] = research_df["Unemployement(%)"].str.split('%').str.get(0)
    region_df = research_df[pd.to_numeric(research_df["Unemployement(%)"], errors='coerce').notnull()]
    region_df_copy = region_df.copy()
    region_df_copy["Unemployement(%)"] = region_df_copy["Unemployement(%)"].astype(float)
    region_df_copy = region_df_copy.groupby(["Region"])["Unemployement(%)"].mean()
    research_df["Unemployement(%)"] = research_df.apply(
        lambda row: row["Unemployement(%)"] if ''.join(filter(str.isalnum, str(row["Unemployement(%)"]))).isnumeric() else region_df_copy.get(str(row["Region"])),
        axis=1)
    for i in range(len(research_df["Unemployement(%)"])):
        # try:
        if (research_df["Unemployement(%)"][i] == "10.9."):
            research_df["Unemployement(%)"][i] = research_df["Unemployement(%)"][i][0:-1]


    research_df = pd.merge(df_happy_2015, research_df, how='inner', on = 'Country')
    research_df = research_df.drop(columns=["Family", "DystopiaResidual"])
    research_df.index = np.arange(1, len(research_df) + 1)
    research_df = research_df.to_json()
    return research_df

def impute_by_mean(df,column):
    frames = []
    for i in list(set(df['Country'])):
        df_country = df[df['Country']== i]
        if len(df_country) > 1:    
            df_country[column].fillna(df_country[column].mean(),inplace = True)        
        else:
            df_country[column].fillna(df[column].mean(),inplace = True)
        frames.append(df_country)    
        final_df = pd.concat(frames)
    return final_df

def impute_df(**context):
    df_life_expectancy = context['task_instance'].xcom_pull(task_ids='load_data_3')
    df_life_expectancy = pd.DataFrame(json.loads(df_life_expectancy))
    df_alcohol_imputed = impute_by_mean(df_life_expectancy, "Alcohol")
    df_alcohol_imputed.sort_values(by=['Country'])
    df_life_expectancy = df_alcohol_imputed.copy()
    df_life_expectancy = impute_by_mean(df_life_expectancy, 'HepatitisB')

    final_df_life_expectancy = df_life_expectancy.copy()
    final_df_life_expectancy = final_df_life_expectancy.drop(columns={"Status", "Year" , "GDP" , "percentageexpenditure" ,"Population", "Totalexpenditure" ,"Incomecompositionofresources", "thinness1-19years", "thinness5-9years"})
    final_df_life_expectancy = final_df_life_expectancy.groupby(['Country']).mean()
    final_df_life_expectancy = final_df_life_expectancy.to_json()
    return final_df_life_expectancy


def integrated_df(**context):
    research_df_final = context['task_instance'].xcom_pull(task_ids='research_df')
    final_df_life_expectancy = context['task_instance'].xcom_pull(task_ids='impute')

    research_df_final = pd.DataFrame(json.loads(research_df_final))
    final_df_life_expectancy = pd.DataFrame(json.loads(final_df_life_expectancy))
    
    research_df_final = research_df_final.reset_index(drop=True, inplace=True)
    final_df_life_expectancy = final_df_life_expectancy.reset_index(level=0, inplace=True)
    research_df_final = pd.merge(research_df_final ,final_df_life_expectancy ,how='inner', on = 'Country')
    research_df_final = research_df_final.sort_values(by='HappinessScore', ascending=False)

    research_df_final = research_df_final.to_json()
    return research_df_final

t1 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='store_data',
    python_callable=integrate_data,
    provide_context=True,
    dag=dag,
)

t5 = PythonOperator(
    task_id='load_data_3',
    python_callable=load_data_3,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data_2',
    python_callable=load_data_2,
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='research_df',
    python_callable=research_df,
    provide_context=True,
    dag=dag,
)

impute_task = PythonOperator(
    task_id='impute',
    python_callable=impute_df,
    provide_context=True,
    dag=dag,
)

final_dataset = PythonOperator(
    task_id='integrate',
    python_callable=integrated_df,
    provide_context=True,
    dag=dag,
)
# step 5 - define dependencies
t1, t3, t5 >> t2

t2 >> t4

t4 >> impute_task

impute_task >> final_dataset



