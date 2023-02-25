from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import time
import requests
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from selenium.webdriver.chrome.options import Options
from datetime import datetime


default_args = {
    'start_date': datetime(year=2023, month=2, day=25)
}

def extract():
        global df,df1
        options=Options()
        options.add_argument("headless")
        driver = webdriver.Chrome(options=options)
        url='https://www.laliga.com/en-GB/stats/laliga-santander/scorers/team/fc-barcelona'
        print('Openning....',url)
        driver.get(url)
        driver.maximize_window()
        time.sleep(20)
        npath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[1]/td[3]/a'
        gpath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[1]/td[5]/p'
        mpath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[1]/td[6]/p'
        gpm_path='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[1]/td[7]/p'
        p_name=[]
        p_goals=[]
        p_matches=[]
        p_gpm=[]

        for i in range(2,12):
                p_name.append(driver.find_element(By.XPATH,npath).text)
                p_goals.append(driver.find_element(By.XPATH,gpath).text)
                p_gpm.append(driver.find_element(By.XPATH,gpm_path).text)
                
                npath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[{}]/td[3]/a/p'.format(i)
                gpath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[{}]/td[5]/p'.format(i)
                gpm_path='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[{}]/td[7]/p'.format(i)
                
                
        score=dict(Name =p_name, Goals =p_goals,GPM=p_gpm)
        df = pd.DataFrame.from_dict(score)


        url1='https://www.laliga.com/en-GB/stats/laliga-santander/assists/team/fc-barcelona'
        print('Openning.....', url1)
        driver.get(url1)
        time.sleep(20)
        npath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[1]/td[3]/a/p'
        apath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[1]/td[5]/p'
        apm='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[1]/td[7]/p'
        p_name=[]
        p_assist=[]
        p_apm=[]

        for i in range(2,14):
                p_name.append(driver.find_element(By.XPATH,npath).text)
                p_assist.append(driver.find_element(By.XPATH,apath).text)
                p_apm.append(driver.find_element(By.XPATH,apm).text)

                npath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[{}]/td[3]/a/p'.format(i)
                apath='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[{}]/td[5]/p'.format(i)
                apm='/html/body/div[1]/div[6]/div[2]/div[2]/div/table/tbody/tr[{}]/td[7]/p'.format(i)

        assist=dict(Name =p_name, Assist =p_assist,APM=p_apm)
        driver.close()
        df1 = pd.DataFrame.from_dict(assist)


def load():
        with pd.ExcelWriter('/home/mnt/c/barca_stats.csv') as writer:
            df.to_excel(writer, sheet_name='GOALS',index=False)
            df1.to_excel(writer, sheet_name='ASSISTS',index=False)


with DAG(
    dag_id='etl_users',
    default_args=default_args,
    schedule_interval='@daily',
    description='ETL pipeline for processing users'
) as dag:

    # Task 1 - Fetch user data from LALIGA WEBSITE
    task_extract_users = PythonOperator(
        task_id='extract',
        python_callable=extract,
        
    )

    

    # Task 3 - Save users to CSV
    task_load_users = PythonOperator(
        task_id='load',
        python_callable=load,
        
    )

    task_extract_users >> task_load_users






        
        
        




