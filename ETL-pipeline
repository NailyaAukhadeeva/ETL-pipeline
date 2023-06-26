# coding=utf-8 

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для CH
def ch_get_df(query='Select 1', host='https://clickhouse.lab.karpov.courses', user='student', password='dpo_python_2020'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result


query = """SELECT 
               toDate(time) as event_date, 
               country, 
               source,
               count() as likes
            FROM 
                simulator_20230520.feed_actions 
            where 
                toDate(time) = '2023-05-26' 
                and action = 'like'
            group by
                event_date,
                country,
                source
            format TSVWithNames"""

#Запрос на создание таблицы в схеме test
create_table = """
        CREATE TABLE IF NOT EXISTS test.aukhadeeva_n
        (
            event_date DATE,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_received UInt64,
            messages_sent UInt64,
            users_received UInt64,
            users_sent UInt64   
        )
        ENGINE = MergeTree()
        Order by event_date"""
    
# Подключение к схеме test
connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.aukhadeeva',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 10),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_n_auhadeeva_etl():
    
    #Загружаем данные с ленты и сообщений
    @task()
    def extract_feed_actions():
        query = """SELECT 
                        user_id,
                        countIf(action = 'like')  as likes,
                        countIf(action = 'view')  as views,
                        gender,
                        age,
                        os,
                        toDate(time) as event_date
                    FROM simulator_20230520.feed_actions 
                    WHERE toDate(time) = today() - 1
                    GROUP BY user_id, gender, age, os, event_date
                    format TSVWithNames"""
        df_feed_actions = ch_get_df(query=query)
        return df_feed_actions
    @task()
    def extract_message_actions():
        query = """
            SELECT
              today() - 1 as event_date,
              user_id,
              messages_sent,
              users_sent,
              messages_received,
              users_received,
              gender,
              age,
              os
            FROM 
            (SELECT
              user_id,
              COUNT(reciever_id) as messages_sent,
              COUNT(DISTINCT reciever_id) as users_sent,
              gender,
              age,
              os
            FROM simulator_20230520.message_actions
            WHERE toDate(time) = today() - 1
            GROUP BY user_id, gender, age, os)t1
            LEFT JOIN 
            (SELECT
              reciever_id,
              COUNT(user_id) as messages_received,
              COUNT(DISTINCT user_id) as users_received
            FROM simulator_20230520.message_actions
            WHERE toDate(time) = today() - 1
            GROUP BY reciever_id
            ) t2
            ON t1.user_id = t2.reciever_id
            format TSVWithNames
            """
        df_message_actions = ch_get_df(query=query)
        return df_message_actions
    
 #Объединяем таблицы
    @task
    def merge_data(df_feed_actions, df_message_actions):
        full_df = df_feed_actions.merge(df_message_actions, how = 'outer', on = ['user_id', 'event_date', 'gender', 'os', 'age']).fillna(0)
        return full_df
    
 # срез по полу
    @task
    def transform_gender(full_df):
        gender_df = full_df[['gender', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received', 'event_date']] \
                            .groupby('gender', as_index = False) \
                            .sum().rename(columns={'gender':'dimension_value'}) 
        gender_df['dimension'] = 'gender'
        return gender_df         
    
        
 # срез по возрасту
    @task
    def transform_age(full_df):
        age_df = full_df[['age', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received', 'event_date']] \
                        .groupby('age', as_index = False) \
                        .sum().rename(columns={'age':'dimension_value'}) 
        age_df['dimension'] = 'age'
        return age_df
    
 # срез по ос
    @task
    def transform_os(full_df):
        os_df = full_df[['os', 'likes', 'views', 'messages_sent', 'users_sent', 'messages_received', 'users_received', 'event_date']] \
                        .groupby('os', as_index = False) \
                        .sum().rename(columns={'os':'dimension_value'}) 
        os_df['dimension'] = 'os'
        return os_df

 # объединение срезова
    @task
    def contact_dimension(age_df, gender_df, os_df):
        df_all_dimension = pd.concat([age_df, gender_df, os_df], ignore_index=True)    
        return df_all_dimension

 # выгружаем в отдельную таблицу в ClickHouse
    @task
    def load(result):
        ph.execute(query=create_table, connection=connection_test)
        ph.to_clickhouse(df = result, table = 'aukhadeeva_n', connection=connection_test, index=False)
        
        
#запуск тасков последовательно    
    df_feed_actions = extract_feed_actions()
    df_message_actions = extract_message_actions()
    full_df = merge_data(df_feed_actions, df_message_actions)
    os_df = transform_os(full_df)
    gender_df = transform_gender(full_df)
    age_df = transform_age(full_df)
    df_all_dimension = contact_dimension(age_df, gender_df, os_df) 
    load(df_all_dimension)
        
    
dag_n_auhadeeva_etl = dag_n_auhadeeva_etl()

