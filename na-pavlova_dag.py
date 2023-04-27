# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import os
from dotenv import load_dotenv

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


load_dotenv()
connection = os.getenv('CONNECTION_DB_SIM')
connection_test = os.getenv('CONNECTION_DB_TEST')


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.pavlova',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 19),
}

# Интервал запуска DAG
schedule_interval = '0 7 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def npavlova_dag():

    @task()
    def extract_feed_actions():
        query = """
        SELECT   user_id,
                age,
                gender,
                os,
                CountIf(action, action = 'like') as likes,
                CountIf(action, action = 'view') as views
        FROM    simulator_20230320.feed_actions
        WHERE   toDate(time) = today() - 1
        GROUP BY user_id, age, gender, os
        """
        df_feed = ph.read_clickhouse(query, connection=connection)
        return df_feed
        
    @task()
    def extract_message_actions():
        query = """
        SELECT  DISTINCT t4.user_id AS user_id,
                age,
                gender,
                os,
                messages_sent,
                users_sent,
                messages_received,
                users_received
          FROM 
            (SELECT user_id,
                    uniq(reciever_id) AS users_sent,
                    count(reciever_id) AS messages_sent
            FROM simulator_20230320.message_actions
            WHERE toDate(time) = today() - 1 
            GROUP BY user_id) t1 
          LEFT JOIN
            (SELECT reciever_id,
                    uniq(user_id) AS users_received,
                    count(user_id) AS messages_received
            FROM simulator_20230320.message_actions
            WHERE toDate(time) = today() - 1 
            GROUP BY reciever_id) t2
          ON  t1.user_id = t2.reciever_id
          LEFT JOIN simulator_20230320.message_actions AS t4
          ON  t1.user_id = t4.user_id 
            OR t2.reciever_id = t4.user_id
        """
        df_message = ph.read_clickhouse(query, connection=connection)
        return  df_message
    @task()
    def merge_table(df_feed,df_message):
        df = df_feed.merge(df_message, on='user_id', how='outer')
        df['age_x'] = df['age_x'].fillna(df.loc[df.age_x.isna()].age_y)
        df['gender_x'] = df['gender_x'].fillna(df.loc[df.gender_x.isna()].gender_y)
        df['os_x'] = df['os_x'].fillna(df.loc[df.os_x.isna()].os_y)
        df =  df.drop(columns=['age_y', 
                               'gender_y', 
                               'os_y'])\
                .rename(columns={'age_x': 'age',
                                 'gender_x': 'gender', 
                                 'os_x': 'os'})\
                .fillna(0)
        df = df.astype({'age': 'int64', 
                    'gender': 'int64', 
                    'likes':'int64', 
                    'views':'int64', 
                    'messages_sent':'int64', 
                    'users_sent':'int64', 
                    'messages_received':'int64', 
                    'users_received':'int64'})

        return df
    
    @task()
    def split_by_os(df):    
        df_os =  df.groupby('os', as_index=False)\
                  .agg({'likes':'sum',
                        'views':'sum',
                        'messages_sent':'sum',
                        'users_sent':'sum',
                        'messages_received':'sum',
                        'users_received':'sum'})\
                  .rename(columns={'os':'dimension_value'})
        df_os.insert(0, 'dimension', 'os')
        df_os.insert(0, 'event_date', datetime.now().date() - timedelta(days = 1))
        return df_os
    @task()
    def split_by_age(df):    
        df_age =  df.groupby('age', as_index=False)\
              .agg({'likes':'sum',
                    'views':'sum',
                    'messages_sent':'sum',
                    'users_sent':'sum',
                    'messages_received':'sum',
                    'users_received':'sum'})\
              .rename(columns={'age':'dimension_value'})
        df_age.insert(0, 'dimension', 'age')
        df_age.insert(0, 'event_date', datetime.now().date() - timedelta(days = 1))
        return df_age

    @task()
    def split_by_gender(df):    
        df_gender =  df.groupby('gender', as_index=False)\
              .agg({'likes':'sum',
                    'views':'sum',
                    'messages_sent':'sum',
                    'users_sent':'sum',
                    'messages_received':'sum',
                    'users_received':'sum'})\
              .rename(columns={'gender':'dimension_value'})
        df_gender.insert(0, 'dimension', 'gender')
        df_gender.insert(0, 'event_date', datetime.now().date() - timedelta(days = 1))
        return df_gender

    
    @task()
    def concat_df(df_os, df_age, df_gender):
        df_concat = pd.concat([df_os, df_age, df_gender], axis=0)
        return df_concat
    
    
    @task()
    def load(df_concat):
        query_create_table = f"""
            CREATE TABLE IF NOT EXISTS test.na_pavlova
            (
                event_date Date,
                dimension String,
                dimension_value String,
                likes UInt64,
                views UInt64,
                messages_sent UInt64,
                messages_received UInt64,
                users_sent UInt64,
                users_received UInt64
                

            ) ENGINE = MergeTree()
            PRIMARY KEY (event_date, dimension, dimension_value)
        """


        ph.execute(query_create_table, connection=connection_test)
        ph.to_clickhouse(df_concat, 'na_pavlova', index=False, connection=connection_test)

    df_feed = extract_feed_actions()
    df_message = extract_message_actions()
    df = merge_table(df_feed,df_message)
    df_os = split_by_os(df)
    df_age = split_by_age(df)
    df_gender = split_by_gender(df)
    df_concat = concat_df(df_os, df_age, df_gender)
    load(df_concat)
npavlova_dag = npavlova_dag()
