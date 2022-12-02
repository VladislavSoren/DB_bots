import pandas as pd
import numpy as np
import re
from datetime import datetime

import sqlalchemy as sql
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine.url import URL

import psycopg2
from psycopg2 import OperationalError
from contextlib import closing

import warnings
warnings.filterwarnings('ignore')

import logging

logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")


# Класс пользовательских исключений
class AuthError(Exception): pass


# Функция создания движка
def cr_eng(name_BD, user, password, host, port):
    '''
    inputs:
        name_BD - имя БД
        user - пользователь, создавший БД
        password - пароль пользователя
        port - порт

    outputs:
        engine - движок
    '''

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{name_BD}')
    return engine


# Функция создания связи с БД
def create_connection(name_BD, user, password, host, port):
    '''
    inputs:
        name_BD - имя БД
        user - пользователь, создавший БД
        password - пароль пользователя
        host - хост
        port - порт

    outputs:
        engine - движок
    '''
    connection = None

    try:
        connection = psycopg2.connect(
            database=name_BD,
            user=user,
            password=password,
            host=host,
            port=port,
        )
        #		print(type(connection))
        #		print("Connection to PostgreSQL DB successful")

        return connection

    except OperationalError as e:
        print(f"The error '{e}' occurred")


# Функция получения таблицы из базы в виде кортежей
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None

    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result

    except OperationalError as e:
        print(f"The error '{e}' occurred")


# Функция занесения таблицы в БД
def create_table(name_BD, name_table, table, user, password, host, port):
    # создаём движок
    engine = cr_eng(name_BD, user, password, host, port)

    # устанавливаем соединение на время создания таблицы
    with engine.connect() as connection:
        table.to_sql(name_table, engine, index=True)  # создание таблицы с именем  test и содержимым data


# Функция получения таблицы из базы в виде пандас таблицы

def get_table(name_BD, name_table, user, password, host, port):
    connection = create_connection(name_BD, user, password, host, port)
    table = pd.io.sql.read_sql(f"SELECT * FROM {name_table} order by index", connection)
    connection.close()
    return table


# Функция получения кол-ва строк таблицы
def get_table_size(name_BD, name_table, user, password, host, port):
    con = create_connection(name_BD, user, password, host, port)

    sql = f'SELECT count(*) FROM {name_table};'

    with closing(con) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            for row in cursor:
                return row


# Функция получения названий столбцов таблицы
def get_col_names(name_BD, name_table, user, password, host, port):
    con = create_connection(name_BD, user, password, host, port)

    sql = f'''SELECT column_name FROM information_schema.columns WHERE table_name = '{name_table}' ORDER BY ordinal_position;'''

    names_list = []

    with closing(con) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            for row in cursor:
                names_list.append(row[0])

    return names_list


# Функция удаления строк из БД по интервалу
def drop_inerval(name_BD, name_table, user, password, host, port, start_index, end_index):
	# Удаляем значения в таблице по индексам
	con = create_connection(name_BD, user, password, host, port)
	sql = f'DELETE FROM {name_table} WHERE index BETWEEN {start_index} AND {end_index};' # чем заполнять пустые значения?
#	print(sql)
	with closing(con) as conn:
		with conn.cursor() as cursor:
			cursor.execute(sql)
			# фиксируем изменеия *ВАЖНО!
			con.commit()


# Функция обновления названий столбцов новой таблицы на актуальные из таблицы в БД
def check(names_old, names_new, df):
    df_new = df.copy()
    count = 0

    for name_n in names_new:
        for name_o in names_old:
            if name_n.strip().lower() == name_o.lower():
                df_new.rename(columns={name_n: name_o}, inplace=True)
                count += 1

    if len(names_new) == count:
        return df_new, True
    else:
        return df_new, False


# Функция присоединения новой таблицы к таблице в БД
def insert_target(name_BD, name_table, user, password, host, port, df_DB):
    
    df_DB = df_DB.copy()
    
    # assert df_DB.isna().sum().sum() == 0, 'Загружаемая в базу таблица содержит NaN!'
    if df_DB.isna().sum().sum() != 0:
        raise AuthError('Загружаемая в базу таблица содержит NaN!')

    # подключемся к БД
    con = create_connection(name_BD, user, password, host, port)

    # создаём курсор для взамодействия с базой
    with closing(con) as conn:
        with conn.cursor() as cursor:

            # получаем названия стобцов таблицы из базы! (исключаем колонку index!!!)
            col_names = get_col_names(name_BD, name_table, user, password, host, port)[1:]

            # Актуализируем названия столбцов новой таблицы (+ исключаем некоторые ошибки в названиях)
            names_new = list(df_DB.columns)
            df_DB = check(col_names, names_new, df_DB)[0]

            # ставим столбцы в том же порядке, что и в базе
            df_DB = df_DB[col_names]

            # получаем кол-во строк таблицы
            l1 = get_table_size(name_BD, name_table, user, password, host, port)[0]

            # получаем массив индексов для верного заполнения столбца INDEX
            indexes = np.arange(l1, l1 + len(df_DB), 1)

            # проходимся по всем строкам новой таблицы (которую надо загрузить в БД)
            for i, old_row in enumerate(df_DB.values):  #

                new_row = []
                for val in old_row:
                    if type(val) == pd._libs.tslibs.timestamps.Timestamp:
                        new_row.append(str(val))
                    else:
                        new_row.append(val)

                new_row = tuple(new_row)
                new_row = (indexes[i],) + new_row
                sql = f'INSERT INTO {name_table} VALUES {new_row};'

                try:
                    cursor.execute(sql)
                except Exception as e:
                    logging.exception(e)
                    logging.info(col_names)
                    logging.info(new_row)
                    print('!.............................!')
                    print(e)
                    print(i, '- индекс строки')
                    print(col_names, '- столбцы')
                    print(new_row, '- значения в строке')

                    break

                # фиксируем изменеия *ВАЖНО!
                #                con.commit()
                if i % 2000 == 0:
                    print("Records inserted successfully")
            con.commit()  # !!!


# Функция создания нового столбца
def add_column(name_BD, name_table, user, password, host, port, name_column):
    con = create_connection(name_BD, user, password, host, port)

    sql = f'ALTER TABLE {name_table} ADD COLUMN "{name_column}" text;'  # чем заполнять пустые значения?

    with closing(con) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            # фиксируем изменеия *ВАЖНО!
            con.commit()


# Функция изменения ВСЕХ значений столбца
def update_values(name_BD, name_table, user, password, host, port, name_column, value):
    con = create_connection(name_BD, user, password, host, port)

    sql = f'UPDATE {name_table} SET "{name_column}" = {value};'

    #	print(sql)
    with closing(con) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            # фиксируем изменеия *ВАЖНО!
            con.commit()