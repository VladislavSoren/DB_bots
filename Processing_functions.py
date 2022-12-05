from datequarter import DateQuarter
import datetime

import subprocess
import os
from  DB_functions import *
from urllib.parse import urlparse as url

from google.oauth2.service_account import Credentials
from googleapiclient import discovery
import warnings
warnings.filterwarnings('ignore')

import logging

logging.basicConfig(level=logging.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

'''
if vybor_bazy == 'vlad':

    table_choose = st.selectbox('Выберите таблицу', ('all_data', 'pivot',
                                                     'data_inf', 'data_pod', 'data_soc', 'inf_pod_soc', 'regions_scor',
                                                     'budget',
                                                     'kpi_cpc', 'kpi_cpf', 'kpi_year', 'gov_publics'), index=0)

    if table_choose is not None:
        baza = 'vlad'
        table = table_choose
        user = 'sidorov'
        password = '13Password64'
        host = '10.128.0.20'
        port = '6432'
'''

# %load /media/sidorov/dev/NoteProjects/Gospublics/reg_dict.py
# Унифицированный словарь регионов
base_names = {
    'Сириус': ['ЦУР_Сириус', 'сириус', 'Сириус'],
    'Республика Адыгея': ['адыгея'],
    'Республика Алтай': ['алтай'],
    'Алтайский Край': ['алтайский'],
    'Амурская область': ['амурская'],
    'Архангельская область': ['архангельская'],
    'Астраханская область': ['астраханская'],
    'Республика Башкортостан': ['башкортостан'],
    'Белгородская область': ['белгородская'],
    'Брянская область': ['брянская'],
    'Республика Бурятия': ['бурятия'],
    'Владимирская область': ['владимирская'],
    'Волгоградская область': ['волгоградская'],
    'Вологодская область': ['вологодская'],
    'Воронежская область': ['воронежская'],
    'Республика Дагестан': ['дагестан'],
    'Еврейская АО': ['еврейская', 'еао'],

    'Забайкальский Край': ['забайкальский'],
    'Ивановская область': ['ивановская'],
    'Республика Ингушетия': ['ингушетия'],
    'Иркутская область': ['иркутская'],
    'Кабардино-Балкарская Республика': ['кабардино_балкарская', 'кабардино_балкария'],
    'Калининградская область': ['калининградская'],
    'Республика Калмыкия': ['калмыкия'],
    'Калужская область': ['калужская'],
    'Камчатский Край': ['камчатский'],
    'Карачаево-Черкесская Республика': ['карачаево_черкесская', 'карачаево_черкессия', 'карачаево_черкесия'],
    'Республика Карелия': ['карелия'],
    'Кировская область': ['кировская'],
    'Республика Коми': ['коми'],

    'Костромская область': ['костромская'],
    'Краснодарский Край': ['краснодарский'],
    'Красноярский Край': ['красноярский'],
    'Республика Крым': ['крым'],
    'Кемеровская область': ['кузбасс', 'кемеровская'],
    'Курганская область': ['курганская'],
    'Курская область': ['курская'],
    'Ленинградская область': ['ленинградская'],
    'Санкт-Петербург': ['санкт_петербург', 'спб', 'cанкт_петербург', 'cанкт петербург'],
    # 'cанкт-петербург'],   латиница не жрётся моим парсером!!!

    'Липецкая область': ['липецкая'],
    'Магаданская область': ['магаданская'],
    'Республика Марий Эл': ['марий эл'],
    'Республика Мордовия': ['мордовия'],
    'Московская область': ['московская'],
    'Москва': ['москва'],

    'Мурманская область': ['мурманская'],
    'Ненецкий АО': ['ненецкий', 'нао'],
    'Нижегородская область': ['нижегородская'],
    'Новгородская область': ['новгородская'],
    'Новосибирская область': ['новосибирская'],
    'Омская область': ['омская'],
    'Оренбургская область': ['оренбургская'],
    'Орловская область': ['орловская'],
    'Пензенская область': ['пензенская'],
    'Пермский Край': ['пермский'],
    'Приморский Край': ['приморский'],

    'Псковская область': ['псковская'],
    'Ростовская область': ['ростовская'],
    'Рязанская область': ['рязанская'],
    'Самарская область': ['самарская'],
    'Саратовская область': ['саратовская'],
    'Республика Саха-Якутия': ['саха якутия', 'якутия', 'саха_якутия'],
    'Сахалинская область': ['сахалинская'],
    'Свердловская область': ['свердловская'],
    'Севастополь': ['севастополь'],
    'Республика Северная Осетия': ['северная осетия', 'северная осетия_алания'],
    'Смоленская область': ['смоленская'],
    'Ставропольский Край': ['ставропольский'],
    'Тамбовская область': ['тамбовская'],
    'Республика Татарстан': ['татарстан'],

    'Тверская область': ['тверская'],
    'Томская область': ['томская'],
    'Тульская область': ['тульская'],
    'Республика Тыва': ['тыва'],
    'Тюменская область': ['тюменская'],
    'Удмуртская Республика': ['удмуртская', 'удмуртия'],
    'Ульяновская область': ['ульяновская'],
    'Хабаровский Край': ['хабаровский'],
    'Республика Хакасия': ['хакасия'],
    'Ханты-Мансийский АО': ['ханты_мансийский', 'хмао'],
    'Челябинская область': ['челябинская'],
    'Чеченская Республика': ['чеченская', 'чечня'],
    'Чувашская Республика': ['чувашская', 'чувашия'],
    'Чукотский АО': ['чукотский', 'чао'],
    'Ямало-Ненецкий АО': ['ямало_ненецкий', 'янао'],
    'Ярославская область': ['ярославская'],

    'Министерство культуры': ['министерство культуры'],

    'Министерство образования': ['министерство образования'],

    'Министерство экономического развития': ['министерство экономического развития'],

    'МЧС': ['мчс'],

    'Национальные приоритеты': ['национальные приоритеты'],

    'Роспотребнадзор': ['роспотребнадзор'],

    'Россельхознадзор': ['россельхознадзор'],

    'Ростуризм': ['ростуризм'],

    'Полпредство СКФО' : ['полпредство скфо'],

    'Полпредство ПФО': ['полпредство пфо'],

    'Федеральный офис': ['федеральный офис']

}

# слова которые сразу будут вычищаться
drop_names = {'республика ' : '',
          ' республика' : '',
          ' автономная' : '',
          ' область' : '',
          ' край' : '',
          ' автономный округ' : '',
#           'ао' : '',
          'цур ' : '',
          '-' : '_',
          '  ' : ' '
         }


# Функция очистки названий регионов
def clean_names(names, drop_names):
    names = [clean_str(name, '[^a-zA-Zа-яА-я0-9-_ ]').lower() for name in names]

    text = []
    names_new = []

    # чистим паттерны с "_"
    for name in names:
        for i, j in drop_names.items():
            name = name.replace(i, j)
        names_new.append(name)

    # чистим паттерны без "_"
    for name in names_new:
        for i, j in drop_names.items():
            if bool(re.search(r'\b\{0}\b'.format(i), name)) == True:
                name = name.replace(i, j)

        text.append(name.strip())

    return text


# Функция приведения регионов к стандартным названиям
def get_normal_names(base_names, reg_names):
    count = 0
    count_soch = 0
    names_new = []
    exitFlag = False

    # перебор "грязных" регионов
    for reg_name in reg_names:
        count_soch = 0
        # перебор базового словаря
        for key_name, names_list in base_names.items():
            count_soch += 1
            exitFlag = False
            for base_name in names_list:
                try:  # проверка для слов начинающихся с Кириллицы
                    match = bool(re.search(r'\b\{0}\b'.format(base_name), reg_name)) == True
                except:  # проверка для слов начинающихся с Латиницы
                    match = bool(re.search(r'\b{0}\b'.format(base_name), reg_name)) == True

                if match:
                    names_new.append(key_name)
                    count += 1
                    exitFlag = True  # если нашли, то бежим!
                    break

            if exitFlag:  # бежим!
                break

            # Если перебрали все, но не нашли совпадений
            if count_soch == len(base_names):
                names_new.append(reg_name)  # сюда нужна херь которая первоначально была на этом месте в reg_names

    print(count, '- соответствий')

    return names_new


# Функция добавления координат регионов с сохранением дефолтных названий (регионов)
def merge_two_table_by_region(df_master, df_slave, on_cols):
    df_master = df_master.copy()
    df_slave = df_slave.copy()

    # проверка на рассогласование названий регионов
    print('Проверка на рассогласование в регионах ДО нормализации')
    print(set(df_master['подрядчик']) - set(df_slave['подрядчик']))
    print(set(df_slave['подрядчик']) - set(df_master['подрядчик']))

    # нормализуем для df_master
    reg_def = df_master['подрядчик'].values  # Запоминаем исходные значения
    reg_names = df_master['подрядчик']
    reg_names = list(clean_names(reg_names, drop_names))
    reg_names = get_normal_names(base_names, reg_names)
    df_master['подрядчик'] = reg_names

    # нормализуем для df_slave
    reg_names = df_slave['подрядчик']
    reg_names = list(clean_names(reg_names, drop_names))
    reg_names = get_normal_names(base_names, reg_names)
    df_slave['подрядчик'] = reg_names

    # повторная проверка на рассогласование названий регионов
    print('Проверка на рассогласование в регионах ПОСЛЕ нормализации')
    print(set(df_master['подрядчик']) - set(df_slave['подрядчик']))
    print(set(df_slave['подрядчик']) - set(df_master['подрядчик']))

    # производим конкатенацию
    df_master = df_master.merge(df_slave, on=on_cols, how='left')

    # оставляем дефолтные названия в таблици воизбежание проблем в дашборде
    df_master['подрядчик'] = reg_def

    return df_master


# Функция предобработки массива (первая часть универсальна)
def predobr_target(name_BD, name_table, user, password, host, port, data):

    data = data.copy()

    # # Избавляемся от пробелов в заголовках
    # data.rename(columns=lambda x: x.strip(), inplace=True)

    # если пришла кривая таблица то ставим строку заголовков наместо
    if np.isnan(data.values[0][0]):
        data.dropna(axis='columns', how='all', inplace=True)
        data.dropna(axis='rows', how='all', inplace=True)
        data.reset_index(inplace=True)
        data.columns = data.iloc[0]
        data.reset_index(inplace=True, level=None, )
        data.drop(index=[0], axis=0, inplace=True)

        data.dropna(axis='columns', how='all', inplace=True)
        data = data.iloc[:, 2:]

    # удаляем пустые столбцы и строки
    data.dropna(axis='columns', how='all', inplace=True)
    data.dropna(axis='rows', how='all', inplace=True)

    # удаляем строки где в "Статус" значение "Отклонена"
    mask1 = data['Статус'].str.contains('ТКЛОНЕН', regex=True, flags=re.IGNORECASE) # можно регулярку и в нижнем писать
    mask2 = data['Тематика'].str.contains('БОНУС', regex=True, flags=re.IGNORECASE) # разницы нет
    mask = mask1 | mask2
    data = data[~mask]

    data.reset_index(drop=True, inplace=True)

    # Обработка заголовков
    data.rename(columns=lambda x: clean_str(x, '[^a-zA-Zа-яА-я0-9-. ]'), inplace=True)

    # Создание требуемых столбцов
    data = add_podr(data, 'Подрядчик')
    if name_table == 'data_inf':
        data = add_ER(data, 'ER-одобрения')

    # ............Добавляем координаты к социологии..........
    if name_table == 'data_soc':
        data = add_reg_coords(data)

    #........................................Блок специального заполнения пропусков
    if 'Федеральный округ' in data:
        data['Федеральный округ'].fillna('Федеральный офис', inplace=True)

    if 'Головная задача' in data:
        data['Головная задача'].fillna('Головная задача 1', inplace=True)

    #........................................Блок специального исправления значений в столбцах (исключение появления типа datetime.datetime)
    if 'Срок' in data:
        data['Срок'] = data['Срок'].apply(lambda x: str(x))

    #.............................Проверка на соответствие столбцов таблице в базе..........................
    # названия столбцов новой таблицы
    names_new_l = list(data.columns)
    names_new_s = set(names_new_l)

    # названия столбцов таблицы в БД (БЕЗ индексов)
    names_old_l = get_col_names(name_BD, name_table, user, password, host, port)[1:]
    names_old_s =set(names_old_l)

    # Если названия столбцов у новой Т и Т в БД одинаковы
    if names_new_s == names_old_s:
        print("Всё ровненько")
        data = data[names_old_l] # сохраняем порядок колонок как в БД и ничего НЕ изменяем


    # В БД есть новые столбцы для новой таблицы
    if len(names_old_s - names_new_s) > 0:
        print("В БД есть новые столбцы для новой таблицы")

        print(names_old_s - names_new_s)
        # Добавляем недостающие столбцы в новую таблицу
        for name_col in (names_old_s - names_new_s):
            data[name_col] = np.nan
            data[name_col] = data[name_col].fillna('0')

    # В новой таблице есть новые столбцы для БД
    if len(names_new_s - names_old_s) > 0:

        print("В новой таблице есть новые столбцы для БД")
        print(names_new_s, '- new')
        print(names_old_s, '- old')
        print(names_new_s - names_old_s, 'new - old')

        # Добавляем недостающие столбцы в БД
        new_columns = list(names_new_s - names_old_s)
        print(new_columns)#????
        for name in new_columns:
            add_column(name_BD, name_table, user, password, host, port, name)
            update_values(name_BD, name_table, user, password, host, port, name, '0')

    # делаем порядок таким же как и в таблице в базе
    names_old_l = get_col_names(name_BD, name_table, user, password, host, port)[1:]
    data = data[names_old_l] #

    return data


# Функция создания столбца "ER-одобрения"
def add_ER(data, col_name):

    data = data.copy()

    fill_list = []

    for i, row in data.iterrows():
        # Если не NaN и есть "клип", то константно 0.005
        if str(row['Место размещения']) != 'nan':
            if len(re.findall(r'клип', row['Место размещения'])) != 0:
                fill_list.append(0.005)
            else:  # в противном случае рассчитываем ER
                try:
                    ER = (row['Кол-во лайков'] + row['Кол-во репостов']) / row['Кол-во показов']
                    fill_list.append(ER)
                except:
                    fill_list.append(0)
        else:
            try:
                ER = (row['Кол-во лайков'] + row['Кол-во репостов']) / row['Кол-во показов']
                fill_list.append(ER)
            except:
                fill_list.append(0)

    data[col_name] = fill_list
    data[col_name].fillna(0, inplace=True)

    data.reset_index(drop=True, inplace=True)

    return data


# In[38]:

# Функция создания столбца "Подрядчик"
def add_podr(data, col_name):

    data = data.copy()

    podr_list = []

    for i, row in data.iterrows():
        if str(row['ЦУР']) != 'nan':
            podr_list.append(row['ЦУР'])
        elif row['Заказчик'] == 'Васильев Константин':
            podr_list.append('Редакция')
        elif str(row['Тематика']) != 'nan':
            if len(re.findall(r'нтифейк', row['Тематика'])) != 0:  # сделать гибче
                podr_list.append('Антифейк')
            else:
                podr_list.append('Федеральный офис')
        else:
            podr_list.append('Федеральный офис')

    data[col_name] = podr_list

    data.reset_index(drop=True, inplace=True)

    return data


# In[39]:

# Преобразование типов столбцов к максимально возможным уровням
def change_type(data):

    data = data.copy()

    for col_name in data.columns:
        try:
            data = data.astype({col_name: "Int64"})
            data[col_name].fillna(0, inplace=True)
        except:
            try:
                data = data.astype({col_name: "float64"})
                data[col_name].fillna(0, inplace=True)
            except:
                try:
                    data = data.astype({col_name: "datetime64[ns]"})
                    last_time = data['Дата завершения'].describe().values[-1]
                    data[col_name].fillna(last_time, inplace=True)
                except:
                    data = data.astype({col_name: "object"})
                    data[col_name].fillna('0', inplace=True)

        # глобал чистка нах
        if data[col_name].dtype == 'object':
            if col_name != 'coords':
                data[col_name] = data[col_name].apply(lambda x: clean_str(x, '[^a-zA-Zа-яА-я0-9-. ]'))

    # кастыль из-за косяка в данных (где-то все id - число, где-то строка)

    if 'id кампании в РК' in data:
        data['id кампании в РК'] = data['id кампании в РК'].apply(lambda x: str(x))
    if 'ID объявления' in data:
        data['ID объявления'] = data['ID объявления'].apply(lambda x: str(x))

    return data


# Функция приведения строки к нормальному виду
def clean_str(s, condition):
    reg = re.compile(condition, re.UNICODE)
    try:
        s = reg.sub('', s)
        s = s.strip()
    except:
        return s

    return s


def save_excel(data, path, header=True):

    data = data.copy()

    writer = pd.ExcelWriter(path,
                            engine='xlsxwriter',
                            options={'strings_to_urls': False})
    data.to_excel(writer, header=header, index=False)
    writer.close()


# Функция получения столбцов group_id & post_id
def get_ID_columns(df, url_col):
    a = 'VK'
    b = 'OK'
    c = 'Inst'
    d = 'FB'
    e = 'Tiktok'
    f = 'TG'
    g = 'Не определена'
    h = 'Viber'
    i = 'YouTube'
    k = 'Yappy'

    df[url_col] = df[url_col].replace('NAN', np.nan)

    mask1 = df[url_col].str.contains(r'vk\.com', na=False)
    mask2 = df[url_col].str.contains(r'ok\.ru', na=False)
    mask3 = df[url_col].str.contains(r'instagram\.com', na=False)
    mask4 = df[url_col].str.contains(r'facebook\.com', na=False)
    mask5 = df[url_col].str.contains(r'/fb\.', na=False)
    mask6 = df[url_col].str.contains(r'tiktok\.com', na=False)
    mask7 = df[url_col].str.contains(r'telegram\.me|t\.me', na=False)
    # searchfor = ['telegram.me', 'tiktok.com', '/fb.', 'facebook.com', 'instagram.com', 'ok.ru', 'vk.com']
    # mask8 = ~df[url_col].str.contains('|'.join(searchfor))
    mask8 = ~df[url_col].str.contains(
        r'telegram\.me|t\.me|tiktok\.com|/fb\.|facebook\.com|instagram\.com|ok\.ru|vk\.com|viber\.com|youtube\.com|yappy',
        na=False)
    mask9 = df[url_col].str.contains(r'viber\.com|vb\.me', na=False)
    mask10 = df[url_col].str.contains(r'youtube\.com', na=False)
    #				mask11 = df[url_col].str.contains(r'yappy', na= False)

    df.loc[mask1, 'Соцсеть'] = a
    df.loc[mask2, 'Соцсеть'] = b
    df.loc[mask3, 'Соцсеть'] = c
    df.loc[mask4, 'Соцсеть'] = d
    df.loc[mask5, 'Соцсеть'] = d
    df.loc[mask6, 'Соцсеть'] = e
    df.loc[mask7, 'Соцсеть'] = f
    df.loc[mask9, 'Соцсеть'] = h
    df.loc[mask10, 'Соцсеть'] = i
    #				df.loc[mask11, 'Соцсеть'] = k
    df.loc[mask8, 'Соцсеть'] = g
    print('Разбили всё по соцсетям')

    df_k_YT = df[(df['Соцсеть'] == 'YouTube')]
    yturl = df_k_YT[url_col]
    ytpoid = []
    ytgid = []
    for i in yturl:
        ytgid.append('не определён')
        ytpoid.append(i[-29:])

    df_k_YT['group_id'] = ytgid
    df_k_YT['post_id'] = ytpoid

    df_k_VB = df[(df['Соцсеть'] == 'Viber')]
    vburl = df_k_VB[url_col]
    vbpoid = []
    vbgid = []
    for i in vburl:
        vbgid.append('не определён')
        vbpoid.append(i[-29:])

    df_k_VB['group_id'] = vbgid
    df_k_VB['post_id'] = vbpoid

    # достаем айдишки групп и постов по ВК
    df_k_VK = df[(df['Соцсеть'] == 'VK')]
    vkurl = df_k_VK[url_col]
    vkpoid = []
    vkgid = []
    test = []
    for i in vkurl:
        vkf = url(i)
        vks = vkf.path
        vks = vks.split('_')
        vkc = vkf.path.split('-')
        vkq = vkf.query.split('_')
        if re.sub('[^a-z]', '', vks[0]) == 'wall' and len(vks) >= 2:
            vkgid_cycle = re.sub('[^\d]', '', vks[0])
            vkpoid_cycle = re.sub('[^\d]', '', vks[1])
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        elif len(vkq) >= 2 and re.sub('[^a-z]', '', vkq[0]) == 'wwall':
            vkgid_cycle = re.sub('[^\d]', '', vkq[0])
            vkpoid_cycle = re.sub('[^\d]', '', vkq[1])
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        elif len(vkq) >= 2 and re.sub('[^a-z]', '', vkq[0]) == 'wstory':
            vkgid_cycle = re.sub('[^\d]', '', vkq[0])
            vkpoid_cycle = re.sub('[^\d]', '', vkq[1])
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        elif len(vkq) >= 2 and re.sub('[^a-z]', '', vkq[0]) == 'listwall':
            vkgid_cycle = re.sub('[^\d]', '', vkq[0])
            vkpoid_cycle = re.sub('[^\d]', '', vkq[1])
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        elif len(vkq) >= 2 and re.sub('[^a-z]', '', vkq[0]) == 'revpost':
            vkgid_cycle = re.sub('[^\d]', '', vkq[0])
            vkpoid_cycle = re.sub('[^\d]', '', vkq[1])
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        elif re.sub('[^a-z]', '', vks[0]) == 'photo' and len(vks) >= 2:
            vkgid_cycle = re.sub('[^\d]', '', vks[0])
            vkpoid_cycle = re.sub('[^\d]', '', vks[1])
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        elif re.sub('[^a-z]', '', vks[0]) == 'video' and len(vks) >= 2:
            vkgid_cycle = re.sub('[^\d]', '', vks[0])
            vkpoid_cycle = re.sub('[^\d]', '', vks[1])
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        elif re.sub('[^a-z]', '', vks[0]) == 'story' and len(vks) >= 2:
            vkgid_cycle = re.sub('[^\d]', '', vks[0])
            vkpoid_cycle = re.sub('[^\d]', '', vks[1])
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        elif len(vkc) < 2:
            vkgid_cycle = re.sub('[^a-z^\d]', '', vks[0])
            vkpoid_cycle = ''
            vkgid.append(vkgid_cycle)
            vkpoid.append(vkpoid_cycle)
        else:
            vkgid.append('не определён')
            vkpoid.append('не определён')
    df_k_VK['group_id'] = vkgid
    df_k_VK['post_id'] = vkpoid

    # достаем айдишки групп и постов по ОК
    df_k_OK = df[(df['Соцсеть'] == 'OK')]
    xex = df_k_OK[url_col]
    gid = []
    poid = []
    for i in xex:
        f = url(i)
        ge = f.path
        ge = ge.split('/')
        if len(ge) == 6:
            g = ge[2]
            gid.append(g)
            p = ge[5]
            poid.append(p)
        elif len(ge) == 5:
            g = ge[2]
            gid.append(g)
            p = ge[4]
            poid.append(p)
        elif len(ge) == 4:
            g = ge[1]
            gid.append(g)
            p = ge[3]
            poid.append(p)
        elif len(ge) == 3:
            g = 'blank'
            gid.append(g)
            p = ge[2]
            poid.append(p)
        else:
            g = 'не определён'
            gid.append(g)
            p = 'не определён'
            poid.append(p)
    df_k_OK['group_id'] = gid
    df_k_OK['post_id'] = poid

    # удаляем дубликаты
    # df_k_OK_pars = df_k_OK_pars.drop_duplicates(['post_id', 'group_id'], keep = 'last')
    # df_k_OK_pars

    # достаем айдишки групп и постов по Инстаграму
    df_k_Inst = df[(df['Соцсеть'] == 'Inst')]
    insturl = df_k_Inst[url_col]
    instpoid = []
    instgid = []

    for i in insturl:
        instf = url(i)
        insts = instf.path
        insts = insts.split('/')
        if insts[1] == 'p':
            instpoid_cycle = insts[2]
            instpoid.append(instpoid_cycle)
            instgid.append('')
        elif len(insts) >= 4 and insts[1] == 'stories':
            # st.write (len(insts))
            instpoid_cycle = insts[3]
            instpoid.append(instpoid_cycle)
            instgid_cycle = insts[2]
            instgid.append(instgid_cycle)
        else:
            instgid.append(insts[1])
            instpoid.append('')

    df_k_Inst['group_id'] = instgid
    df_k_Inst['post_id'] = instpoid

    df_k_Inst_pars = df_k_Inst[df_k_Inst['group_id'] == '']

    ### Telegram ###
    df_k_TG = df[(df['Соцсеть'] == 'TG')]
    tg_url = df_k_TG[url_col]
    tggid = []
    tgpoid = []

    for i in tg_url:
        tgf = url(i)
        if tgf.netloc == 't.me':
            tgs = tgf.path
            tgs = tgs.split('/')
            if len(tgs) < 3:
                try:
                    tggid_cycle = tgs[1]
                    tggid.append(tggid_cycle)
                    tgpoid.append('')
                except:
                    tggid.append('не определён')
                    tgpoid.append('не определён')
            else:
                tggid_cycle = tgs[1]
                tggid.append(tggid_cycle)
                tgpoid_cycle = tgs[2]
                tgpoid.append(tgpoid_cycle)
        else:
            tgs = tgf.path
            tgs = tgs.split('/')
            if len(tgs) >= 3:
                # st.write(len(tgs))
                tggid_cycle = tgs[1]
                tggid.append(tggid_cycle)
                tgpoid_cycle = tgs[2]
                tgpoid.append(tgpoid_cycle)
            else:
                tggid.append('не определён')
                tgpoid.append('не определён')

    df_k_TG['group_id'] = tggid
    df_k_TG['post_id'] = tgpoid

    # df_k_TG = df_k_TG.drop_duplicates(subset=['group_id', 'post_id'], keep='last')
    # df_k_TG

    ###Тикток###
    df_k_Tiktok = df[(df['Соцсеть'] == 'Tiktok')]
    tkt_url = df_k_Tiktok[url_col]
    tktgid = []
    tktpoid = []

    for i in tkt_url:
        tktf = url(i)
        if tktf.netloc == 'vm.tiktok.com':
            tkts = tktf.path
            tkts = tkts.split('/')
            tktpoid_cycle = tkts[1]
            tktpoid.append(tktpoid_cycle)
            tktgid_cycle = ''
            tktgid.append(tktgid_cycle)
        elif len(tktf.path) >= 30:
            tkts = tktf.path
            tkts = tkts.split('/')
            tktgid_cycle = tkts[1]
            tktgid.append(tktgid_cycle)
            # st.write(len(tktf.path))
            tktpoid_cycle = tkts[3]
            tktpoid.append(tktpoid_cycle)
        elif len(tktf.path) < 20:
            tkts = tktf.path
            tkts = tkts.split('/')
            tktgid_cycle = tkts[1]
            tktgid.append(tktgid_cycle)
            # st.write(tkts)
            # tktpoid_cycle = tkts[3]
            tktpoid.append('')
        else:
            tktgid.append('не определён')
            tktpoid.append('не определён')

    df_k_Tiktok['group_id'] = tktgid
    df_k_Tiktok['post_id'] = tktpoid
    # df_k_Tiktok = df_k_Tiktok.drop_duplicates(subset=['post_id', 'group_id'], keep='last')
    # df_k_Tiktok

    ###Facebook###
    df_k_FB = df[(df['Соцсеть'] == 'FB')]
    fb_url = df_k_FB[url_col]  # почему не "нормализованный_урл"?
    fbgid = []
    fbpoid = []

    for i in fb_url:
        fbf = url(i)
        fbs = fbf.path
        fbs = fbs.split('/')
        if len(fbs) > 2 and fbs[2] == 'posts':
            fbpoid_cycle = fbs[3]
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[1]
            fbgid.append(fbgid_cycle)
        elif len(fbs) > 2 and fbs[1] == 'group':
            fbpoid_cycle = fbs[3]
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[2]
            fbgid.append(fbgid_cycle)
        elif len(fbs) > 3 and fbs[1] == 'groups' and fbs[3] == 'permalink':
            fbpoid_cycle = fbs[4]
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[2]
            fbgid.append(fbgid_cycle)
        elif len(fbs) == 3 and fbs[1] == 'groups':
            fbpoid_cycle = '-'
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[2]
            fbgid.append(fbgid_cycle)
        elif len(fbf.query) >= 40 and fbs[1] == 'story.php':
            fbstory = fbf.query
            fbstory = fbstory.split('&')
            fbpoid_cycle = re.sub("\D", "", fbstory[0])
            fbpoid.append(fbpoid_cycle)
            # st.write(len(fbf.query))
            fbgid_cycle = re.sub("\D", "", fbstory[1])
            fbgid.append(fbgid_cycle)
        elif fbf.netloc == 'fb.watch':
            fbpoid.append(fbs[1])
            fbgid.append('')
        elif len(fbs) == 4 and fbs[2] == 'photos':
            fbpoid_cycle = fbs[3]
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[1]
            fbgid.append(fbgid_cycle)
        elif len(fbs) > 4 and fbs[2] == 'photos':
            fbpoid_cycle = fbs[4]
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[1]
            fbgid.append(fbgid_cycle)
        elif fbs[1] == 'profile.php':
            fbgid.append(re.sub("\D", "", fbf.query))
            fbpoid.append('')
        elif len(fbs) > 3 and fbs[1] == 'groups' and fbs[3] == 'posts':
            fbpoid_cycle = fbs[4]
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[2]
            fbgid.append(fbgid_cycle)
        elif fbs[1] == 'permalink.php':
            post = fbf.query
            post = post.split('&')
            fbpoid_cycle = re.sub("\D", "", post[0])
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = re.sub("\D", "", post[1])
            fbgid.append(fbgid_cycle)
        elif len(fbs) > 3 and fbs[1] == 'groups' and fbs[3] == 'pending_posts':
            fbpoid_cycle = fbs[4]
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[2]
            fbgid.append(fbgid_cycle)
        elif fbs[1] == 'watch':
            video = fbf.query
            fbpoid_cycle = re.sub("\D", "", video)
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = '-'
            fbgid.append(fbgid_cycle)
        elif len(fbs[1]) > 150:
            fbpoid_cycle = fbf.query
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[1]
            fbgid.append(fbgid_cycle)
        elif fbs[1] == 'photo':
            photo = fbf.query
            photo = photo.split('&')
            fbpoid_cycle = re.sub("\D", "", photo[0])
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = re.sub("\D", "", photo[1])
            fbgid.append(fbgid_cycle)
        elif len(fbs) > 3 and fbs[2] == 'videos':
            fbpoid_cycle = fbs[3]
            fbpoid.append(fbpoid_cycle)
            fbgid_cycle = fbs[1]
            fbgid.append(fbgid_cycle)
        else:
            fbpoid.append('-')
            fbgid.append(fbs[1])

    df_k_FB['group_id'] = fbgid
    df_k_FB['post_id'] = fbpoid

    df_k_notsoc = df[(df['Соцсеть'] == 'Не определена')]
    #    df_k_notsoc = df_k_notsoc.drop_duplicates(subset=[url_col], keep='last')

    frames = [df_k_VK, df_k_OK, df_k_Inst, df_k_TG, df_k_Tiktok, df_k_FB, df_k_YT, df_k_VB, df_k_notsoc]

    result = pd.concat(frames)
    # result['подрядчик'] = result['подрядчик'].str.replace('ЦУР ', '')
    # result.rename(columns={'подрядчик': 'Регион'}, inplace=True)

    # else:
    df = result
    #    df = df.drop_duplicates(['post_id', 'group_id'], keep='last')

    return df

# Функция добавления координат регионов с сохранением дефолтных названий (регионов)
def add_reg_coords(df):

    df = df.copy()

    coords = get_table('vlad', 'regions_coords', 'sidorov', '13Password64', '10.128.0.20', '6432')
    coords.rename(columns={'Регион': 'Подрядчик'}, inplace=True)

    # проверка на рассогласование названий регионов
    # set(df['Подрядчик']) - set(coords['Подрядчик'])
    # set(coords['Подрядчик']) - set(df['Подрядчик'])

    # нормализуем названия регионов, чтобы сделат конкатенацию координат
    reg_def = df['Подрядчик'].values
    reg_names = df['Подрядчик']
    reg_names = list(clean_names(reg_names, drop_names))
    reg_names = get_normal_names(base_names, reg_names)
    df['Подрядчик'] = reg_names

    # проверка на рассогласование названий регионов
    # set(df['Подрядчик']) - set(coords['Подрядчик'])
    # set(coords['Подрядчик']) - set(df['Подрядчик'])

    # производим конкатенацию
    df = df.merge(coords[['Подрядчик', 'coords']], on=['Подрядчик'], how='left')

    # оставляем дефолтные названия в таблици воизбежание проблем в дашборде
    df['Подрядчик'] = reg_def

    return df


# Функция изменения порядка колонок под те что в шаблоне
def change_col_order(data, name_table):

    data = data.copy()

    if name_table == 'data_inf':
        data = data[
            ['Пост',
             'Заказчик',
             'Дата создания',
             'Исполнитель',
             'Срок',
             'Федеральный округ',
             'ЦУР',
             'Соц.сеть',
             'Место размещения',
             'Пол',
             'Статус',
             'Приоритет',
             'Цель таргетирования',
             'CPM',
             'CTR',
             'ER-like',
             'id кампании в РК',
             'ID объявления',
             'SI',
             'Возраст до',
             'Головная задача',
             'Возраст от',
             'Все акцептанты',
             'Все исполнители',
             'Гео',
             'Дата завершения',
             'Дата изменения',
             'Дата начала',
             'Дата окончания',
             'Длительность ',
             'Жалобы',
             'Желаемое кол-во показов',
             'Запрошенные подписи',
             'Кампания',
             'Кол-во кликов',
             'Напоминание',
             'Настройки таргетинга',
             'Начало',
             'Номер задачи',
             'Общие плановые трудозатраты',
             'Общие фактические трудозатраты',
             'Орг. единица исполнителя',
             'Осталось',
             'Охват',
             'Переходы по внешней ссылке',
             'Повестка',
             'Подписавшие переход',
             'Потрачено',
             'Принадлежность соц.сетей',
             'Просмотр 10 сек',
             'Просмотр 100%',
             'Просмотр 25%',
             'Просмотр 3 сек',
             'Просмотр 50%',
             'Просмотр 75%',
             'Просмотр 95%',
             'Процент выполнения',
             'Прошло',
             'Скриншот',
             'Скриншот 2',
             'Скриншот 3',
             'Скриншот 4',
             'Скриншот 5',
             'Скрытия',
             'Ссылка',
             'Старт просмотра',
             'Тематика',
             'Фактическое завершение',
             'Фактическое начало',
             'Формат контента',
             'Цена за лайк',
             'Частота показов',
             'Кол-во лайков',
             'Кол-во показов',
             'Кол-во репостов',
             'Кол-во комментариев',
             'ER',
             'Статус скриншота']
        ]
    elif name_table == 'data_pod':
        data = data[
            ['Пост',
             'Заказчик',
             'Дата создания',
             'Исполнитель',
             'Срок',
             'Соц.сеть',
             'Место размещения',
             'Пол',
             'Статус',
             'Цель таргетирования',
             'CPC',
             'CPF',
             'CPM',
             'id кампании в РК',
             'ID объявления',
             'LCPC',
             'LCTR',
             'SI',
             'Вступления',
             'Гео',
             'Дата завершения',
             'Дата изменения',
             'Дата начала',
             'Дата окончания',
             'Жалобы',
             'Желаемое кол-во показов',
             'Заявка на таргетинг',
             'Кампания',
             'Кол-во кликов',
             'Кол-во показов',
             'Настройки таргетинга',
             'Начало',
             'Номер задачи',
             'Осталось',
             'Охват',
             'Переходы',
             'Повестка',
             'Подписавшие переход',
             'Потрачено',
             'Принадлежность соц.сетей',
             'Процент выполнения',
             'Прошло',
             'Скриншот',
             'Скриншот 2',
             'Скриншот 3',
             'Скриншот 4',
             'Скриншот 5',
             'Скрытия',
             'Ссылка',
             'Тематика',
             'Фактическое завершение',
             'Фактическое начало',
             'Федеральный округ',
             'Формат контента',
             'ЦУР',
             'Частота показов',
             'Головная задача']
        ]
    else:
        data = data[
            ['Пост',
             'Заказчик',
             'Дата создания',
             'Исполнитель',
             'Срок',
             'Федеральный округ',
             'ЦУР',
             'Соц.сеть',
             'Место размещения',
             'Статус',
             'Приоритет',
             'Кол-во анкет',
             'CPC',
             'CPM',
             'ID кампании в РК',
             'LCPC',
             'LCTR',
             'Возраст до',
             'Возраст от',
             'Все акцептанты',
             'Все исполнители',
             'Гео',
             'Дата завершения',
             'Дата изменения',
             'Дата начала',
             'Дата окончания',
             'Длительность ',
             'Жалобы',
             'Желаемое кол-во показов',
             'Запрошенные подписи',
             'Кампания',
             'Кол-во показов',
             'Кол-во репостов',
             'Напоминание',
             'Настройки таргетинга',
             'Начало',
             'Номер задачи',
             'Общие плановые трудозатраты',
             'Общие фактические трудозатраты',
             'Орг. единица исполнителя',
             'Осталось',
             'Повестка',
             'Подписавшие переход',
             'Пол',
             'Потрачено',
             'Принадлежность соц.сетей',
             'Процент выполнения',
             'Прошло',
             'Скриншот',
             'Скриншот 2',
             'Скриншот 3',
             'Скриншот 4',
             'Скриншот 5',
             'Скрытия',
             'Ссылка',
             'Тематика',
             'Фактическое завершение',
             'Фактическое начало',
             'Формат контента',
             'Цель таргетирования',
             'Частота показов',
             'Кол-во кликов',
             'CR',
             'CTR',
             'CPA',
             'Головная задача',
             'Заявка на таргетинг']
        ]

    return data


# Функция удаления ненужных столбцов (не используемых)
def drop_extra_cols(data, name_table):

    data = data.copy()

    if name_table == 'data_inf':
        data = data[[
            'Заказчик',
            'Дата создания',
            'Дата начала',
            'Федеральный округ',
            'ЦУР',
            'Тематика',
            'Соц.сеть',
            'Место размещения',
            'Статус',
            'Ссылка',
            'Формат контента',
            'Потрачено',
            'Цена за лайк',
            'Кол-во лайков',
            'Кол-во показов',
            'Кол-во репостов',
            'Кол-во комментариев',
            'Головная задача',
            'ER',
            'CPM',
            'CTR',
            'Повестка',
            'Просмотр 100',
            'ER-одобрения'
        ]]
    elif name_table == 'data_pod':
        data = data[[
            'Заказчик',
            'Дата создания',
            'Дата начала',
            'Федеральный округ',
            'ЦУР',
            'Тематика',
            'Соц.сеть',
            'Место размещения',
            'Статус',
            'Ссылка',
            'Формат контента',
            'Потрачено',
            'Вступления',
            'Кол-во кликов',
            'Кол-во показов',
            'Переходы',
            'Головная задача',
            'CPC',
            'CPF',
            'CPM',
            'Повестка'
        ]]
    else:
        data = data[[
            'Заказчик',
            'Дата создания',
            'Дата начала',
            'Федеральный округ',
            'ЦУР',
            'Тематика',
            'Соц.сеть',
            'Место размещения',
            'Статус',
            'Ссылка',
            'Формат контента',
            'Потрачено',
            'Кол-во анкет',
            'Кол-во показов',
            'Кол-во репостов',
            'Кол-во кликов',
            'Головная задача',
            'CR',
            'CTR',
            'CPA',
            'Повестка'
        ]]

    return data


# Функция получения объединённой таблицы из выгрузок (информирование + подписки + социология)
def get_InfPodSoc_table(data_inf, data_pod, data_soc):

    data_inf = data_inf.copy()
    data_pod = data_pod.copy()
    data_soc = data_soc.copy()

    # Добавляем столбец "Тип данных" для идентификации
    data_inf['тип данных'] = 'информирование'
    data_pod['тип данных'] = 'подписки'
    data_soc['тип данных'] = 'социология'

    # удаляем столбец координат, если он есть
    for t in [data_inf, data_pod, data_soc]:
        if 'coords' in t:
            t.drop(columns=['coords'], axis=1, inplace=True)

    # объединяем базы
    data_all = pd.concat([data_inf, data_pod, data_soc])

    return data_all


# Функция получения regions_scor (выгрузки + бюджет)
def get_regions_score(data_all, baza, user, password, host, port):

    df = data_all.copy()

    # добавляем номер месяца и подчищенное поле "Подрядчик"
    df['номер_месяца'] = df['дата создания'].dt.month
    df['подрядчик'] = df['подрядчик'].apply(lambda x: clean_str(x, '[^a-zA-Zа-яА-я0-9-. ]'))

    # подгружаем таблицу "budget"
    # df_b = get_table(baza, 'budget', user, password, host, port)

    # Получаем таблицу "бюджет" из файла (СДЕЛАТЬ ИЗ ШАБЛОНА!)
    df_b = get_budget_table()

    # Присоединяем к большой таблице бюджет по номеру месяца
    df_all = merge_two_table_by_region(df, df_b, ['подрядчик', 'номер_месяца'])

    # в случае появления лишних столбцов - удаляем их
    if 'index' in df_all:
        df_all.drop('index', inplace=True, axis=1)
    if 'index_x' in df_all:
        df_all.drop('index_x', inplace=True, axis=1)
    if 'index_y' in df_all:
        df_all.drop('index_y', inplace=True, axis=1)

    return df_all


# Функция обновления листа в шаблоне
def update_shablon_sheet(path_table, name_sheet, table):
    with pd.ExcelWriter(path_table, engine="openpyxl", mode="a", if_sheet_exists='replace') as sheet_writer: # нет options={'strings_to_urls': False} сука
        table.to_excel(sheet_writer, sheet_name=name_sheet, index=False)


# Функция предобработкм телеграма
async def predobr_otchet(chat_id, tg_bot, df):
    """
    :param chat_id: ID чата
    :param tg_bot: объект бота
    :param df: отчёт по заявкам
    :return: обработанный массив, сумму показов, сумму подписок
    """
    df = df.copy()
    def_size = df.shape[0]  # Размер таблицы до удаления полностью пустых строк

    # Избавление от Nan и постановка заголовка на нужное место
    df.dropna(axis='rows', how='all', inplace=True)
    df.dropna(subset=[0], inplace=True)
    df.columns = df.iloc[0, :].values
    df.reset_index(inplace=True, drop=True)
    df.rename(columns=lambda x: x[:33], inplace=True)  # режем названия до длины (Прогнозируемое кол-во подписчиков)
    df = df[1:]
    await tg_bot.send_message(chat_id, f'Удалено пустых строк: {def_size - df.shape[0]}')

    # Удаляем строки с пропусками в полях Показы, Подписчики, CTR
    mask = df['Показы'].isna() | df['Подписчики'].isna() | df['CTR'].isna()
    await tg_bot.send_message(chat_id,
                           f'''Удалено строк с пустыми значениями в полях Показы, Подписки, CTR.\nКол-во таких строк: {df[mask].shape[0]}''')
    df = df[~mask]
    df.reset_index(inplace=True, drop=True)

    # Проверка на строки, где есть иные пропуски
    empty_count = df.isna().sum(axis=0).sum(axis=0)
    if empty_count == 0:
        await tg_bot.send_message(chat_id, f'Больше пропусков нет')
    else:
        empty_col_names = df.isna().sum(axis=0)[df.isna().sum(axis=0) == 1].index.values
        await tg_bot.send_message(chat_id,
                               f'''Есть пропуски в следующих столбцах: {empty_col_names},\nВсего строк с пропусками: {empty_count}''')

        # Отбираем данные строки и отправляем пользователю
        nan_flag_series = df.isna().sum(axis=1)
        empty_row_indexes = nan_flag_series[nan_flag_series != 0].index
        df_empty = df.iloc[empty_row_indexes]
        save_excel(df_empty, f'/media/sidorov/dev/NoteProjects/Bots/target_dash/rows_with_nan.xlsx')
        await tg_bot.send_document(chat_id=chat_id, document=open(
            f'/media/sidorov/dev/NoteProjects/Bots/target_dash/rows_with_nan.xlsx', 'rb'))

    # Убираем грязь из числовых столбцов
    df['Показы'] = df['Показы'].apply(lambda x: int(clean_str(str(x), '[^0-9.]')))
    df['Подписчики'] = df['Подписчики'].apply(lambda x: int(clean_str(str(x), '[^0-9.]')))
    df['CTR'] = df['CTR'].apply(lambda x: clean_str(str(x), '[^0-9.]'))
    df['CTR'] = df['CTR'].apply(lambda x: np.round(float(x), 6))

    # Запоминаем суммы агрегируемых показатели для самотестирования
    def_sum_show = df['Показы'].sum()
    def_sum_describe = df['Подписчики'].sum()

    # Столбец дата приводим в нормальный вид: если указаны две даты, то оставляем вторую
    df['Дата'] = df['Дата'].apply(lambda x: x.split(' - ')[-1])

    # kastyl po kanal 'Экограм' КАСТЫЛЬ
    df['Канал'] = df['Канал'].apply(lambda x: str(x))
    mask = df['Канал'].str.contains('gretaishere')
    df['Канал'][mask] = 'https://t.me/eco_gram'

    # Выделяем id канала из ссылки (можно взять код из обработчика, который написал Саня)
    df = get_ID_columns(df, 'Канал')
    df.drop(columns=['post_id'], inplace=True)
    df.rename({'group_id': 'ID'}, inplace=True)
    df.rename({'group_id': 'ID'}, inplace=True, axis=1)

    # По итогу оставляем упрощённое формирование ID
    df['ID'] = df['Канал'].apply(lambda x: get_TG_id(x))
    # df['ID'] = df['Канал'].apply(lambda x: x.split('t.me/')[1])
    # s1.split('t.me/')[1]
    return df, def_sum_show, def_sum_describe


# Функция заготовки Google таблицы
def get_google_table(df, kpi):
    '''
    :param df: обработанный отчёт по заявкам
    :param kpi: таблица kpi
    :return: готовую таблицу для заливки в google
    '''
    kpi_table = kpi.copy()
    df_google = df.copy()
    df_google_def = df.copy()

    df_google.drop(columns=['Соцсеть'], axis=1, inplace=True)
    # Удаляем CTR
    df_google.drop(columns=['CTR'], axis=1, inplace=True)

    df_google = df_google.groupby('Заявка', as_index=False).agg({
        'Показы': 'sum',
        'Подписчики': 'sum'
    })

    df_google = df_google.merge(df[['Дата', 'Нейминг', 'Заявка', 'Канал', 'ID', 'Текст', 'Аудитория', 'Направление']],
                                on=['Заявка'], how='left')
    df_google.drop_duplicates(subset=['Заявка'], inplace=True)

    df_google['CTR'] = df_google['Подписчики'] / df_google['Показы']

    # Добавляем 5 новых столбцов
    pivot = df_google_def.groupby(['Заявка', 'Дата'], as_index=False).agg({
        'Показы': 'sum',
        'Подписчики': 'sum',
    })
    pivot = pivot.sort_values(by=['Заявка', 'Дата'])

    pivot_ctr = pivot.drop_duplicates(subset=['Заявка'], keep='last')
    pivot_ctr_first = pivot.drop_duplicates(subset=['Заявка'], keep='first')

    pivot_ctr['Дата_min'] = pivot_ctr_first['Дата'].values
    pivot_ctr['ctr_last'] = pivot_ctr['Подписчики'] / pivot_ctr['Показы']

    pivot_ctr = pivot_ctr[['Заявка', 'Дата_min', 'Дата', 'Показы', 'Подписчики', 'ctr_last']]
    pivot_ctr.columns = ['Заявка', 'Дата_начала', 'Дата_динамика', 'Показы_динамика_сутки', 'Подписчики_динамика_сутки',
                         'CTR_сутки']

    # merge tables
    df_google = df_google.merge(pivot_ctr,
                                on=['Заявка'], how='left')
    df_google.drop_duplicates(subset=['Заявка'], inplace=True)

    # соединяем данные по статистике и kpi
    df_google = df_google.merge(kpi_table[['Округ', 'Регион',
                                     'ID']], on=['ID'], how='left')

    # удаляем данные по федеральному офису
    mask = df_google['Регион'] == 'Федеральный офис'
    df_google = df_google[~mask]

    df_google = df_google[['Округ', 'Регион', 'Заявка',
                           'Текст', 'Аудитория', 'Показы', 'Подписчики',
                           'CTR', 'Дата_начала', 'Дата_динамика', 'Показы_динамика_сутки',
                           'Подписчики_динамика_сутки', 'CTR_сутки']]

    # берём тоблицу для отправки в гугл Пашке
    df_google = df_google.reset_index().T.reset_index().T
    df_google = df_google.drop([0], axis=1)
    df_google = df_google.reset_index()
    df_google = df_google.drop(['index'], axis=1)

    return df_google


# Функция получения таблицы telegram
def get_telegram_table(df, kpi, user, password, host, port):
    
    kpi_table = kpi.copy()
    df_telegram = df.copy()
    
    # соединяем данные по статистике и kpi
    df_telegram = df_telegram.merge(kpi_table[['Округ', 'Регион',
                       'Название канала',
                       'ID',
                       'Прогнозируемое кол-во подписчиков',
                       'Необходимое кол-во показов']], on=['ID'], how='left')

    save_excel(df_telegram, '/media/sidorov/dev/NoteProjects/Zalivka_BD/after_ID_merge.xlsx')
    
    # Присоединяем координаты региона (для карты, из нашей базы)
    coords = get_table('vlad', 'regions_coords', user, password, host, port)
    coords.rename({'подрядчик': 'Регион'}, inplace=True, axis=1)
    
    df_telegram = df_telegram.merge(coords[['Регион', 'coords']], on=['Регион'], how='left')
    
    # Дополнительная предобработка
    df_telegram.rename(columns=lambda x: x[:33], inplace=True)  # режем названия до длины (Прогнозируемое кол-во подписчиков)
    df_telegram.fillna('0', inplace=True)

    return df_telegram


# Функция обновления листа "Статистика ТГ" шаблона по таргету (+ сохранение)
def update_stat_in_shablon(df, name_sheet):

    df_shablon = df.copy()

    df_shablon = df_shablon[['Дата', 'Нейминг', 'Заявка', 'Канал', 'Текст', 'Аудитория',
                     'Показы', 'Подписчики', 'CTR', 'ID', 'Регион']]
    try:
        os.remove('/media/sidorov/dev/PycharmProjects/TG_bots/DB_bots/Shablon_lists/Статистика_ТГ.xlsx')
    except:
        pass

    save_excel(df_shablon, '/media/sidorov/dev/PycharmProjects/TG_bots/DB_bots/Shablon_lists/Статистика_ТГ.xlsx')

    update_shablon_sheet('Шаблон для таргета.xlsx', name_sheet, df_shablon)


# Функция загрузки гугл таблицы на диск
def upload_google_table(df_google):

    df_google = df_google.copy()
    df_google.fillna('', inplace=True)

    # Отправляем на гугл диск Пашке
    credentials = 'key.json'
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    creds = Credentials.from_service_account_file(
        credentials, scopes=scope)

    service = discovery.build('sheets', 'v4', credentials=creds)
    spreadsheet_id = '1b2LP1wmnnU_9hveNlfeCrdtHLHJ1bb3Wo--fMUzGBgE'
    list_df = df_google.values.tolist()

    value_range_body = {
        'values': list_df
    }

    request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id,
                                                     valueInputOption='USER_ENTERED',
                                                     range='A1',
                                                     body=value_range_body)
    response = request.execute()

    return response


# Функция получения сводной таблицы по телеграму
async def get_telegram_little(chat_id, tg_bot, df, kpi, def_sum_show, def_sum_describe):
    
    kpi = kpi.copy()
    df = df.copy()
    
    # Схлопываем по региону kpi таблицу и суммируем прогнозы показам и подпискам
    little = kpi.groupby(by=['Округ', 'Регион'], as_index=False).agg(
        {
            'Прогнозируемое кол-во подписчиков': 'sum',
            'Необходимое кол-во показов': 'sum',
        }
    )

    # Схлопываем по региону и суммируем показы и подписки
    df_buf = df.groupby(by=['Регион'], as_index=False).agg(
        {
            'Показы': 'sum',
            'Подписчики': 'sum',
        }
    )

    # Соединяем обе таблицы по региону (фактические данные и прогнозы сводятся в одну таблицу)
    little = little.merge(df_buf, on='Регион', how='inner')

    # Отладочное сохранение
    save_excel(little, '/media/sidorov/dev/NoteProjects/Zalivka_BD/test_little.xlsx')

    # Оставляем только нужные столбцы, переименовываем их названия и заполняем пропуски
    little = little[['Регион',
                     'Подписчики',
                     'Показы',
                     'Необходимое кол-во показов',
                     'Прогнозируемое кол-во подписчиков',
                     'Округ']]
    little.columns = ['Регион', 'Подписчики', 'Показы', 'KPI показы', 'KPI подписки', 'Округ']
    little.fillna('0', inplace=True)

    # БЛОК САМОТЕСТИРОВАНИЯ
    # Проверка по полю показов
    if def_sum_show == little['Показы'].sum():
        await tg_bot.send_message(chat_id, f'Сумма показов в исходной таблице РАВНА сумме показов в сводной')
    else:
        diff = def_sum_show - little['Показы'].sum()
        await tg_bot.send_message(chat_id,
                               f'Сумма показов в исходной таблице БОЛЬШЕ на {diff} \nСледует сделать проверку')

    # Проверка по полю подписчиков
    if def_sum_describe == little['Подписчики'].sum():
        await tg_bot.send_message(chat_id, f'Сумма подписок в исходной таблице РАВНА сумме показов в сводной')
    else:
        diff = def_sum_describe - little['Подписчики'].sum()
        await tg_bot.send_message(chat_id,
                               f'Сумма подписок в исходной таблице БОЛЬШЕ на {diff} \nСледует сделать проверку')
        
    return little


# Функция обновления таблицы в облаке
async def update_cloud_table(message, dir_name, new_target_name):
    try:
        # Удаляем предыдущую таблицу в облаке
        list_f = subprocess.run(['rm',
                                 f'/run/user/1004/gvfs/dav:host=cloud.dialog-regions.ru,ssl=true,prefix=%2Fremote.php%2Fdav/files/SidorovVS/{dir_name}/{new_target_name}',
                                 ],
                                stdout=subprocess.PIPE,
                                text=True,
                                )

        # Копируем новую таблицу в облако
        list_f = subprocess.run(['cp',
                                 new_target_name,
                                 f'/run/user/1004/gvfs/dav:host=cloud.dialog-regions.ru,ssl=true,prefix=%2Fremote.php%2Fdav/files/SidorovVS/{dir_name}'],
                                stdout=subprocess.PIPE,
                                text=True,
                                )
    except Exception as e:
        print(e)
        await message.reply(e)

    return list_f


# Функция исправления типов и заполнения пропусков столбцов
def fix_type_fill_nan(df):
    
    df = df.copy()
    
    df = change_type(df)
    df.reset_index(inplace=True)
    df.drop('index', axis=1, inplace=True)

    if 'level_0' in df:
        df.drop('level_0', axis=1, inplace=True)
        
    return df

# def get_
# data_inf = get_table(baza, 'data_inf', user, password, host, port)
# data_inf.rename(columns=lambda x: x.lower(), inplace=True)
# data_pod = get_table(baza, 'data_pod', user, password, host, port)
# data_pod.rename(columns=lambda x: x.lower(), inplace=True)
# data_soc = get_table(baza, 'data_soc', user, password, host, port)
# data_soc.rename(columns=lambda x: x.lower(), inplace=True)


def get_reg_scor_with_other_tables(reg_scor):

    reg_scor = reg_scor.copy()

    # ...................Подготовка таблицы kpi для мержинга с reg_scor
    kpi_cpf = pd.read_excel('/media/sidorov/dev/PycharmProjects/TG_bots/DB_bots/Shablon_lists/KPI.xlsx')
    kpi_cpf = kpi_cpf.rename(columns={'Подрядчик': 'подрядчик'})

    # .................Подготовка таблицы telegram_little для мержинга с reg_scor
    tel_lit = get_table('target_dash', 'telegram_little', 'sidorov', '13Password64', '10.128.0.20', '6432')
    tel_lit.drop(columns=['index'], inplace=True)
    tel_lit['доля выполнения подписчиков'] = tel_lit['Подписчики'] / tel_lit['KPI подписки']
    tel_lit['доля выполнения показов'] = tel_lit['Показы'] / tel_lit['KPI показы']
    tel_lit = tel_lit.rename(columns={'Регион': 'подрядчик'})

    new_cols = [f'TG_{i}' for i in tel_lit.columns]
    tel_lit.columns = new_cols
    tel_lit = tel_lit.rename(columns={'TG_подрядчик': 'подрядчик'})

    # .......................Подготовка таблицы "аудитория" из шаблона для мержинга с reg_scor
    # ? Связать с шаблоном ?
    auditory = pd.read_excel('/media/sidorov/dev/PycharmProjects/TG_bots/DB_bots/Шаблон для таргета.xlsx',
                             sheet_name='аудитория')

    # Удаляем строки с пропущенным регионом полностью пустые колонки или строки
    auditory.dropna(subset=['Регион'], inplace=True)
    auditory.dropna(axis='columns', how='all', inplace=True)
    auditory.dropna(axis='rows', how='all', inplace=True)
    auditory.drop(columns=['Округ'], inplace=True)

    # Приводим названия в соответствие с контентом (добавляем "Аудитория")
    auditory.columns = [f'Аудитория_{i}' for i in auditory.columns]
    auditory = auditory.rename(columns={'Аудитория_Регион': 'подрядчик'})

    # Соединяем полученные таблицы с reg_scor
    reg_scor =  merge_two_table_by_region(reg_scor, kpi_cpf, ['подрядчик'])
    reg_scor =  merge_two_table_by_region(reg_scor, tel_lit, ['подрядчик'])
    reg_scor =  merge_two_table_by_region(reg_scor, auditory, ['подрядчик'])

    # Создаём новый столбец "переход"
    reg_scor['переход'] = '0'
    reg_scor['переход'][reg_scor['тематика'].str.contains('переход', regex=True, flags=re.IGNORECASE)] = '1'
    reg_scor[reg_scor['переход'] == '1']

    return reg_scor

def add_bans_cols(reg_scor):

    reg_scor = reg_scor.copy()

    date_col = 'дата_создания_точность_дни'

    # ..................Подготовка bans
    bans = pd.read_excel('/media/sidorov/dev/PycharmProjects/TG_bots/DB_bots/Штрафы.xlsx')
    bans.columns = [i.strip().lower() for i in bans.columns]

    # Добавляем необходимые столбцы в "bans" "дата_создания_точность_дни" и "дата_создания_квартал"
    bans = bans.rename(columns={'дата штрафа': date_col})
    bans['дата_создания_квартал'] = bans[date_col].apply(lambda d: get_quart_from_numpydate(d))

    # Нормализуем подрядчика
    bans = bans.rename(columns={'регион': 'подрядчик'})
    bans = get_normal_reg_frame(bans)

    # ..................Подготовка reg_scor
    # Добавляем необходимые столбцы в reg_scor
    reg_scor['дата_создания_точность_дни'] = reg_scor['дата создания'].apply(lambda d: np.datetime64(str(d)[:10]))
    reg_scor['дата_создания_квартал'] = reg_scor['дата_создания_точность_дни'].apply(
        lambda d: get_quart_from_numpydate(d))

    # Нормализуем подрядчика
    reg_scor = reg_scor.rename(columns={'регион': 'подрядчик'})
    reg_def = reg_scor['подрядчик'].values  # Запоминаем исходные значения
    reg_scor = get_normal_reg_frame(reg_scor)

    # ban_type_list = ['информирование', 'подписки', 'социология', 'подписки тг']
    ban_type_list = ['информирование', 'подписки', 'социология']

    # Добавляем коэффициенты штрафов в reg_scor
    for i, ban_type in enumerate(ban_type_list):

        # Создаём столбцы для каждого типа штрафов
        col_name_ban = f'Штраф_{ban_type}'
        reg_scor[col_name_ban] = 0

        # берём срез по текущему типу в bans
        mask_ban = ~bans[ban_type].isna()
        bans_sres = bans[mask_ban]

        # Если есть баны по данному типу данных
        if bans_sres.shape[0] > 0:
            for index, ban_row in bans_sres.iterrows():

                # Убираем из расчёта строки, где заполнено штрафное поле (оставляем только пустые)
                mask_empty = reg_scor[col_name_ban] == 0
                # берём срез по текущему типу в reg_scor
                mask_type = reg_scor['тип данных'] == ban_type
                # общая маска (тип + пустые значения)
                full_mask = mask_empty & mask_type
                reg_scor_sres = reg_scor[full_mask]

                # получаем записи по имени и дате региона по таблице штрафов
                mask_reg = reg_scor_sres['подрядчик'] == ban_row['подрядчик']
                mask_date = reg_scor_sres['дата_создания_точность_дни'] == ban_row['дата_создания_точность_дни']
                mask = mask_reg & mask_date

                # Получаем записи подходящие под условия штрафа
                df_shtraf = reg_scor_sres[mask]

                # Если такие записи нашлись, то
                if df_shtraf.shape[0] > 0:
                    # то берём индекс первой
                    index_ban_line = df_shtraf.index.values[0]

                    # и по нему вносим штраф в ИСХОДНЫЙ reg_scor
                    mask = reg_scor.index == index_ban_line
                    reg_scor[col_name_ban][mask] = ban_row[ban_type]

                # Если нет записей подходящий под условие
                else:
                    # получаем записи по имени и кварталу
                    mask_reg = reg_scor_sres['подрядчик'] == ban_row['подрядчик']
                    mask_date = reg_scor_sres['дата_создания_квартал'] == ban_row['дата_создания_квартал']
                    mask = mask_reg & mask_date

                    # Получаем записи подходящие под условия штрафа (квартал)
                    df_shtraf = reg_scor_sres[mask]

                    # берём индекс первой записи
                    index_ban_line = df_shtraf.index.values[0]

                    # и по нему вносим штраф в ИСХОДНЫЙ reg_scor
                    mask = reg_scor.index == index_ban_line
                    reg_scor[col_name_ban][mask] = ban_row[ban_type]
        # Если банов по данному типу нет
        else:
            # Ничего не делаем
            pass

    # Ставим на место дефолтные значения reg_scor, во избежание проблем в базе
    reg_scor['подрядчик'] = reg_def

    return reg_scor


# Функция получения квартала из numpy.datetime64
def get_quart_from_numpydate(date_nampy):
    date_datetime = to_datetime(date_nampy)
    item = DateQuarter.from_date(date_datetime)  # 2019 4Q
    quart = item.quarter()

    return quart


# Converts a numpy datetime64 object to a python datetime object
def to_datetime(date):
    """
    Converts a numpy datetime64 object to a python datetime object
    Input:
      date - a np.datetime64 object
    Output:
      DATE - a python datetime object
    """
    timestamp = ((date - np.datetime64('1970-01-01T00:00:00'))
                 / np.timedelta64(1, 's'))
    return datetime.utcfromtimestamp(timestamp)


def get_normal_reg_frame(df_master):
    df_master = df_master.copy()

    reg_names = df_master['подрядчик']
    reg_names = list(clean_names(reg_names, drop_names))
    reg_names = get_normal_names(base_names, reg_names)
    df_master['подрядчик'] = reg_names

    return df_master


# Функция получения таблицы "бюджет" в нужном формате (длинная таблица)
def get_budget_table():

    # В словаре исключён март, т.к. он используется в стартовой таблице
    month_num_dict = {
        'январь': 1,
        'февраль': 2,
        'апрель': 4,
        'май': 5,
        'июнь': 6,
        'июль': 7,
        'август': 8,
        'сентябрь': 9,
        'октябрь': 10,
        'ноябрь': 11,
        'декабрь': 12,
    }

    budg_new = pd.read_excel('/media/sidorov/dev/PycharmProjects/TG_bots/DB_bots/Shablon_lists/Бюджет.xlsx', header=[0, 1])

    budg_new.columns = budg_new.columns.map('_'.join)
    budg_new.rename(columns={'Unnamed: 0_level_0_Регион': 'подрядчик'}, inplace=True)
    budg_new.drop('Unnamed: 1_level_0_ФО', inplace=True, axis=1)
    col_names_def = budg_new.columns

    month_cols = [col_name_def for col_name_def in col_names_def if 'март'.lower() in col_name_def.lower()]
    budg_start = budg_new[['подрядчик'] + month_cols]
    budg_start['номер_месяца'] = 3
    budg_start.columns = ['подрядчик', 'ВК', 'ОК', 'номер_месяца']

    for month, num in month_num_dict.items():

        month_cols = [col_name_def for col_name_def in col_names_def if month.lower() in col_name_def.lower()]

        # Если такой месяц нашёлся в ВК и ОК, то производим конкатинацию
        if len(month_cols) == 2:
            budg_buf = budg_new[['подрядчик'] + month_cols]
            month_num = month_num_dict[month]
            budg_buf['номер_месяца'] = month_num
            budg_buf.columns = ['подрядчик', 'ВК', 'ОК', 'номер_месяца']

            budg_start = pd.concat([budg_start, budg_buf])

        else:
            pass

    return budg_start


# http://
# https://
# http://@
# https://@


# Функция выделения ID телеграмм канала
def get_TG_id(s1):
    # Выделяем часть после "t.me/"
    try:
        s1 = s1.split('t.me/')[1]
    except Exception as e:
        logging.exception(f'{e}, - {s1}')
        print(e)
        print(s1)
        s1 = re.sub(r'https:', '', s1)
        s1 = re.sub(r'http:', '', s1)
        s1 = re.sub(r'//', '', s1)
        s1 = re.sub(r'/', '', s1)
        s1 = re.sub(r'@', '', s1)

    # Выделенной части находим первый не буквенный и числовой элемент (и "_")
    simbols = re.findall(r'\W', s1)

    # Если какая-то дич после ID нашлась
    if len(simbols) != 0:
        split_simbol = simbols[0]

        # По найденному элементу бьём строку и берём первую часть (это наш ID)
        s1 = s1.split(split_simbol)[0]

    return s1


# Функция занесения чата id в таблицу
def add_chat_id(username, chat_id):
    UsChatId = pd.read_excel('UsChatId.xlsx')
    if username not in UsChatId['user_name'].values:
        UsChatId = UsChatId.append({'user_name': username, 'chat_id': chat_id}, ignore_index=True)
        save_excel(UsChatId, 'UsChatId.xlsx')


# Тест fun
async def test_mes(chat_id, tg_bot):
    await tg_bot.send_message(chat_id, 'Hellow')
    return 5, 6, 7



