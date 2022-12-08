
# import datetime
import shutil
import asyncio

import pandas as pd
from dotenv import load_dotenv, find_dotenv

from Processing_functions import *
from DB_functions import *

# Импортируем необходимые модули аиограма
from aiogram import Bot, Dispatcher, executor, types
from aiogram.bot.api import TelegramAPIServer

#  Модули для машины состояний
# from aiogram.dispatcher import FSMContext #
# from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage

import warnings
warnings.filterwarnings('ignore')

# apihelper.API_URL = "http://localhost:8090/bot{0}/{1}"
path_base = 'C:/Projects/Python_projects/DB_bots'


# Конфиденциальные данные
load_dotenv(find_dotenv()) # погрузка .env
API_TOKEN = os.getenv('API_TOKEN')
user = os.getenv('user')
password = os.getenv('password')

baza = 'target_dash'
host = 'rc1a-g1bf60qz9xqx8l90.mdb.yandexcloud.net'
port = '6432'

# Настройка подключения
local_server = TelegramAPIServer.from_base('http://localhost:8081') # 8081 - port our API server
bot = Bot(token=API_TOKEN, server=local_server)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


# Функция, обрабатывающая команду /start
@dp.message_handler(commands=['start'])
async def start_message(message):

    await bot.send_message(message.chat.id, f'Инициализация')

    users = pd.read_csv('users_target.txt')
    username = message.from_user.username
    access = username in users['User_name'].values
    chat_id = message.chat.id
    if access:

        # Заносим id чата с пользователем в базу (чтобы ему спамить)
        add_chat_id(username, chat_id)

        await bot.send_message(message.chat.id, f'Выбранная база: {baza}')
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        item1 = types.KeyboardButton("Подгрузить таблицы")
        item2 = types.KeyboardButton("Загрузить в БД")
        item3 = types.KeyboardButton("Обновить regions_scor")
        item4 = types.KeyboardButton(f'''Получить "Шаблон для таргета"''')
        markup.add(item1)
        markup.add(item2)
        markup.add(item3)
        markup.add(item4)
        await bot.send_message(message.chat.id, 'Чего изволите?', reply_markup=markup)
    else:
        await bot.send_message(message.chat.id, 'У вас нет паспорта')


# Функция изменения целевой базы
@dp.message_handler(lambda message: message.text in ['vlad', 'target_dash'],
                    content_types=['text'])
async def change_base(message):
    global baza
    baza = message.text
    await bot.send_message(message.chat.id, f'Выбранная база: {baza}')


# Функция удаления предыдущих таблиц на сервере (не в базе)
@dp.message_handler(lambda message: message.text == "Подгрузить таблицы")
async def delete_ex_tables(message):

    users = pd.read_csv('users_target.txt')
    username = message.from_user.username
    access = username in users['User_name'].values
    if access:
        try:
            dirs = [path_base + '/target_dash/inf/',
                    path_base + '/target_dash/pod/',
                    path_base + '/target_dash/soc/'
                    ]

            await bot.send_message(message.chat.id, 'Удаляем старые')
            # удаление
            for dir_name in dirs:
                file_names = os.listdir(dir_name)
                for file_name in file_names:
                    os.remove(dir_name + file_name)
            await bot.send_message(message.chat.id, 'Старые удалены, давайте новые')
        except Exception as e:
            await bot.send_message(message.chat.id, e)
    else:
        await bot.send_message(message.chat.id, 'У вас нет паспорта')

    # ..............................Подгрузка таблиц по таргету.......................
    # Загрузка информирования
    @dp.message_handler(lambda message: 'форм' in message.document.file_name.lower(),
                        content_types=['document'])
    async def load_inf(message):
        users = pd.read_csv('users_target.txt')
        username = message.from_user.username
        access = username in users['User_name'].values
        if access:
            try:
                file_info = await bot.get_file(message.document.file_id)
                df = pd.read_excel(file_info.file_path)
                path = '/target_dash/inf/' + file_info.file_path.split('documents/')[1]
                save_excel(df, path)
                dir_names = os.listdir('/target_dash/inf')
                await bot.send_message(message.chat.id, f'''Загружено файлов по информированию - {len(dir_names)}''')
            except Exception as e:
                await bot.send_message(message.chat.id, e)
        else:
            await bot.send_message(message.chat.id, 'У вас нет паспорта')

    # Загрузка подписок
    @dp.message_handler(lambda message: 'одпи' in message.document.file_name.lower(),
                        content_types=['document'])
    async def load_pod(message):
        users = pd.read_csv('users_target.txt')
        username = message.from_user.username
        access = username in users['User_name'].values
        if access:
            try:
                file_info = await bot.get_file(message.document.file_id)
                df = pd.read_excel(file_info.file_path)
                path = '/target_dash/pod/' + file_info.file_path.split('documents/')[1]
                save_excel(df, path)
                dir_names = os.listdir('/target_dash/pod')
                await bot.send_message(message.chat.id, f'''Загружено файлов по подпискам - {len(dir_names)}''')
            except Exception as e:
                await bot.send_message(message.chat.id, e)
        else:
            await bot.send_message(message.chat.id, 'У вас нет паспорта')

    # Загрузка социологии
    @dp.message_handler(lambda message: 'оцио' in message.document.file_name.lower(),
                        content_types=['document'])
    async def load_soc(message):
        users = pd.read_csv('users_target.txt')
        username = message.from_user.username
        access = username in users['User_name'].values
        if access:
            try:
                file_info = await bot.get_file(message.document.file_id)
                # await bot.send_message(message.chat.id, file_info.file_path)
                df = pd.read_excel(file_info.file_path)
                path = '/target_dash/soc/' + file_info.file_path.split('documents/')[1]
                save_excel(df, path)
                dir_names = os.listdir('/target_dash/soc')
                await bot.send_message(message.chat.id, f'''Загружено файлов по социологии - {len(dir_names)}''')
            except Exception as e:
                await bot.send_message(message.chat.id, e)
        else:
            await bot.send_message(message.chat.id, 'У вас нет паспорта')


# ..............................Загрузка в БД.......................
@dp.message_handler(lambda message: message.text == "Загрузить в БД")
async def update_db(message):
    users = pd.read_csv('users_target.txt')
    username = message.from_user.username
    access = username in users['User_name'].values
    chat_id = message.chat.id
    if access:
        try:
            dirs = [
                '/target_dash/inf/',
                '/target_dash/pod/',
                '/target_dash/soc/',
            ]

            tables = [
                'data_inf',
                'data_pod',
                'data_soc',
            ]

            sheet_names = {
                'data_inf': 'Информирование',
                'data_pod': 'подписки',
                'data_soc': 'социология',
            }

            # Задаём путь к шаблону по таргету
            path_shablon = '/Шаблон для таргета.xlsx'

            # Создаём копию шаблона по таргету с текущей датой (изменения будем производить в ней)
            current_date = datetime.now()
            # today_date = f'{current_date.day}_{current_date.month}_{current_date.year}'
            today_date = f'{current_date.year}_{current_date.month}_{current_date.day}'
            new_target_name = f'Таргет_{today_date}.xlsx'
            shutil.copyfile(path_shablon, new_target_name)

            # ..........................Объединение всех таблиц воедино
            for dir_name, table in zip(dirs, tables):
                await bot.send_message(message.chat.id, f'Загружаем {table}')
                uploaded_file = os.listdir(dir_name)
                df_start = pd.read_excel(dir_name + uploaded_file[0])
                df_start = predobr_target(baza, table, user, password, host, port, df_start)
                if len(uploaded_file) > 1:
                    for file in uploaded_file[1:]:
                        df = pd.read_excel(dir_name + file)
                        df = predobr_target(baza, table, user, password, host, port, df)
                        df_start = pd.concat([df_start, df])

                # Обновляем индекс и перезаписываем df
                df_start.reset_index(drop=True, inplace=True)
                df = df_start.copy()

                # Для шаблона (уйти от данного распаралеливания путём удаления ненужных столбцов из базы!!!)
                df_shablon = df.copy()
                df_shablon = drop_extra_cols(df_shablon, table)
                save_excel(df_shablon, 'Шаблон из кода.xlsx')

                # Обновляем шаблон для облака
                # name_sheet = sheet_names[table]
                # await bot.send_message(message.chat.id, f'Началась загрузка шаблона - {name_sheet}')
                # update_shablon_sheet(new_target_name, name_sheet, df_shablon)
                # await bot.send_message(message.chat.id, f'Завершилась загрузка шаблона - {name_sheet}')

                # Для базы исправляем типы и заполняем пропуски, чтобы всё безошибочно легло
                df = change_type(df)
                df.fillna('0', inplace=True)

                # ....................................ОБновляем таблицу в БД....................................
                await bot.send_message(message.chat.id, f'Размер таблицы ДО внесения изменений: {get_table_size(baza, table, user, password, host, port)[0]}')
                n_rows = get_table_size(baza, table, user, password, host, port)[0]
                drop_inerval(baza, table, user, password, host, port, n_rows - n_rows, n_rows - 1)
                insert_target(baza, table, user, password, host, port, df)
                await bot.send_message(message.chat.id, f'Размер таблицы ПОСЛЕ внесения изменений: {get_table_size(baza, table, user, password, host, port)[0]}')

            # Обновляем таблицу в облаке
            task_cloud = asyncio.create_task(update_cloud_table(message, 'Target', new_target_name))
            req_answer = await task_cloud

        except Exception as e:
            print(e)
            await message.reply(e)
        await bot.send_message(message.chat.id, 'Таблицы загружены!')
    else:
        await bot.send_message(message.chat.id, 'У вас нет паспорта')


# ..............................Обновление большой таблицы в БД.......................
@dp.message_handler(lambda message: message.text == "Обновить regions_scor")
async def update_big_table(message):
    users = pd.read_csv('users_target.txt')
    username = message.from_user.username
    access = username in users['User_name'].values
    if access:
        try:
            await bot.send_message(message.chat.id, 'Обновляем regions_scor для Datalens!')

            # Загружаем таблицы для Даталенса и приводим их заголовки к нижнему регистру
            data_inf = get_table(baza, 'data_inf', user, password, host, port)
            data_inf.rename(columns=lambda x: x.lower(), inplace=True)
            data_pod = get_table(baza, 'data_pod', user, password, host, port)
            data_pod.rename(columns=lambda x: x.lower(), inplace=True)
            data_soc = get_table(baza, 'data_soc', user, password, host, port)
            data_soc.rename(columns=lambda x: x.lower(), inplace=True)

            # Получение объединённой таблицы для Даталенса (информирование + подписки + социология)
            df_datalens = get_InfPodSoc_table(data_inf, data_pod, data_soc)
            await bot.send_message(message.chat.id, f'Размер после объединения: {df_datalens.shape}')

        # ....................................Аппендикс.....................................
            # Исправление типов и заполнения пропусков столбцов
            df_datalens = fix_type_fill_nan(df_datalens)

            table = 'inf_pod_soc'
            await bot.send_message(message.chat.id, 'Загружаем аппендикс (объединённую БЕЗ бюджета)')
            n_rows = get_table_size(baza, table, user, password, host, port)[0]
            drop_inerval(baza, table, user, password, host, port, n_rows-n_rows, n_rows-1)
            insert_target(baza, table, user, password, host, port, df_datalens)
            await bot.send_message(message.chat.id, 'Аппендикс загружен')

            # ........................Получение  regions_scor для Даталенс (+ бюджет) ................................
            df_datalens = get_regions_score(df_datalens, baza, user, password, host, port)
            await bot.send_message(message.chat.id, f'Размер финальной таблицы: {df_datalens.shape}')

            # Исправление типов и заполнения пропусков столбцов
            df_datalens = fix_type_fill_nan(df_datalens)

            # в качестве целевой таблицы ставим regions_scor
            table = 'regions_scor'
            #	create_table(baza, table, df_datalens, user, password, host, port)
            await bot.send_message(message.chat.id, f'Размер таблицы ДО внесения изменений: {get_table_size(baza, table, user, password, host, port)[0]}')
            n_rows = get_table_size(baza, table, user, password, host, port)[0]
            drop_inerval(baza, table, user, password, host, port, n_rows-n_rows, n_rows-1)
            insert_target(baza, table, user, password, host, port, df_datalens)
            await bot.send_message(message.chat.id, f'Размер таблицы ПОСЛЕ внесения изменений: {get_table_size(baza, table, user, password, host, port)[0]}')
            await bot.send_message(message.chat.id, 'regions_scor в Даталенс обновлена')

            # ........................Обновление базы для Apache Superset...........................
            await bot.send_message(message.chat.id, 'Обновляем regions_scor для Superset!')

            # Берём файл с самой актуальной датой
            name_files = os.listdir(path=".") # из текущей дирректории
            target_files = [name for name in name_files if 'Таргет' in name]
            target_files.sort()
            new_target_name = target_files[-1]

            # Парсим необходимую статику из шаблона
            inf_super = pd.read_excel(new_target_name, sheet_name='Информирование')
            inf_super.rename(columns=lambda x: x.lower(), inplace=True)
            pod_super = pd.read_excel(new_target_name, sheet_name='подписки')
            pod_super.rename(columns=lambda x: x.lower(), inplace=True)
            soc_super = pd.read_excel(new_target_name, sheet_name='социология')
            soc_super.rename(columns=lambda x: x.lower(), inplace=True)

            # Получение объединённой таблицы для Суперсета (информирование + подписки + социология)
            df_regscor_super = get_InfPodSoc_table(inf_super, pod_super, soc_super)
            await bot.send_message(message.chat.id, f'Размер после объединения: {df_regscor_super.shape}')

            # Кастыль по названиям (красиво решить)
            df_regscor_super.rename(columns={'цур': 'подрядчик'}, inplace=True)

            # ........................Получение  regions_scor для Суперсет (+ бюджет) .............................
            # Избавляемся от пропусков в поле "подрядчик" и "дата начала"
            df_regscor_super['подрядчик'].fillna('Федеральный офис', inplace=True)
            mask =df_regscor_super['дата начала'].isna()
            df_regscor_super['дата начала'][mask] =  df_regscor_super['дата создания'][mask]

            df_regscor_super = get_regions_score(df_regscor_super, baza, user, password, host, port)
            await bot.send_message(message.chat.id, f'Размер после добавления бюджета: {df_regscor_super.shape}')
            df_regscor_super = fix_type_fill_nan(df_regscor_super)

            # .............................Кастыльный отрезок по добавлению таблиц (требует доработки).................................
            await bot.send_message(message.chat.id, f'Началось объединение с другими таблицами')
            df_regscor_super = get_reg_scor_with_other_tables(df_regscor_super)
            df_regscor_super = add_bans_cols(df_regscor_super)
            await bot.send_message(message.chat.id, f'Размер финальной таблицы: {df_regscor_super.shape}')

            # Исправление типов и заполнения пропусков столбцов
            df_regscor_super = fix_type_fill_nan(df_regscor_super)

            # .........................загружаем regions_score_apache в БД.....................
            table = 'regions_score_apache'
            # create_table(baza, table, df_regscor_super, user, password, host, port)
            await bot.send_message(message.chat.id, f'Размер таблицы ДО внесения изменений: {get_table_size(baza, table, user, password, host, port)[0]}')
            n_rows = get_table_size(baza, table, user, password, host, port)[0]
            drop_inerval(baza, table, user, password, host, port, n_rows-n_rows, n_rows-1)
            insert_target(baza, table, user, password, host, port, df_regscor_super)
            await bot.send_message(message.chat.id, f'Размер таблицы ПОСЛЕ внесения изменений: {get_table_size(baza, table, user, password, host, port)[0]}')
            await bot.send_message(message.chat.id, 'regions_scor в Суперсет обновлена')
        except Exception as e:
            print(e)
            await message.reply(e)
    else:
        await bot.send_message(message.chat.id, 'У вас нет паспорта')


# .....................Получение шаблона для таргета.....................
@dp.message_handler(lambda message: message.text == f'''Получить "Шаблон для таргета"''')
async def get_target_shablon(message):

    chat_id = message.chat.id
    await bot.send_document(chat_id=chat_id, document=open(f'Шаблон для таргета.xlsx', 'rb'))


# .....................Обновление шаблона для таргета.....................
@dp.message_handler(lambda message: 'аблон' in message.document.file_name.lower() and
                                    'аргет' in message.document.file_name.lower(),
                    content_types=['document'])
async def load_soc(message):
    users = pd.read_csv('users_target.txt')
    username = message.from_user.username
    access = username in users['User_name'].values
    if access:
        try:
            await bot.send_message(message.chat.id, f'''Шаблон обновляется...''')
            file_info = await bot.get_file(message.document.file_id)
            path_new_target = file_info.file_path
            shutil.copyfile(path_new_target, 'Шаблон для таргета.xlsx')
            await bot.send_message(message.chat.id, f'''Шаблон обновлён!''')
        except Exception as e:
            await bot.send_message(message.chat.id, e)
    else:
        await bot.send_message(message.chat.id, 'У вас нет паспорта')


# .....................................Telegram.............................
# Загрузка в даталенс таргета по телеграмму (Приходит одна, в базу ложатся две)
@dp.message_handler(lambda message: 'аявк' in message.document.file_name.lower(),
                    content_types=['document'])
async def target_telegram(message):
    users = pd.read_csv('users_telegram.txt')
    username = message.from_user.username
    access = username in users['User_name'].values
    chat_id = message.chat.id
    if access:
        try:
            await bot.send_message(message.chat.id, f'Таблица сохраняется')

            # Загрузка таблицы "Отчёт по заявкам" на сервер
            file_info = await bot.get_file(message.document.file_id)
            # Получаем таблицу "Отчёт по заявкам"
            df = pd.read_excel(file_info.file_path, sheet_name='Заявки по дням', header=None)
            # Получаем таблицу KPI из базы
            kpi = get_table(baza, 'telegram_kpi', user, password, host, port)

            # ................................Обработка отчёта по заявкам...............................
            await bot.send_message(message.chat.id, f'Таблица обрабатывается')
            task_otchet = asyncio.create_task(predobr_otchet(message.chat.id, bot, df))
            df, def_sum_show, def_sum_describe = await task_otchet
            await bot.send_message(message.chat.id, f'Размер финальной таблицы: {df.shape[0]}')

            # ...............................Формирование таблицы для заливки в Google.......................
            df_google = get_google_table(df, kpi)
            save_excel(df_google, 'Подписки TG.xlsx')
            await bot.send_message(message.chat.id, f'Размер таблицы для гугла: {df_google.shape[0]}')

            # ............................Формирование таблицы telegram (НЕ схлопнутая)
            df = get_telegram_table(df, kpi, user, password, host, port)

            # Обновляем лист "Статистика ТГ" в шаблоне по таргету
            # name_sheet = 'Статистика ТГ'
            # await bot.send_message(message.chat.id, f'Началась загрузка шаблона - {name_sheet}')
            # update_stat_in_shablon(df, name_sheet)
            # await bot.send_message(message.chat.id, f'Завершилась загрузка шаблона - {name_sheet}')

            # Обновление таблицы "telegram" в базе
            await bot.send_message(message.chat.id, f'Таблица загружается в БД')
            n_rows = get_table_size(baza, 'telegram', user, password, host, port)[0]
            drop_inerval(baza, 'telegram', user, password, host, port, n_rows - n_rows, n_rows - 1)
            insert_target(baza, 'telegram', user, password, host, port, df)
            await bot.send_message(message.chat.id, f'Таблица загружена в БД')

            # ........Загрузка гугл таблицы на гугл диск........
            response = upload_google_table(df_google)
            await bot.send_message(message.chat.id, f'{response}')

            # ........Загрузка гугл таблицы в cloud........
            task_cloud = asyncio.create_task(update_cloud_table(message, 'Подписки_TG', 'Подписки TG.xlsx'))
            req_answer = await task_cloud
            await bot.send_message(message.chat.id, f'{req_answer}')
            await bot.send_message(message.chat.id, f'Гугл таблица в cloud загружена')


            # ..................telegram_little(сводная таблица)
            task_telegram_little = asyncio.create_task(get_telegram_little(message.chat.id, bot, df, kpi,
                                                                      def_sum_show, def_sum_describe))
            little = await task_telegram_little

            # Обновление таблицы "telegram_little" в базу
            n_rows = get_table_size(baza, 'telegram_little', user, password, host, port)[0]
            drop_inerval(baza, 'telegram_little', user, password, host, port, n_rows - n_rows, n_rows - 1)
            insert_target(baza, 'telegram_little', user, password, host, port, little)
            await bot.send_message(message.chat.id, f'Таблица загружена в БД')
        except Exception as e:
            print(e)
            await message.reply(e)
    else:
        await bot.send_message(message.chat.id, 'У вас нет паспорта')


# Обновление таблицы по kpi (участвует)
@dp.message_handler(lambda message: 'kpi' in message.document.file_name.lower(),
                    content_types=['document'])
async def target_telegram(message):
    users = pd.read_csv('users_telegram.txt')
    username = message.from_user.username
    access = username in users['User_name'].values
    if access:
        try:
            # Загрузка файла из беседы
            file_info = await bot.get_file(message.document.file_id)

            # Обрабатываем и обновляем таблицу "telegram_kpi"
            kpi = pd.read_excel(file_info.file_path)

            kpi.rename(columns=lambda x: x[:33], inplace=True) # режем названия до длины (Прогнозируемое кол-во подписчиков)

            # нормализуем имена, чтобы они смогли соединиться с координатами
            reg_names = kpi['Регион']
            reg_names = list(clean_names(reg_names, drop_names))
            reg_names = get_normal_names(base_names, reg_names)
            kpi['Регион'] = reg_names

            # прочищаем щели полю с грязью
            kpi['Название канала'] = kpi['Название канала'].apply(lambda x: clean_str(x, '[^a-zA-Zа-яА-я0-9-. ]'))

            # Обновление таблицы "telegram" в базе
            n_rows = get_table_size(baza, 'telegram_kpi', user, password, host, port)[0]
            drop_inerval(baza, 'telegram_kpi', user, password, host, port, n_rows - n_rows, n_rows - 1)
            insert_target(baza, 'telegram_kpi', user, password, host, port, kpi)
            await bot.send_message(message.chat.id, f'''Таблица 'telegram_kpi' загружена в БД''')
        except Exception as e:
            print(e)
            await message.reply(e)
    else:
        await bot.send_message(message.chat.id, 'У вас нет паспорта')


# Запускаем бота
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)






