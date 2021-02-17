import warnings
import pandas as pd
import numpy as np
import calendar
from datetime import date, datetime
from dateutil import relativedelta as rdelta
from operator import is_not
from functools import partial
from sqlalchemy import create_engine
import statsmodels.api as sm
import logging
from ISU_death_functions import time_factor_calculation, amount_days_in_month
from preprocessing import death_preprocessing

warnings.filterwarnings('ignore')


# # Main
logging.basicConfig(filename='logfile.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logging.info('Start of the mortality analysis algorithm')
start_time_ALL = datetime.now()
print('Start of the mortality analysis algorithm')
try:

    POSTGRES_ADDRESS = '10.248.23.152'
    POSTGRES_PORT = '5432'
    POSTGRES_USERNAME = 'isu'
    POSTGRES_PASSWORD = 'isupass'
    POSTGRES_DBNAME = 'isu_db'

    postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME, 
                                                                                            password=POSTGRES_PASSWORD, 
                                                                                            ipaddress=POSTGRES_ADDRESS, 
                                                                                            port=POSTGRES_PORT, 
                                                                                            dbname=POSTGRES_DBNAME))
    cnx = create_engine(postgres_str)

    lastDate = pd.read_sql_query('''SELECT MAX(death) FROM public."death"''', cnx).values[0]
    print(lastDate)
    #lastDate = date(2020,11,30)
    #print(lastDate)

    if amount_days_in_month(lastDate)==True:
        print('The month is over. Start forming tasks ...')
        REGION = ['Воловский', 'Грязинский', 'Данковский', 'Добринский', 'Добровский', 'Долгоруковский',
                  'Елецкий', 'Задонский', 'Измалковский', 'Краснинский', 'Лебедянский', 'Лев-Толстовский',
                  'Липецкий', 'Становлянский', 'Тербунский', 'Усманский', 'Хлевенский', 'Чаплыгинский',
                  'Елец', 'Липецк']
        df_MKB = pd.read_sql_query('''SELECT * FROM public."MKB"''', cnx)
        df_FIO = pd.read_sql_query('''SELECT * FROM public."fio_recipient"''', cnx)
        df_Population = pd.read_sql_query('''SELECT * FROM public."Population"''', cnx)
        MKB_CODE_LIST = NotNaNFilter(df_MKB, 'MKB_CODE')
        MKB_GROUP_LIST = NotNaNFilter(df_MKB, 'MKB_GROUP_NAME')
        MKB_GROUP_LIST_MAIN = ['НОВООБРАЗОВАНИЯ (C00-D48)',
                               'ПСИХИЧЕСКИЕ РАССТРОЙСТВА И РАССТРОЙСТВА ПОВЕДЕНИЯ (F00-F99)',
                               'БОЛЕЗНИ ЭНДОКРИННОЙ СИСТЕМЫ, РАССТРОЙСТВА ПИТАНИЯ И НАРУШЕНИЯ ОБМЕНА ВЕЩЕСТВ (E00-E90)',
                               'БОЛЕЗНИ НЕРВНОЙ СИСТЕМЫ (G00-G99)', 'БОЛЕЗНИ СИСТЕМЫ КРОВООБРАЩЕНИЯ (I00-I99)',
                               'БОЛЕЗНИ ОРГАНОВ ДЫХАНИЯ (J00-J99)', 'БОЛЕЗНИ ОРГАНОВ ПИЩЕВАРЕНИЯ (K00-K93)',
                               'СИМПТОМЫ, ПРИЗНАКИ И ОТКЛОНЕНИЯ ОТ НОРМЫ, ВЫЯВЛЕННЫЕ ПРИ КЛИНИЧЕСКИХ И ЛАБОРАТОРНЫХ ИССЛЕДОВАНИЯХ, НЕ КЛАССИФИЦИРОВАННЫЕ В ДРУГИХ РУБРИКАХ (R00-R99)',
                               'ТРАВМЫ, ОТРАВЛЕНИЯ И НЕКОТОРЫЕ ДРУГИЕ ПОСЛЕДСТВИЯ ВОЗДЕЙСТВИЯ ВНЕШНИХ ПРИЧИН (S00-T98)']
        FIO_dict = dict(zip(df_FIO.position, df_FIO.fio))
        MONTH_number = list(range(1, 13))
        MONTH_name = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 'Июль', 'Август', 'Сентябрь',
                      'Октябрь', 'Ноябрь', 'Декабрь']
        MONTHS_dict = dict(zip(MONTH_number, MONTH_name))
        df_Population = df_Population[(df_Population['Region'].isin(REGION)) &
                                      (df_Population['Territory'].isin(['Все население'])) &
                                      (df_Population['Gender'].isin(['Оба пола']))]
        df_Population.index = range(df_Population.shape[0])
        df_Population.columns = ['id', 'Feature', 'Region', 'Territory', 'GENDER', 'AGE_GROUP', 'Year', 'Population']

        death_preprocessing()

        #Загрузка исходных данных
        df_START = pd.read_sql_query('''SELECT * FROM public."death_finished"''', cnx)
        ##Основные списки
        YEARS = sorted(df_START['year_death'].unique())
        MONTHS = sorted(df_START['month_death'].unique())
        DATES = sorted(df_START['DATE'].unique())
        GENDERS = sorted(df_START['gender'].unique())
        AGE_GROUPS = sorted(df_START['age_group_death'].unique())

        death_rule_first_55()    
        death_rule_second_new()  
        death_escalation()

        print('The end of the mortality analysis algorithm. elapsed time {}'.format((datetime.now() - start_time_ALL)))

    else:
        print('The month is not over yet.')
        print('The end of the mortality analysis algorithm. elapsed time {}'.format((datetime.now() - start_time_ALL)))
        logging.info('The month is not over yet.')
        logging.info('The end of the mortality analysis algorithm. elapsed time {}'.format((datetime.now() -
                                                                                            start_time_ALL)))
        
except Exception as e:
    logging.exception('Exception occurred')
    logging.info('The execution of the mortality analysis algorithm was not completed due to an error') 
