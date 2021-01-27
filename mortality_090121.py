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

# # Анализ динамики смертности по группам МКБ. Рост три месяца подряд


def death_rule_second_new():
    start_time = datetime.now()
    program = 'death_rule_second_new1'
    logging.info('{} started'.format(program))
    print('{} started'.format(program))  
    
    # Расчет показателей

    # # Таблица с численностью населения
    df_population_mo = pd.DataFrame(columns=['Region', 'Year', 'Population'])
    k = 0
    for region in REGION[:]:
        for year in YEARS[1:]:
            population = df_Population[df_Population.Region.isin([region]) &
                                       df_Population.Year.isin([year]) &
                                       df_Population.AGE_GROUP.isin(['Всего'])].Population.sum()
            df_population_mo.loc[k] = {'Region': region, 'Year': year, 'Population': population}
            k += 1

    # # Количество смертей за месяц + временной коэффициент
    df_amount_death = pd.DataFrame(columns=['Region', 'MKB', 'DATE', 'AmountDeathAll', 'AmountDeathMKB'])
    k = 0
    for region in REGION[:]:
        for MKB in MKB_GROUP_LIST_MAIN[:]:
            for Date in DATES:
                df_amount_death.loc[k] = {'Region': region, 'MKB': MKB, 'DATE': Date,
                                          'AmountDeathAll': len(df_START[(df_START['district_location'].isin([region])) &
                                                                         (df_START['DATE'].isin([Date]))]),
                                          'AmountDeathMKB': len(df_START[(df_START['district_location'].isin([region])) &
                                                                         (df_START['DATE'].isin([Date])) &
                                                                         (df_START['MKB_GROUP_NAME_original_reason'].isin([MKB]))])}
                k += 1   

    for i in df_amount_death.index:
        df_amount_death.loc[i, 'Year'] = df_amount_death.loc[i, 'DATE'].year
        df_amount_death.loc[i, 'Month'] = df_amount_death.loc[i, 'DATE'].month
        df_amount_death.loc[i, 'time_factor_month'] = time_factor_calculation(df_amount_death.loc[i, 'Year'],
                                                                              df_amount_death.loc[i, 'Month'])[0]
        df_amount_death.loc[i, 'time_factor_period'] = time_factor_calculation(df_amount_death.loc[i, 'Year'],
                                                                               df_amount_death.loc[i, 'Month'])[1]

    df_amount_death = df_amount_death[~df_amount_death.Year.isin([2017])]
    df_amount_death.index = range(df_amount_death.shape[0])

    # Рабочая таблица

    # # Базовые вычисления
    df_operating = df_amount_death.merge(df_population_mo, how='left', on=['Region', 'Year'])

    # Если данные о численности еще отсутствуют, то берем данные за предыдущий год
    for i in df_operating.index:
        if pd.isnull(df_operating.loc[i, 'Population']):
            region = df_operating.loc[i, 'Region']
            YearNULL = df_operating.loc[i, 'Year']
            YearNotNULL = YearNULL - 1
            population = df_operating[df_operating.Region.isin([region]) &
                                      df_operating.Year.isin([YearNotNULL])].Population.values[0]
            df_operating.loc[i, 'Population'] = population

    for i in df_operating.index:
        df_operating.loc[i, 'AmountDeath/Population*time_factor_month'] = round(df_operating.loc[i, 'AmountDeathMKB'] /
                                                                                df_operating.loc[i, 'Population'] *
                                                                                100000 *
                                                                                df_operating.loc[i, 'time_factor_month'], 2)

    # Поиск аномалий.

    # # Рост смертности три периода подряд.
    df_Results = pd.DataFrame(columns=['Region', 'MKB', 'DATE', 'Year', 'Month', 'meaning_last3'])
    main_column = 'AmountDeath/Population*time_factor_month'

    k = 0
    for MKB in MKB_GROUP_LIST_MAIN:
        for region in REGION:
            temp = df_operating[df_operating.MKB.isin([MKB]) & df_operating.Region.isin([region])]
            for i in temp.index[3:]:
                if (temp.loc[i, main_column] > temp.loc[i-1, main_column]) & (temp.loc[i-1, main_column] > temp.loc[i-2, main_column]) & (temp.loc[i-2, main_column] > temp.loc[i-3, main_column]):
                    df_Results.loc[k] = {'Region': temp.loc[i, 'Region'],
                                         'MKB': temp.loc[i, 'MKB'],
                                         'DATE': temp.loc[i, 'DATE'],
                                         'Year': temp.loc[i, 'Year'],
                                         'Month': temp.loc[i, 'Month'],
                                         'meaning_last3': '{}: {},{}: {},{}: {},{}: {}'.format(temp.loc[i-3, 'DATE'],
                                                                                               temp.loc[i-3, main_column],
                                                                                               temp.loc[i-2, 'DATE'],
                                                                                               temp.loc[i-2, main_column],
                                                                                               temp.loc[i-1, 'DATE'],
                                                                                               temp.loc[i-1, main_column],
                                                                                               temp.loc[i, 'DATE'],
                                                                                               temp.loc[i, main_column])}
                    k += 1

    year = YEARS[-1]
    Date = DATES[-1]
    # за последний месяц
    RESULTS_blowout = df_Results[df_Results.Year.isin([year]) & df_Results.DATE.isin([Date])]

    # # Формируем результат работы и записываем в БД
    output = pd.DataFrame(columns=['recipient', 'message', 'deadline', 'release',
                                   'task_type', 'title', 'fio_recipient'])

    if len(pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)) == 0:
        k = 0
    else:
        k = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx).id.max()+1

    for i in RESULTS_blowout.index:
        if (RESULTS_blowout.loc[i, 'Region'] == 'Липецк') | (RESULTS_blowout.loc[i, 'Region'] == 'Елец'):
            corr = ''
        else:
            corr = 'район'
        recipient = 'Главный врач ЦРБ {} {}'.format(RESULTS_blowout.loc[i, 'Region'], corr)
        message = 'Проанализировать и принять меры по снижению смертности. На протяжении последних трех месяцев в районе наблюдается рост смертности от заболеваний из Группы {}'.format(RESULTS_blowout.loc[i, 'MKB'])

        MKB = MKB_GROUP_LIST_MAIN.index(RESULTS_blowout.loc[i, 'MKB'])
        task_type = 'Смертность_П2_new1_{}'.format(MKB)
        
        if RESULTS_blowout.loc[i, 'DATE'].month == 12:
            release = date(RESULTS_blowout.loc[i, 'DATE'].year+1, 1, 1)
        else:
            release = date(RESULTS_blowout.loc[i, 'DATE'].year, RESULTS_blowout.loc[i, 'DATE'].month+1, 1)
            
        title = 'Рост смертности от заболеваний из группы {}'.format(RESULTS_blowout.loc[i, 'MKB'])
        if recipient in FIO_dict.keys():
            FIO = FIO_dict[recipient]
        else:
            FIO = ''    
        output.loc[k] = {'task_type': task_type, 
                         'recipient': recipient, 
                         'message': 'ИСУ обычная {}'.format(message), 
                         'release': release,
                         'deadline': str(date.today() + pd.Timedelta(days=14)),
                         'title': title, 'fio_recipient': FIO}
        k += 1
    output.to_sql('test_output', cnx, if_exists='append', index_label='id')

    print('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    print('Number of generated tasks {}'.format(len(output)))
    logging.info('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    logging.info('Number of generated tasks {}'.format(len(output)))    

    start_time = datetime.now()
    program = 'death_rule_second_new2'
    logging.info('{} started'.format(program))
    print('{} started'.format(program))

    # Поиск аномалий.

    # # Сравнение с аналогичным периодом прошлого года.
    df_Results = pd.DataFrame(columns=['Region', 'MKB', 'DATE', 'Year', 'Month', 'AmountDeathMKB_T',
                                       'AmountDeathMKB_T-1', 'AmountDeath/Population*time_factor_month_T',
                                       'AmountDeath/Population*time_factor_month_T-1', 'increase_deaths'])
    k = 0
    for MKB in MKB_GROUP_LIST_MAIN:
        for region in REGION:
            temp = df_operating[df_operating.MKB.isin([MKB]) & df_operating.Region.isin([region])]
            for i in temp.index[:]:
                if temp.loc[i, 'Year'] > 2018:
                    TheSamePeriodLastYear = date(int(temp.loc[i, 'Year']-1), int(temp.loc[i, 'Month']), 1)
                    AmDeathMKB1 = temp.loc[i, 'AmountDeathMKB']
                    AmDeathMKB0 = temp[temp.DATE.isin([TheSamePeriodLastYear])]['AmountDeathMKB'].values[0]
                    AmountDeathMKB1 = temp.loc[i, 'AmountDeath/Population*time_factor_month']
                    AmountDeathMKB0 = temp[temp.DATE.isin([TheSamePeriodLastYear])]['AmountDeath/Population*time_factor_month'].values[0]
                    increase_deaths = round(AmountDeathMKB1/AmountDeathMKB0, 2)     

                    df_Results.loc[k] = {'Region': region, 'MKB': MKB,
                                         'DATE': temp.loc[i, 'DATE'],
                                         'Year': temp.loc[i, 'Year'],
                                         'Month': temp.loc[i, 'Month'],
                                         'AmountDeathMKB_T': AmDeathMKB1, 
                                         'AmountDeathMKB_T-1': AmDeathMKB0,
                                         'AmountDeath/Population*time_factor_month_T': AmountDeathMKB1, 
                                         'AmountDeath/Population*time_factor_month_T-1': AmountDeathMKB0,
                                         'increase_deaths': increase_deaths}
                    k += 1

    year = YEARS[-1]
    Date = DATES[-1]
    # за последний месяц
    UpperBound = 1.5
    MinimumAmountDeathMKBTheSamePeriodLastYear = 5
    RESULTS_blowout = df_Results[df_Results.Year.isin([year]) & df_Results.DATE.isin([Date]) &
                                 (df_Results.increase_deaths > UpperBound) &
                                 (df_Results['AmountDeathMKB_T-1'] > MinimumAmountDeathMKBTheSamePeriodLastYear) &
                                 (df_Results.increase_deaths != np.inf)].sort_values('increase_deaths',ascending = False)

    ##Формируем результат работы и записываем в БД
    output = pd.DataFrame(columns = ['recipient','message','deadline', 'release', 'task_type', 'title', 'fio_recipient'])

    if len(pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)) == 0:
        k=0
    else:
        k=pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx).id.max()+1

    for i in RESULTS_blowout.index:  

        if ((RESULTS_blowout.loc[i,'Region']=='Липецк')|(RESULTS_blowout.loc[i,'Region']=='Елец')):
            corr = ''
        else:
            corr = 'район'

        recipient = 'Главный врач ЦРБ {} {}'.format(RESULTS_blowout.loc[i,'Region'], corr)
        message = 'Проанализировать и принять меры по снижению смертности. В районе по сравнению с аналогичным периодом прошлого года наблюдается значительный рост смертности от заболеваний из Группы {}'.format(RESULTS_blowout.loc[i,'MKB'])

        MKB = MKB_GROUP_LIST_MAIN.index(RESULTS_blowout.loc[i,'MKB'])
        task_type = 'Смертность_П2_new2_{}'.format(MKB)
        
        if RESULTS_blowout.loc[i,'DATE'].month == 12:
            release = date(RESULTS_blowout.loc[i,'DATE'].year+1,1,1)
        else:
            release = date(RESULTS_blowout.loc[i,'DATE'].year,RESULTS_blowout.loc[i,'DATE'].month+1,1)
        
        title = 'Рост смертности от заболеваний из группы {} по сравнению с АППГ'.format(RESULTS_blowout.loc[i,'MKB'])
        if recipient in FIO_dict.keys():
            FIO = FIO_dict[recipient]
        else:
            FIO = ''    
        output.loc[k] = {'task_type': task_type, 
                         'recipient': recipient, 
                         'message': 'ИСУ обычная {}'.format(message), 
                         'release': release,
                         'deadline': str(date.today() + pd.Timedelta(days=14)),
                         'title': title, 'fio_recipient': FIO}
        k+=1  
    output.to_sql('test_output', cnx, if_exists='append', index_label='id')

    print('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    print('Number of generated tasks {}'.format(len(output)))
    logging.info('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    logging.info('Number of generated tasks {}'.format(len(output)))


# # Эскалация задач
def death_escalation():
    start_time = datetime.now()
    program = 'death_escalation'
    logging.info('{} started'.format(program))
    print('{} started'.format(program))

    escalation_recipient_list = {1: 'Начальник Управления здравоохранения',
                                 2: 'Заместитель главы администрации (вопросы здравоохранения, соц.защиты, труда и занятости населения, демографической политики)',
                                 3: 'Глава администрации'}

    escalation_recipient_text = {1: 'Разобраться.',
                                 2: 'Принять меры.',
                                 3: 'Заслушать доклад.'}

    #Считаем задачи
    df_TASK_LIST = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)
    DATES = sorted(df_TASK_LIST.release.unique())
    DATES = DATES[-4:]

    df_TASK_LIST = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)
    df_TASK_LIST = df_TASK_LIST[df_TASK_LIST.release.isin(DATES)]

    df_task_count_list = pd.DataFrame(columns = ['task_type', 'recipient', 'task_count'])

    k = 0

    for i in df_TASK_LIST.task_type.unique():
        for j in df_TASK_LIST.recipient.unique():
            df_task_count_list.loc[k] = {'task_type': i, 'recipient': j, 
                                         'task_count': len(df_TASK_LIST[df_TASK_LIST.task_type.isin([i]) &
                                                                   df_TASK_LIST.recipient.isin([j])])}
            k += 1

    # оставляем только задачи, которые выставлялись на исполнителя более одного раза
    df_task_count_list = df_task_count_list[df_task_count_list.task_count>1]
    df_task_count_list = df_task_count_list.sort_values(['task_count'],ascending = False)
    df_task_count_list.index = range(df_task_count_list.shape[0])

    #Итоговая реализация алгоритма
    RESULTS = pd.DataFrame(columns = ['task_type', 'recipient', 'message','release', 'escalation_level'])

    k = 0
    for i in range(len(df_task_count_list)):
        temp = df_TASK_LIST[df_TASK_LIST.recipient.isin([df_task_count_list.loc[i,'recipient']]) &
                            df_TASK_LIST.task_type.isin([df_task_count_list.loc[i,'task_type']])]

        temp_dates = sorted(temp.release.values)

        for i in range(len(temp_dates)):

            temp_dates_ = temp_dates[i:]

            if (len(temp_dates_) - 1) == 0:
                break

            if (DATES.index(temp_dates_[-1]) - DATES.index(temp_dates_[-2]) > 1):
                escalation_level = 'Эскалация не требуется'
                break

            if (DATES.index(temp_dates_[-1]) - DATES.index(temp_dates_[0]) == len(temp_dates_) - 1):
                if (len(temp_dates_) - 1) > 3:
                    escalation_level = 3
                else:
                    escalation_level = len(temp_dates_) - 1
                break

        RESULTS.loc[k] = {'task_type': temp.task_type.unique()[0], 
                          'recipient': temp.recipient.unique()[0], 
                          'message': df_TASK_LIST[df_TASK_LIST.task_type.isin([temp.task_type.unique()[0]]) &
                                                  df_TASK_LIST.recipient.isin([temp.recipient.unique()[0]]) &
                                                  df_TASK_LIST.release.isin([temp_dates[-1]])].message.values[0],
                          'release': temp_dates[-1], 
                          'escalation_level': escalation_level}
        k += 1 

    # Здесь задать период, за который делаем эскалацию (последний месяц, два месяца и т.д.)
    RESULTS = RESULTS[RESULTS.release.isin(DATES[-1:]) & ~RESULTS.escalation_level.isin(['Эскалация не требуется'])]
    for i in RESULTS.index:
        #message = RESULTS.loc[i, 'message']
        removal_list = ['ИСУ обычная ', 'ИСУ предупреждение ', 'Проанализировать и принять меры по снижению смертности. ']
        for word in removal_list:
            RESULTS.loc[i, 'message'] = RESULTS.loc[i, 'message'].replace(word, '')

    doctors = []
    leaders = []
    for i in RESULTS.index:
        if RESULTS.loc[i,'recipient'].find('Главный врач')==0:
            doctors.append(i)
        if RESULTS.loc[i,'recipient'].find('Глава МО')==0:
            leaders.append(i)      

    # Для главврачей
    RESULTS_doctors = RESULTS[RESULTS.index.isin(doctors)]
    output = pd.DataFrame(columns = ['escalation_recipient', 'task_type', 'original_recipient', 'message', 'release',
                                     'deadline', 'title', 'fio_recipient'])

    if len(pd.read_sql_query('''SELECT * FROM public."death_escalation_output"''', cnx)) == 0:
        k=0
    else:
        k=pd.read_sql_query('''SELECT * FROM public."death_escalation_output"''', cnx).id.max()+1

    for i in RESULTS_doctors.index:
        escalation_level = RESULTS_doctors.loc[i,'escalation_level']

        if escalation_recipient_list[escalation_level] in FIO_dict.keys():
                FIO = FIO_dict[escalation_recipient_list[escalation_level]]
        else:
                FIO = ''

        output.loc[k] = {'escalation_recipient': escalation_recipient_list[escalation_level], 
                         'task_type': RESULTS_doctors.loc[i,'task_type'], 
                         'original_recipient': RESULTS_doctors.loc[i,'recipient'], 
                         'message': 'ИСУ эскалация ' + 'Эскалированная задача. {} Первоначальный исполнитель: {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_text[escalation_level],
                                                                                                                                                        RESULTS_doctors.loc[i,'recipient'],
                                                                                                                                                        RESULTS_doctors.loc[i,'message'],
                                                                                                                                                        (escalation_level+1)),
                         'release': RESULTS_doctors.loc[i,'release'],
                         'deadline': str(date.today() + pd.Timedelta(days=14)),
                         'title': 'Эскалированная задача. {}'.format(escalation_recipient_text[escalation_level]), 
                         'fio_recipient': FIO}    
        if escalation_level>1:
            for j in range(1, escalation_level):
                if escalation_recipient_list[escalation_level-j] in FIO_dict.keys():
                        FIO = FIO_dict[escalation_recipient_list[escalation_level-j]]
                else:
                        FIO = ''
                output.loc[k+escalation_level-j] = {'escalation_recipient': escalation_recipient_list[escalation_level-j], 
                                                    'task_type': RESULTS_doctors.loc[i,'task_type'], 
                                                    'original_recipient': RESULTS_doctors.loc[i,'recipient'], 
                                                    'message': 'ИСУ предупреждение ' + 'Задача эскалирована на уровень - {}. Первоначальный исполнитель: {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_list[escalation_level],
                                                                                                                                                                                              RESULTS_doctors.loc[i,'recipient'],
                                                                                                                                                                                              RESULTS_doctors.loc[i,'message'], 
                                                                                                                                                                                              (escalation_level+1)),
                                                    'release': RESULTS_doctors.loc[i,'release'],
                                                    'deadline': str(date.today() + pd.Timedelta(days=14)),
                                                    'title': 'Задача эскалирована на уровень - {}.'.format(escalation_recipient_list[escalation_level]),
                                                    'fio_recipient': FIO}    
        if RESULTS_doctors.loc[i,'recipient'] in FIO_dict.keys():
                FIO = FIO_dict[RESULTS_doctors.loc[i,'recipient']]
        else:
                FIO = ''    
        output.loc[k+escalation_level] = {'escalation_recipient': RESULTS_doctors.loc[i,'recipient'], 
                                          'task_type': RESULTS_doctors.loc[i,'task_type'], 
                                          'original_recipient': RESULTS_doctors.loc[i,'recipient'], 
                                          'message': 'ИСУ предупреждение ' + 'Ваша задача эскалирована на уровень - {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_list[escalation_level],
                                                                                                                                                         RESULTS_doctors.loc[i,'message'], 
                                                                                                                                                         (escalation_level+1)),
                                          'release': RESULTS_doctors.loc[i,'release'],
                                          'deadline': str(date.today() + pd.Timedelta(days=14)),
                                          'title': 'Ваша задача эскалирована на уровень - {}.'.format(escalation_recipient_list[escalation_level]),
                                          'fio_recipient': FIO}
        k = k+1+escalation_level
    output.to_sql('death_escalation_output', cnx, if_exists='append', index_label='id')

    # Для глав МО
    RESULTS_leaders = RESULTS[RESULTS.index.isin(leaders)]
    output = pd.DataFrame(columns = ['escalation_recipient', 'task_type', 'original_recipient', 'message', 'release',
                                     'deadline', 'title', 'fio_recipient'])

    if len(pd.read_sql_query('''SELECT * FROM public."death_escalation_output"''', cnx)) == 0:
        k=0
    else:
        k=pd.read_sql_query('''SELECT * FROM public."death_escalation_output"''', cnx).id.max()+1

    for i in RESULTS_leaders.index:
        escalation_level = RESULTS_leaders.loc[i,'escalation_level']    
        if escalation_level == 3:  
            if escalation_recipient_list[escalation_level] in FIO_dict.keys():
                    FIO = FIO_dict[escalation_recipient_list[escalation_level]]
            else:
                    FIO = ''
            output.loc[k] = {'escalation_recipient': escalation_recipient_list[escalation_level], 
                             'task_type': RESULTS_leaders.loc[i,'task_type'], 
                             'original_recipient': RESULTS_leaders.loc[i,'recipient'], 
                             'message': 'ИСУ эскалация ' + 'Эскалированная задача. {} Первоначальный исполнитель: {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_text[escalation_level],
                                                                                                                                                            RESULTS_leaders.loc[i,'recipient'],
                                                                                                                                                            RESULTS_leaders.loc[i,'message'],
                                                                                                                                                            (escalation_level+1)),
                             'release': RESULTS_leaders.loc[i,'release'],
                             'deadline': str(date.today() + pd.Timedelta(days=14)),
                             'title': 'Эскалированная задача. {}'.format(escalation_recipient_text[escalation_level]), 
                             'fio_recipient': FIO}



            if RESULTS_leaders.loc[i,'recipient'] in FIO_dict.keys():
                    FIO = FIO_dict[RESULTS_doctors.loc[i,'recipient']]
            else:
                    FIO = ''
            output.loc[k+escalation_level] = {'escalation_recipient': RESULTS_leaders.loc[i,'recipient'], 
                                              'task_type': RESULTS_leaders.loc[i,'task_type'], 
                                              'original_recipient': RESULTS_leaders.loc[i,'recipient'], 
                                              'message': 'ИСУ предупреждение ' + 'Ваша задача эскалирована на уровень - {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_list[escalation_level],
                                                                                                                                                             RESULTS_leaders.loc[i,'message'], 
                                                                                                                                                             (escalation_level+1)),
                                              'release': RESULTS_leaders.loc[i,'release'],
                                              'deadline': str(date.today() + pd.Timedelta(days=14)),
                                              'title': 'Ваша задача эскалирована на уровень - {}.'.format(escalation_recipient_list[escalation_level]),
                                              'fio_recipient': FIO}
            k = k+2    
    output.to_sql('death_escalation_output', cnx, if_exists='append', index_label='id')

    print('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    print('Number of generated escalation tasks {}'.format(len(output)))
    logging.info('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    logging.info('Number of generated escalation tasks {}'.format(len(output)))


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
        death_preprocessing()

        #Загрузка исходных данных
        df_START = pd.read_sql_query('''SELECT * FROM public."death_finished"''', cnx)
        df_MKB = pd.read_sql_query('''SELECT * FROM public."MKB"''', cnx)
        df_FIO = pd.read_sql_query('''SELECT * FROM public."fio_recipient"''', cnx)
        df_Population = pd.read_sql_query('''SELECT * FROM public."Population"''', cnx)

        ##Основные списки
        REGION = ['Воловский', 'Грязинский', 'Данковский', 'Добринский', 'Добровский', 'Долгоруковский',
                  'Елецкий', 'Задонский', 'Измалковский', 'Краснинский', 'Лебедянский', 'Лев-Толстовский', 
                  'Липецкий', 'Становлянский', 'Тербунский',  'Усманский', 'Хлевенский', 'Чаплыгинский', 
                  'Елец', 'Липецк']

        YEARS = sorted(df_START['year_death'].unique())
        MONTHS = sorted(df_START['month_death'].unique())
        DATES = sorted(df_START['DATE'].unique())

        GENDERS = sorted(df_START['gender'].unique())
        AGE_GROUPS = sorted(df_START['age_group_death'].unique())

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
