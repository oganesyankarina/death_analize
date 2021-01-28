# Анализ динамики смертности по группам МКБ. Рост три месяца подряд и по сравнению с АППГ
from datetime import date, datetime
import pandas as pd
import numpy as np
import logging
import openpyxl
from connect_PostGres import cnx
from ISU_death_lists_dict import df_Population, REGION, MONTHS_dict, FIO_dict, MKB_GROUP_LIST_MAIN
from get_from_death_finished import get_df_death_finished
from ISU_death_functions import time_factor_calculation


def death_rule_second_new():
    start_time = datetime.now()
    program = 'death_rule_second_new1'
    logging.info('{} started'.format(program))
    print('{} started'.format(program))

    df_death_finished, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS = get_df_death_finished()
    # df_death_finished = df_death_finished

    # Расчет показателей
    # Таблица с численностью населения
    df_population_mo = pd.DataFrame(columns=['Region', 'Year', 'Population'])
    k = 0
    for region in REGION[:]:
        for year in YEARS[1:]:
            population = df_Population[df_Population.Region.isin([region]) &
                                       df_Population.Year.isin([year]) &
                                       df_Population.AGE_GROUP.isin(['Всего'])].Population.sum()
            df_population_mo.loc[k] = {'Region': region, 'Year': year, 'Population': population}
            k += 1

    # Количество смертей за месяц + временной коэффициент
    df_amount_death = pd.DataFrame(columns=['Region', 'MKB', 'DATE', 'AmountDeathAll', 'AmountDeathMKB'])
    k = 0
    for region in REGION[:]:
        for MKB in MKB_GROUP_LIST_MAIN[:]:
            for Date in DATES:
                df_amount_death.loc[k] = {'Region': region, 'MKB': MKB, 'DATE': Date,
                                          'AmountDeathAll': len(
                                              df_death_finished[(df_death_finished['district_location'].isin([region]))
                                                                & (df_death_finished['DATE'].isin([Date]))]),
                                          'AmountDeathMKB': len(df_death_finished[
                                                                (df_death_finished['district_location'].isin([region]))
                                                                & (df_death_finished['DATE'].isin([Date]))
                                                                & (df_death_finished['MKB_GROUP_NAME_original_reason'].isin([MKB]))])}
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
    # Базовые вычисления
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
                                                                                df_operating.loc[
                                                                                    i, 'time_factor_month'], 2)

    # Поиск аномалий.
    # Рост смертности три периода подряд.
    df_Results = pd.DataFrame(columns=['Region', 'MKB', 'DATE', 'Year', 'Month', 'meaning_last3'])
    main_column = 'AmountDeath/Population*time_factor_month'

    k = 0
    for MKB in MKB_GROUP_LIST_MAIN:
        for region in REGION:
            temp = df_operating[df_operating.MKB.isin([MKB]) & df_operating.Region.isin([region])]
            for i in temp.index[3:]:
                if (temp.loc[i, main_column] > temp.loc[i - 1, main_column]) & (
                        temp.loc[i - 1, main_column] > temp.loc[i - 2, main_column]) & (
                        temp.loc[i - 2, main_column] > temp.loc[i - 3, main_column]):
                    df_Results.loc[k] = {'Region': temp.loc[i, 'Region'],
                                         'MKB': temp.loc[i, 'MKB'],
                                         'DATE': temp.loc[i, 'DATE'],
                                         'Year': temp.loc[i, 'Year'],
                                         'Month': temp.loc[i, 'Month'],
                                         'meaning_last3': '{}: {},{}: {},{}: {},{}: {}'.format(temp.loc[i - 3, 'DATE'],
                                                                                               temp.loc[
                                                                                                   i - 3, main_column],
                                                                                               temp.loc[i - 2, 'DATE'],
                                                                                               temp.loc[
                                                                                                   i - 2, main_column],
                                                                                               temp.loc[i - 1, 'DATE'],
                                                                                               temp.loc[
                                                                                                   i - 1, main_column],
                                                                                               temp.loc[i, 'DATE'],
                                                                                               temp.loc[
                                                                                                   i, main_column])}
                    k += 1

    year = YEARS[-1]
    Date = DATES[-1]
    # за последний месяц
    RESULTS_blowout = df_Results[df_Results.Year.isin([year]) & df_Results.DATE.isin([Date])]

    # Формируем результат работы и записываем в БД
    output = pd.DataFrame(columns=['recipient', 'message', 'deadline', 'release',
                                   'task_type', 'title', 'fio_recipient'])

    if len(pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)) == 0:
        k = 0
    else:
        k = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx).id.max() + 1

    for i in RESULTS_blowout.index:
        if (RESULTS_blowout.loc[i, 'Region'] == 'Липецк') | (RESULTS_blowout.loc[i, 'Region'] == 'Елец'):
            corr = ''
        else:
            corr = 'район'
        recipient = 'Главный врач ЦРБ {} {}'.format(RESULTS_blowout.loc[i, 'Region'], corr)
        message = 'Проанализировать и принять меры по снижению смертности. На протяжении последних трех месяцев в районе наблюдается рост смертности от заболеваний из Группы {}'.format(
            RESULTS_blowout.loc[i, 'MKB'])

        MKB = MKB_GROUP_LIST_MAIN.index(RESULTS_blowout.loc[i, 'MKB'])
        task_type = 'Смертность_П2_new1_{}'.format(MKB)

        if RESULTS_blowout.loc[i, 'DATE'].month == 12:
            release = date(RESULTS_blowout.loc[i, 'DATE'].year + 1, 1, 1)
        else:
            release = date(RESULTS_blowout.loc[i, 'DATE'].year, RESULTS_blowout.loc[i, 'DATE'].month + 1, 1)

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
                    TheSamePeriodLastYear = date(int(temp.loc[i, 'Year'] - 1), int(temp.loc[i, 'Month']), 1)
                    AmDeathMKB1 = temp.loc[i, 'AmountDeathMKB']
                    AmDeathMKB0 = temp[temp.DATE.isin([TheSamePeriodLastYear])]['AmountDeathMKB'].values[0]
                    AmountDeathMKB1 = temp.loc[i, 'AmountDeath/Population*time_factor_month']
                    AmountDeathMKB0 = \
                    temp[temp.DATE.isin([TheSamePeriodLastYear])]['AmountDeath/Population*time_factor_month'].values[0]
                    increase_deaths = round(AmountDeathMKB1 / AmountDeathMKB0, 2)

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
                                 (df_Results.increase_deaths != np.inf)].sort_values('increase_deaths', ascending=False)

    ##Формируем результат работы и записываем в БД
    output = pd.DataFrame(
        columns=['recipient', 'message', 'deadline', 'release', 'task_type', 'title', 'fio_recipient'])

    if len(pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)) == 0:
        k = 0
    else:
        k = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx).id.max() + 1

    for i in RESULTS_blowout.index:

        if ((RESULTS_blowout.loc[i, 'Region'] == 'Липецк') | (RESULTS_blowout.loc[i, 'Region'] == 'Елец')):
            corr = ''
        else:
            corr = 'район'

        recipient = 'Главный врач ЦРБ {} {}'.format(RESULTS_blowout.loc[i, 'Region'], corr)
        message = 'Проанализировать и принять меры по снижению смертности. В районе по сравнению с аналогичным периодом прошлого года наблюдается значительный рост смертности от заболеваний из Группы {}'.format(
            RESULTS_blowout.loc[i, 'MKB'])

        MKB = MKB_GROUP_LIST_MAIN.index(RESULTS_blowout.loc[i, 'MKB'])
        task_type = 'Смертность_П2_new2_{}'.format(MKB)

        if RESULTS_blowout.loc[i, 'DATE'].month == 12:
            release = date(RESULTS_blowout.loc[i, 'DATE'].year + 1, 1, 1)
        else:
            release = date(RESULTS_blowout.loc[i, 'DATE'].year, RESULTS_blowout.loc[i, 'DATE'].month + 1, 1)

        title = 'Рост смертности от заболеваний из группы {} по сравнению с АППГ'.format(RESULTS_blowout.loc[i, 'MKB'])
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

