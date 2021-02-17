# Анализ динамики смертности по группам МКБ. Рост три месяца подряд и по сравнению с АППГ
from datetime import date, datetime
import pandas as pd
import numpy as np
import logging

from connect_PostGres import cnx
from ISU_death_lists_dict import df_Population, REGION, FIO_dict, MKB_GROUP_LIST_MAIN
# from get_from_death_finished import get_df_death_finished
from ISU_death_functions import time_factor_calculation, get_df_death_finished, get_db_last_index


def death_rule_second_new(save_to_sql=True, save_to_excel=False):
    start_time = datetime.now()
    program = 'death_rule2_3monthgrow'
    logging.info(f'{program} started')
    print(f'{program} started')

    df_death, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS = get_df_death_finished()
    # df_death_finished = df_death_finished

    # Расчет показателей
    # Таблица с численностью населения
    df_population_mo = pd.DataFrame(columns=['Region', 'Year', 'Population'])
    k = 0
    for region in REGION[:]:
        for last_year in YEARS[1:]:
            population = df_Population[df_Population.Region.isin([region]) &
                                       df_Population.Year.isin([last_year]) &
                                       df_Population.AGE_GROUP.isin(['Всего'])].Population.sum()
            df_population_mo.loc[k] = {'Region': region, 'Year': last_year, 'Population': population}
            k += 1

    # Количество смертей за месяц + временной коэффициент
    print('Рассчитываем количество смертей за месяц в разрезе муниципальных образований и групп заболеваний...')
    df_amount_death = pd.DataFrame(columns=['Region', 'MKB', 'DATE', 'AmountDeathAll', 'AmountDeathMKB'])
    k = 0
    for region in REGION[:]:
        for MKB_id in MKB_GROUP_LIST_MAIN[:]:
            for last_date in DATES:
                df_amount_death.loc[k] = {'Region': region, 'MKB': MKB_id, 'DATE': last_date,
                                          'AmountDeathAll': len(
                                              df_death[(df_death['district_location'].isin([region]))
                                                       & (df_death['DATE'].isin([last_date]))]),
                                          'AmountDeathMKB': len(df_death[
                                                                    (df_death['district_location'].isin([region]))
                                                                    & (df_death['DATE'].isin([last_date]))
                                                                    & (df_death['MKB_GROUP_NAME_original_reason'].isin([MKB_id]))])}
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

    print('Добавляем информацию о численности населения...')
    # Если данные о численности еще отсутствуют, то берем данные за предыдущий год
    for i in df_operating.index:
        if df_operating.loc[i, 'Population'] == 0:
            region = df_operating.loc[i, 'Region']
            YearNULL = df_operating.loc[i, 'Year']
            YearNotNULL = YearNULL - 1
            population = df_operating[df_operating.Region.isin([region]) &
                                      df_operating.Year.isin([YearNotNULL])].Population.values[0]
            df_operating.loc[i, 'Population'] = population

    print('Рассчитываем коэффициенты смертности...')
    for i in df_operating.index:
        df_operating.loc[i, 'AmountDeath/Population*time_factor_month'] = round(df_operating.loc[i, 'AmountDeathMKB'] /
                                                                                df_operating.loc[i, 'Population'] *
                                                                                100000 *
                                                                                df_operating.loc[
                                                                                    i, 'time_factor_month'], 2)

    if save_to_excel:
        path = r'C:\Users\oganesyanKZ\PycharmProjects\ISU_death\Рассчеты/'
        with pd.ExcelWriter(f'{path}amountdeathMKB_{str(date.today())}.xlsx', engine='openpyxl') as writer:
            df_operating.to_excel(writer, sheet_name=f'amountdeathMKB_{str(date.today())}', header=True,
                                  index=False, encoding='1251')

    # Поиск аномалий. Рост смертности три периода подряд.
    print('Ищем ситуации роста на протяжении трех месяцев...')
    df_Results = pd.DataFrame(columns=['Region', 'MKB', 'DATE', 'Year', 'Month', 'meaning_last3'])
    main_column = 'AmountDeath/Population*time_factor_month'
    k = 0
    for MKB_id in MKB_GROUP_LIST_MAIN:
        for region in REGION:
            temp = df_operating[df_operating.MKB.isin([MKB_id]) & df_operating.Region.isin([region])]
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

    last_year = YEARS[-1]
    last_date = DATES[-1]
    # за последний месяц
    results_blowout = df_Results[df_Results.Year.isin([last_year]) & df_Results.DATE.isin([last_date])]

    # Формируем результат работы и записываем в БД
    print('Формируем перечень задач, назначаем ответственных и сроки...')
    output = pd.DataFrame(columns=['recipient', 'message', 'deadline', 'release',
                                   'task_type', 'title', 'fio_recipient'])

    k = get_db_last_index('test_output')
    # if len(pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)) == 0:
    #     k = 0
    # else:
    #     k = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx).id.max() + 1

    for i in results_blowout.index:
        mo = results_blowout.loc[i, 'Region']
        MKB = results_blowout.loc[i, 'MKB']
        if (mo == 'Липецк') | (mo == 'Елец'):
            corr = ''
        else:
            corr = 'район'
        recipient = f'Главный врач ЦРБ {mo} {corr}'
        message = f'Проанализировать и принять меры по снижению смертности. На протяжении последних трех месяцев в районе наблюдается рост смертности от заболеваний из Группы {MKB}'

        MKB_id = MKB_GROUP_LIST_MAIN.index(results_blowout.loc[i, 'MKB'])
        task_type = f'Смертность_П2_new1_{MKB_id}'

        if results_blowout.loc[i, 'DATE'].month == 12:
            release = date(results_blowout.loc[i, 'DATE'].year + 1, 1, 1)
        else:
            release = date(results_blowout.loc[i, 'DATE'].year, results_blowout.loc[i, 'DATE'].month + 1, 1)

        title = f'Рост смертности от заболеваний из группы {MKB}'
        if recipient in FIO_dict.keys():
            FIO = FIO_dict[recipient]
        else:
            FIO = ''
        output.loc[k] = {'task_type': task_type,
                         'recipient': recipient,
                         'message': f'ИСУ обычная {message}',
                         'release': release,
                         'deadline': str(date.today() + pd.Timedelta(days=14)),
                         'title': title, 'fio_recipient': FIO}
        k += 1

    print('Сохраняем результаты...')
    if save_to_sql:
        output.to_sql('test_output', cnx, if_exists='append', index_label='id')
    if save_to_excel:
        path = r'C:\Users\oganesyanKZ\PycharmProjects\ISU_death\Рассчеты/'
        with pd.ExcelWriter(f'{path}death_rule2_3monthgrow_{str(date.today())}.xlsx', engine='openpyxl') as writer:
            df_Results.to_excel(writer, sheet_name=f'rule2_3monthgrow_{str(date.today())}', header=True,
                                index=False, encoding='1251')
        with pd.ExcelWriter(f'{path}death_rule2_3monthgrow_выбросы{str(date.today())}.xlsx', engine='openpyxl') as writer:
            results_blowout.to_excel(writer, sheet_name=f'3monthgrow_выбросы_{str(date.today())}', header=True,
                                     index=False, encoding='1251')

    print(f'{program} done. elapsed time {datetime.now() - start_time}')
    print(f'Number of generated tasks {len(output)}')
    logging.info(f'{program} done. elapsed time {datetime.now() - start_time}')
    logging.info(f'Number of generated tasks {len(output)}')

    start_time = datetime.now()
    program = 'death_rule_second_new2'
    logging.info(f'{program} started')
    print(f'{program} started')
########################################################################################################################
########################################################################################################################
    # Поиск аномалий.
    # Сравнение с аналогичным периодом прошлого года.
    df_Results = pd.DataFrame(columns=['Region', 'MKB', 'DATE', 'Year', 'Month', 'AmountDeathMKB_T',
                                       'AmountDeathMKB_T-1', 'AmountDeath/Population*time_factor_month_T',
                                       'AmountDeath/Population*time_factor_month_T-1', 'increase_deaths'])
    k = 0
    for MKB_id in MKB_GROUP_LIST_MAIN:
        for region in REGION:
            temp = df_operating[df_operating.MKB.isin([MKB_id]) & df_operating.Region.isin([region])]
            for i in temp.index[:]:
                if temp.loc[i, 'Year'] > 2018:
                    TheSamePeriodLastYear = date(int(temp.loc[i, 'Year'] - 1), int(temp.loc[i, 'Month']), 1)
                    AmDeathMKB1 = temp.loc[i, 'AmountDeathMKB']
                    AmDeathMKB0 = temp[temp.DATE.isin([TheSamePeriodLastYear])]['AmountDeathMKB'].values[0]
                    AmountDeathMKB1 = temp.loc[i, 'AmountDeath/Population*time_factor_month']
                    AmountDeathMKB0 = temp[temp.DATE.isin([TheSamePeriodLastYear])]['AmountDeath/Population*time_factor_month'].values[0]
                    increase_deaths = round(AmountDeathMKB1 / AmountDeathMKB0, 2)

                    df_Results.loc[k] = {'Region': region, 'MKB': MKB_id,
                                         'DATE': temp.loc[i, 'DATE'],
                                         'Year': temp.loc[i, 'Year'],
                                         'Month': temp.loc[i, 'Month'],
                                         'AmountDeathMKB_T': AmDeathMKB1,
                                         'AmountDeathMKB_T-1': AmDeathMKB0,
                                         'AmountDeath/Population*time_factor_month_T': AmountDeathMKB1,
                                         'AmountDeath/Population*time_factor_month_T-1': AmountDeathMKB0,
                                         'increase_deaths': increase_deaths}
                    k += 1

    last_year = YEARS[-1]
    last_date = DATES[-1]
    # за последний месяц
    UpperBound = 1.5
    MinimumAmountDeathMKBTheSamePeriodLastYear = 5
    results_blowout = df_Results[df_Results.Year.isin([last_year]) & df_Results.DATE.isin([last_date]) &
                                 (df_Results.increase_deaths > UpperBound) &
                                 (df_Results['AmountDeathMKB_T-1'] > MinimumAmountDeathMKBTheSamePeriodLastYear) &
                                 (df_Results.increase_deaths != np.inf)].sort_values('increase_deaths', ascending=False)

    # Формируем результат работы и записываем в БД
    output = pd.DataFrame(columns=['recipient', 'message', 'deadline', 'release', 'task_type',
                                   'title', 'fio_recipient'])

    k = get_db_last_index('test_output')
    # if len(pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)) == 0:
    #     k = 0
    # else:
    #     k = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx).id.max() + 1

    for i in results_blowout.index:
        mo = results_blowout.loc[i, 'Region']
        MKB = results_blowout.loc[i, 'MKB']

        if (mo == 'Липецк') | (mo == 'Елец'):
            corr = ''
        else:
            corr = 'район'

        recipient = f'Главный врач ЦРБ {mo} {corr}'
        message = f'Проанализировать и принять меры по снижению смертности. В районе по сравнению с аналогичным периодом прошлого года наблюдается значительный рост смертности от заболеваний из Группы {MKB}'

        MKB_id = MKB_GROUP_LIST_MAIN.index(MKB)
        task_type = f'Смертность_П2_new2_{MKB_id}'

        if results_blowout.loc[i, 'DATE'].month == 12:
            release = date(results_blowout.loc[i, 'DATE'].year + 1, 1, 1)
        else:
            release = date(results_blowout.loc[i, 'DATE'].year, results_blowout.loc[i, 'DATE'].month + 1, 1)

        title = f'Рост смертности от заболеваний из группы {MKB} по сравнению с АППГ'
        if recipient in FIO_dict.keys():
            FIO = FIO_dict[recipient]
        else:
            FIO = ''
        output.loc[k] = {'task_type': task_type,
                         'recipient': recipient,
                         'message': f'ИСУ обычная {message}',
                         'release': release,
                         'deadline': str(date.today() + pd.Timedelta(days=14)),
                         'title': title, 'fio_recipient': FIO}
        k += 1

    print('Сохраняем результаты...')
    if save_to_sql:
        output.to_sql('test_output', cnx, if_exists='append', index_label='id')
    if save_to_excel:
        path = r'C:\Users\oganesyanKZ\PycharmProjects\ISU_death\Рассчеты/'
        with pd.ExcelWriter(f'{path}death_rule2_sameperiod_{str(date.today())}.xlsx', engine='openpyxl') as writer:
            df_Results.to_excel(writer, sheet_name=f'rule2_sameperiod_{str(date.today())}', header=True,
                                index=False, encoding='1251')
        with pd.ExcelWriter(f'{path}death_rule2_sameperiod_выбросы{str(date.today())}.xlsx',
                            engine='openpyxl') as writer:
            results_blowout.to_excel(writer, sheet_name=f'sameeperiod_выбросы_{str(date.today())}', header=True,
                                     index=False, encoding='1251')

    print(f'{program} done. elapsed time {datetime.now() - start_time}')
    print(f'Number of generated tasks {len(output)}')
    logging.info(f'{program} done. elapsed time {datetime.now() - start_time}')
    logging.info(f'Number of generated tasks {len(output)}')


if __name__ == '__main__':
    death_rule_second_new(save_to_sql=False, save_to_excel=True)
