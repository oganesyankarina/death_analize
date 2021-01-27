# АНАЛИЗ СМЕРТНОСТИ ОТ ВСЕХ ПРИЧИН и ЗАВИСИМОСТЬ ОТ ДОЛИ НАСЕЛЕНИЯ В ВОЗРАСТЕ 55 ЛЕТ И СТАРШЕ
from datetime import date, datetime
import statsmodels.api as sm
import pandas as pd
import logging
import openpyxl
from ISU_death_lists_dict import df_Population, REGION, MONTHS_dict, FIO_dict
from get_from_death_finished import get_df_death_finished
from ISU_death_functions import time_factor_calculation
from connect_PostGres import cnx


def death_rule_first_55(save_to_sql=True, save_to_excel=False):
    # задаем возрастное ограничение
    age = 55

    start_time = datetime.now()
    program = 'death_rule_first_55+'
    logging.info('{} started'.format(program))
    print('{} started'.format(program))

    df_death_finished, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS = get_df_death_finished()

    # Расчет показателей
    # Расчет среднего возраста умерших
    # по Липецкой области
    print('Средний возраст умерших в 2017-2020гг. {}'.format(round(df_death_finished.age_death.mean(), 2)))
    for last_year in YEARS:
        print('Средний возраст умерших в {} {}'.format(last_year, round(
            df_death_finished[df_death_finished.year_death.isin([last_year])].age_death.mean(), 2)))
    # в разрезе МО
    df_avg_age_death = pd.DataFrame(columns=['Region', 'Year', 'AvgAgeDeath'])
    k = 0
    for region in REGION[:]:
        for last_year in YEARS[1:]:
            df_avg_age_death.loc[k] = {'Region': region, 'Year': last_year,
                                       'AvgAgeDeath': round(df_death_finished[df_death_finished.district_location.isin([region]) &
                                                                              df_death_finished.year_death.isin([last_year])].age_death.mean(), 2)}
            k += 1

    # в разрезе МО
    df_proportion_death_in_old_age = pd.DataFrame(columns=['Region', 'Year', 'AmountDeathInOldAge', 'AllDeath',
                                                           'ProportionDeathInOldAge'])
    k = 0
    for region in REGION[:]:
        for last_year in YEARS[1:]:
            amount_death_in_old_age = len(df_death_finished[df_death_finished.district_location.isin([region]) &
                                                            df_death_finished.year_death.isin([last_year]) &
                                                            (df_death_finished.age_death >= age)])
            all_death = len(df_death_finished[df_death_finished.district_location.isin([region]) & df_death_finished.year_death.isin([last_year])])
            df_proportion_death_in_old_age.loc[k] = {'Region': region, 'Year': last_year,
                                                     'AmountDeathInOldAge': amount_death_in_old_age,
                                                     'AllDeath': all_death,
                                                     'ProportionDeathInOldAge': round(amount_death_in_old_age / all_death, 2)}
            k += 1
    print('Доля смертей в возрасте {} лет и старше составляет {}%'.format(age, round(df_proportion_death_in_old_age.ProportionDeathInOldAge.mean(), 2) * 100))

    # Расчет доли населения старше 55 лет
    print(f'Расчет доли населения в возрасте старше {age} лет в разрезе муниципальных образований...')
    df_proportion_elderly = pd.DataFrame(columns=['Region', 'Year', 'Elderly', 'Population', 'ProportionElderly'])
    k = 0
    for region in REGION[:]:
        for last_year in YEARS[1:]:
            age_groups_elderly = ['55-59', '60-64', '65-69', '70-74', '75-79', '80-84', '85-89', '90-94', '95-99',
                                  '70 лет и старше', '85 и старше', '100 и более', 'Всего']
            elderly = df_Population[df_Population.Region.isin([region]) &
                                    df_Population.Year.isin([last_year]) &
                                    df_Population.AGE_GROUP.isin(age_groups_elderly[:-1])].Population.sum()
            population = df_Population[df_Population.Region.isin([region]) &
                                       df_Population.Year.isin([last_year]) &
                                       df_Population.AGE_GROUP.isin(age_groups_elderly[-1:])].Population.sum()
            proportion = round(elderly / population * 100, 2)

            df_proportion_elderly.loc[k] = {'Region': region, 'Year': last_year, 'Elderly': elderly,
                                            'Population': population,  'ProportionElderly': proportion}
            k += 1

    # Количество смертей за месяц + временной коэффициент
    print(f'Расчет количества смертей за месяц и временного коэффициента в разрезе муниципальных образований...')
    df_amount_death = pd.DataFrame(columns=['Region', 'DATE', 'AmountDeath'])
    k = 0
    for region in REGION[:]:
        for last_date in DATES:
            df_amount_death.loc[k] = {'Region': region, 'DATE': last_date,
                                      'AmountDeath': len(df_death_finished[(df_death_finished['district_location'].isin([region])) &
                                                                           (df_death_finished['DATE'].isin([last_date]))])}
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
    df_operating = df_amount_death.merge(df_avg_age_death, how='left', on=['Region', 'Year'])
    df_operating = df_operating.merge(df_proportion_elderly, how='left', on=['Region', 'Year'])
    # Если данные о численности еще отсутствуют, то берем данные за предыдущий год
    for i in df_operating.index:
        if pd.isnull(df_operating.loc[i, 'Population']):
            region = df_operating.loc[i, 'Region']
            year_null = df_operating.loc[i, 'Year']
            year_not_null = year_null - 1
            population = df_operating[df_operating.Region.isin([region]) &
                                      df_operating.Year.isin([year_not_null])].Population.values[0]
            df_operating.loc[i, 'Population'] = population
        if pd.isnull(df_operating.loc[i, 'ProportionElderly']):
            region = df_operating.loc[i, 'Region']
            year_null = df_operating.loc[i, 'Year']
            year_not_null = year_null - 1
            proportion_elderly = df_operating[df_operating.Region.isin([region]) &
                                              df_operating.Year.isin([year_not_null])]['ProportionElderly70+'].values[0]
            df_operating.loc[i, 'ProportionElderly'] = proportion_elderly

    for i in df_operating.index:
        df_operating.loc[i, 'AmountDeath/Population*time_factor_month'] = round(
            df_operating.loc[i, 'AmountDeath'] / df_operating.loc[i, 'Population'] * 100000 * df_operating.loc[
                i, 'time_factor_month'], 2)

    # year = sorted(df_operating.Year.unique())[-1]
    # last_date = sorted(df_operating.DATE.unique())[-1]
    # за последний месяц
    # df_operating[df_operating.Year.isin([year]) &
    #             df_operating.DATE.isin([last_date])].sort_values('AmountDeath/Population*time_factor_month',
    #                                                              ascending=False)

    # Поиск аномалий. ТРЕНД ЗА ПЕРИОД 2018-2020
    print('Поиск аномальных отклонений от тренда - уровень смертности не соответствует возрастной \
структуре муниципального образования')
    RESULTS = pd.DataFrame(columns=['Region', 'DATE', 'AmountDeath', 'Year', 'Month',
                                    'time_factor_month', 'time_factor_period',
                                    'AvgAgeDeath', 'Elderly', 'Population', 'ProportionElderly',
                                    'AmountDeath/Population*time_factor_month', 'bestfit', 'Deviation from trend'])
    blowout = pd.DataFrame(columns=['Region', 'DATE', 'AmountDeath', 'Year', 'Month',
                                    'time_factor_month', 'time_factor_period',
                                    'AvgAgeDeath', 'Population', 'ProportionElderly',
                                    'AmountDeath/Population*time_factor_month', 'bestfit', 'Deviation from trend'])
    df_Results = df_operating.copy()

    # regression
    df_Results['bestfit'] = sm.OLS(df_Results['AmountDeath/Population*time_factor_month'],
                                   sm.add_constant(df_Results['ProportionElderly'])).fit().fittedvalues
    for i in df_Results.index:
        df_Results.loc[i, 'Deviation from trend'] = df_Results.loc[i, 'AmountDeath/Population*time_factor_month'] - \
                                                    df_Results.loc[i, 'bestfit']

    RESULTS = pd.concat([RESULTS, df_Results])
    blowout_ = df_Results[(df_Results['AmountDeath/Population*time_factor_month'] > df_Results['bestfit']) &
                          (df_Results['Deviation from trend'] > 1.5 * df_Results[
                              'Deviation from trend'].std())].sort_values(['Month', 'Deviation from trend'],
                                                                          ascending=False)
    blowout = pd.concat([blowout, blowout_])

    last_year = sorted(df_Results.Year.unique())[-1]
    last_date = sorted(df_Results.DATE.unique())[-1]
    # аномалии за последний месяц
    results_blowout = blowout[blowout.Year.isin([last_year]) & blowout.DATE.isin([last_date])]

    # Формируем результат работы и записываем в БД
    print('Формируем перечень задач, назначаем ответственных и сроки...')
    output = pd.DataFrame(columns=['recipient', 'message', 'deadline', 'release',
                                   'task_type', 'title', 'fio_recipient'])

    if len(pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)) == 0:
        k = 0
    else:
        k = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx).id.max() + 1

    for i in results_blowout.index:

        if (results_blowout.loc[i, 'Region'] == 'Липецк') | (results_blowout.loc[i, 'Region'] == 'Елец'):
            corr = ''
        else:
            corr = ' район'

        recipient = 'Главный врач ЦРБ {}{}'.format(results_blowout.loc[i, 'Region'], corr)
        month = MONTHS_dict[results_blowout.loc[i, 'Month']]
        last_year = int(results_blowout.loc[i, 'Year'])
        message = 'Проанализировать причины высокого уровня смертности в районе в период {} {} года'.format(month, last_year)
        task_type = 'Смертность_П1_55+'

        if results_blowout.loc[i, 'DATE'].month == 12:
            release = date(results_blowout.loc[i, 'DATE'].year + 1, 1, 1)
        else:
            release = date(results_blowout.loc[i, 'DATE'].year, results_blowout.loc[i, 'DATE'].month + 1, 1)

        if recipient in FIO_dict.keys():
            fio = FIO_dict[recipient]
        else:
            fio = ''

        output.loc[k] = {'task_type': task_type,
                         'recipient': recipient,
                         'message': 'ИСУ обычная {}'.format(message),
                         'release': release,
                         'deadline': '{}'.format(date.today() + pd.Timedelta(days=14)),
                         'title': 'Уровень смертности не соответствует возрастной структуре населения района',
                         'fio_recipient': fio}

        k += 1

    print('Сохраняем результаты...')
    if save_to_sql:
        output.to_sql('test_output', cnx, if_exists='append', index_label='id')
    if save_to_excel:
        path = r'C:\Users\oganesyanKZ\PycharmProjects\ISU_death\Рассчеты/'
        result_file_name = f'{path}death_rule_first_55_{str(date.today())}.xlsx'
        wb = openpyxl.Workbook()
        # Sheet_name = wb.sheetnames
        wb.save(filename=result_file_name)
        with pd.ExcelWriter(result_file_name, engine='openpyxl', mode='a') as writer:
            RESULTS.to_excel(writer, sheet_name=f'П55+_{str(date.today())}', header=True, index=False, encoding='1251')
        with pd.ExcelWriter(result_file_name, engine='openpyxl', mode='a') as writer:
            results_blowout.to_excel(writer, sheet_name=f'П55+_выбросы_{str(date.today())}', header=True, index=False,
                                     encoding='1251')

    print('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    print('Number of generated tasks {}'.format(len(output)))
    logging.info('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    logging.info('Number of generated tasks {}'.format(len(output)))


if __name__ == '__main__':
    death_rule_first_55(save_to_sql=False, save_to_excel=True)
