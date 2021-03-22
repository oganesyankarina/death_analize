""" АНАЛИЗ СМЕРТНОСТИ ОТ ВСЕХ ПРИЧИН и ЗАВИСИМОСТЬ ОТ ДОЛИ НАСЕЛЕНИЯ В ВОЗРАСТЕ 55 ЛЕТ И СТАРШЕ """
import pandas as pd
import uuid
import logging
import statsmodels.api as sm
from datetime import date, datetime

from connect_PostGres import cnx
from ISU_death_lists_dict import df_population, REGION, MONTHS_dict, task_type_dict
from ISU_death_lists_dict import FIO_dict, MKB_GROUP_LIST_MAIN, escalation_recipient_list
from ISU_death_functions import time_factor_calculation, get_df_death_finished, get_db_last_index
from ISU_death_functions import make_recipient, make_corr_for_recipient, make_release_date, make_recipient_uuid, make_recipient_fio
from ISU_death_lists_dict import results_files_path, results_files_suff, attached_file_names_dict

import plotly.graph_objects as go


def death_rule_first_55(save_to_sql=True, save_to_excel=True):
    # задаем возрастное ограничение
    age = 55

    start_time = datetime.now()
    program = f'death_elderly_{age}+'
    logging.info(f'{program} started')
    print(f'{program} started')

    df_death, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS = get_df_death_finished()

    # Расчет показателей
    # Расчет среднего возраста умерших
    # по Липецкой области
    print(f'Средний возраст умерших в 2017-{YEARS[-1]}гг. {df_death.age_death.mean(): .2f}')
    for last_year in YEARS:
        print(f'Средний возраст умерших в {last_year} {df_death[df_death.year_death.isin([last_year])].age_death.mean(): .2f}')
    # в разрезе МО
    df_avg_age_death = pd.DataFrame(columns=['Region', 'Year', 'AvgAgeDeath'])
    k = 0
    for region in REGION[:]:
        for last_year in YEARS[1:]:
            df_avg_age_death.loc[k] = {'Region': region, 'Year': last_year,
                                       'AvgAgeDeath': round(df_death[df_death.district_location.isin([region]) &
                                                                     df_death.year_death.isin([last_year])].age_death.mean(), 2)}
            k += 1
    # в разрезе МО
    df_proportion_death_in_old_age = pd.DataFrame(columns=['Region', 'Year', 'AmountDeathInOldAge', 'AllDeath',
                                                           'ProportionDeathInOldAge'])
    k = 0
    for region in REGION[:]:
        for last_year in YEARS[1:]:
            amount_death_in_old_age = len(df_death[df_death.district_location.isin([region]) &
                                                   df_death.year_death.isin([last_year]) & (df_death.age_death >= age)])
            all_death = len(df_death[df_death.district_location.isin([region]) & df_death.year_death.isin([last_year])])
            df_proportion_death_in_old_age.loc[k] = {'Region': region, 'Year': last_year,
                                                     'AmountDeathInOldAge': amount_death_in_old_age,
                                                     'AllDeath': all_death,
                                                     'ProportionDeathInOldAge': round(amount_death_in_old_age / all_death, 2)}
            k += 1
    print(f'Доля смертей в возрасте {age} лет и старше составляет {df_proportion_death_in_old_age.ProportionDeathInOldAge.mean() * 100: .2f}%')

    # Расчет доли населения старше 55 лет
    print(f'Расчет доли населения в возрасте старше {age} лет в разрезе муниципальных образований...')
    df_proportion_elderly = pd.DataFrame(columns=['Region', 'Year', 'Elderly', 'Population', 'ProportionElderly'])
    k = 0
    for region in REGION[:]:
        for last_year in YEARS[1:]:
            age_groups_elderly = ['55-59', '60-64', '65-69', '70-74', '75-79', '80-84', '85-89', '90-94', '95-99',
                                  '70 лет и старше', '85 и старше', '100 и более', 'Всего']
            elderly = df_population[df_population.region.isin([region]) &
                                    df_population.year.isin([last_year]) &
                                    df_population.age_group.isin(age_groups_elderly[:-1])].population.sum()
            population = df_population[df_population.region.isin([region]) &
                                       df_population.year.isin([last_year]) &
                                       df_population.age_group.isin(age_groups_elderly[-1:])].population.sum()
            proportion = round(elderly / population * 100, 2)

            df_proportion_elderly.loc[k] = {'Region': region, 'Year': last_year, 'Elderly': elderly,
                                            'Population': population,  'ProportionElderly': proportion}
            k += 1

    # Количество смертей за месяц + временной коэффициент
    print(f'Расчет количества смертей за месяц и временного коэффициента в разрезе муниципальных образований...')
    df_amount_death = pd.DataFrame(columns=['Region', 'date_period', 'AmountDeath'])
    k = 0
    for region in REGION[:]:
        for last_date in DATES:
            df_amount_death.loc[k] = {'Region': region, 'date_period': last_date,
                                      'AmountDeath': len(df_death[(df_death['district_location'].isin([region])) &
                                                                  (df_death['date_period'].isin([last_date]))])}
            k += 1
    for i in df_amount_death.index:
        df_amount_death.loc[i, 'Year'] = df_amount_death.loc[i, 'date_period'].year
        df_amount_death.loc[i, 'Month'] = df_amount_death.loc[i, 'date_period'].month
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
        if df_operating.loc[i, 'Population'] == 0:
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
                                              df_operating.Year.isin([year_not_null])]['ProportionElderly'].values[0]
            df_operating.loc[i, 'ProportionElderly'] = proportion_elderly

    for i in df_operating.index:
        df_operating.loc[i, 'AmountDeath/Population*time_factor_month'] = round(
            df_operating.loc[i, 'AmountDeath'] / df_operating.loc[i, 'Population'] * 100000 * df_operating.loc[
                i, 'time_factor_month'], 2)

    # if save_to_excel:
    #     with pd.ExcelWriter(f'{results_files_path}amount_death_{results_files_suff}.xlsx', engine='openpyxl') as writer:
    #         df_operating.to_excel(writer, sheet_name=f'amount_death', header=True,
    #                               index=False, encoding='1251')
########################################################################################################################
    # Поиск аномалий. ТРЕНД ЗА ПЕРИОД 2018-2020
    print('Поиск аномальных отклонений от тренда - уровень смертности не соответствует возрастной \
структуре муниципального образования')
    RESULTS = pd.DataFrame(columns=['Region', 'date_period', 'AmountDeath', 'Year', 'Month',
                                    'time_factor_month', 'time_factor_period',
                                    'AvgAgeDeath', 'Elderly', 'Population', 'ProportionElderly',
                                    'AmountDeath/Population*time_factor_month', 'bestfit', 'Deviation from trend'])
    blowout = pd.DataFrame(columns=['Region', 'date_period', 'AmountDeath', 'Year', 'Month',
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
########################################################################################################################
    RESULTS = pd.concat([RESULTS, df_Results])
    blowout_ = df_Results[(df_Results['AmountDeath/Population*time_factor_month'] > df_Results['bestfit']) &
                          (df_Results['Deviation from trend'] > 1.5 * df_Results[
                              'Deviation from trend'].std())].sort_values(['Month', 'Deviation from trend'],
                                                                          ascending=False)
    blowout = pd.concat([blowout, blowout_])
    # аномалии за последний месяц
    last_year = sorted(df_Results.Year.unique())[-1]
    last_date = sorted(df_Results.date_period.unique())[-1]
    results_blowout = blowout[blowout.Year.isin([last_year]) & blowout.date_period.isin([last_date])]
########################################################################################################################
    # Рисуем и сохраняем график регрессии
    print('Подготавливаем визуализацию...')
    fig = go.Figure()

    fig.add_trace(go.Scatter(name='Линия тренда', mode='lines', marker_color='#6395A9',
                             x=df_Results['ProportionElderly'],
                             y=df_Results['bestfit']))

    text = []
    for i in df_Results.index:
        text.append(
            f'{df_Results.loc[i, "Region"]} Месяц: {int(df_Results.loc[i, "Month"])} Год: {int(df_Results.loc[i, "Year"])}')
    fig.add_trace(go.Scatter(name='Все значения с 2018 года', mode='markers', marker_color='#69B987',
                             text=text, hovertemplate='%{text}<br>Доля: %{x}<br>Коэф. смертности: %{y}',
                             x=df_Results['ProportionElderly'],
                             y=df_Results['AmountDeath/Population*time_factor_month'].values))

    text = []
    for i in results_blowout.index:
        text.append(
            f'{results_blowout.loc[i, "Region"]} Месяц: {int(results_blowout.loc[i, "Month"])} Год: {int(results_blowout.loc[i, "Year"])}')
    fig.add_trace(go.Scatter(name='Критические значения<br>за последний месяц',
                             mode='markers', marker_color='#CC5A76', marker_size=10,
                             text=text, hovertemplate='%{text}<br>Доля: %{x}%<br>Коэф. смертности: %{y}',
                             x=results_blowout['ProportionElderly'],
                             y=results_blowout['AmountDeath/Population*time_factor_month'].values))

    fig.update_layout(xaxis_title=f'Доля населения в возрасте {age} лет и старше',
                      yaxis_title='Количество умерших за месяц/Численность населения<br>*100 тыс. чел.*Временной коэффициент',
                      title=f'Соотношение Доля населения в возрасте {age} лет и старше и <br>Количество умерших за месяц/Численность населения*100 тыс. чел.*Временной коэффициент')
    fig.write_html(f'{results_files_path}{attached_file_names_dict[1][1]}{results_files_suff}.html')
########################################################################################################################
    # Формируем результат работы и записываем в БД
    print('Формируем перечень задач, назначаем ответственных и сроки...')
    output = pd.DataFrame(columns=['recipient_uuid', 'message', 'deadline', 'release',
                                   'task_type_uuid', 'uuid', 'title'])
    k = get_db_last_index('death_output')
    for i in results_blowout.index:
        recipient = make_recipient(results_blowout.loc[i, 'Region'])
        fio = make_recipient_fio(recipient)
        recipient_uuid = make_recipient_uuid(recipient)

        release = make_release_date(results_blowout.loc[i, 'date_period'])

        task_type_uuid = task_type_dict['Смертность_П1_55+'][0]
        title = task_type_dict['Смертность_П1_55+'][1]
        month = MONTHS_dict[results_blowout.loc[i, 'Month']]
        last_year = int(results_blowout.loc[i, 'Year'])
        message = f'Проанализировать причины высокого уровня смертности в районе в период {month} {last_year} года'

        output.loc[k] = {'recipient_uuid': recipient_uuid, 'message': f'ИСУ обычная {message}',
                         'deadline': str(date.today() + pd.Timedelta(days=14)), 'release': release,
                         'task_type_uuid': task_type_uuid,
                         'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, f'{fio}{release}ИСУ обычная {message}'),
                         'title': title
                         }
        k += 1
########################################################################################################################
    # attached_file = pd.DataFrame(columns=['task_uuid', 'file'])
    # k = get_db_last_index('attached_file_death')
    # for i in output.index:
    #     uuid_ = output.loc[i, 'uuid']
    #
    #     attached_file.loc[k] = {'task_uuid': uuid_, 'file': f'{attached_file_names_dict[1][0]}{results_files_suff}.xlsx'}
    #     attached_file.loc[k+1] = {'task_uuid': uuid_, 'file': f'{attached_file_names_dict[1][1]}{results_files_suff}.html'}
    #
    #     k += 2
########################################################################################################################
    print('Сохраняем результаты...')
    if save_to_sql:
        output.to_sql('death_output', cnx, if_exists='append', index_label='id')
        # attached_file.to_sql('attached_file_death', cnx, if_exists='append', index_label='id')

    if save_to_excel:
        # with pd.ExcelWriter(f'{results_files_path}death_elderly_{results_files_suff}.xlsx', engine='openpyxl') as writer:
        #     RESULTS.to_excel(writer, sheet_name=f'elderly', header=True, index=False, encoding='1251')

        with pd.ExcelWriter(f'{results_files_path}{attached_file_names_dict[1][0]}{results_files_suff}.xlsx', engine='openpyxl') as writer:
            results_blowout.to_excel(writer, sheet_name=f'elderly_выбросы', header=True, index=False, encoding='1251')

        with pd.ExcelWriter(f'{results_files_path}{attached_file_names_dict[1][2]}{results_files_suff}.xlsx', engine='openpyxl') as writer:
            output.to_excel(writer, sheet_name=f'elderly', header=True, index=False, encoding='1251')

        # with pd.ExcelWriter(f'{results_files_path}{attached_file_names_dict[1][3]}{results_files_suff}.xlsx', engine='openpyxl') as writer:
        #     attached_file.to_excel(writer, sheet_name=f'attached_file_elderly', header=True, index=False, encoding='1251')
########################################################################################################################
    print(f'{program} done. elapsed time {datetime.now() - start_time}')
    print(f'Number of generated tasks {len(output)}')
    logging.info(f'{program} done. elapsed time {datetime.now() - start_time}')
    logging.info(f'Number of generated tasks {len(output)}')
########################################################################################################################


if __name__ == '__main__':
    logging.basicConfig(filename='logfile.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    death_rule_first_55(save_to_sql=True, save_to_excel=False)
