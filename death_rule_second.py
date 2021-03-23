""" Анализ динамики смертности по группам МКБ. Рост три месяца подряд и по сравнению с АППГ """
import pandas as pd
import numpy as np
import uuid
import logging
from datetime import date, datetime
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from connect_PostGres import cnx
from ISU_death_lists_dict import df_population, REGION, MONTHS_dict, task_type_dict
from ISU_death_lists_dict import FIO_dict, MKB_GROUP_LIST_MAIN, escalation_recipient_list
from ISU_death_lists_dict import results_files_path, results_files_suff, attached_file_names_dict
from ISU_death_functions import time_factor_calculation, get_df_death_finished, get_db_last_index
from ISU_death_functions import make_recipient, make_corr_for_recipient, make_release_date, make_recipient_fio, make_recipient_uuid


def death_rule_second_new(save_to_sql=True, save_to_excel=True):
    start_time = datetime.now()
    program = 'death_3monthgrow'
    logging.info(f'{program} started')
    print(f'{program} started')

    df_death, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS = get_df_death_finished()

    # Расчет показателей
    # Таблица с численностью населения
    df_population_mo = pd.DataFrame(columns=['Region', 'Year', 'Population'])
    k = 0
    for region in REGION[:]:
        for last_year in YEARS[1:]:
            population = df_population[df_population.region.isin([region]) &
                                       df_population.year.isin([last_year]) &
                                       df_population.age_group.isin(['Всего'])].population.sum()
            df_population_mo.loc[k] = {'Region': region, 'Year': last_year, 'Population': population}
            k += 1

    # Количество смертей за месяц + временной коэффициент
    print('Рассчитываем количество смертей за месяц в разрезе муниципальных образований и групп заболеваний...')
    df_amount_death = pd.DataFrame(columns=['Region', 'MKB', 'date_period', 'AmountDeathAll', 'AmountDeathMKB'])
    k = 0
    for region in REGION[:]:
        for MKB_id in MKB_GROUP_LIST_MAIN[:]:
            for last_date in DATES:
                temp = df_death[(df_death['district_location'].isin([region])) & (df_death['date_period'].isin([last_date]))]
                df_amount_death.loc[k] = {'Region': region, 'MKB': MKB_id, 'date_period': last_date,
                                          'AmountDeathAll': len(temp),
                                          'AmountDeathMKB': len(temp[temp['mkb_group_name_original_reason'].isin([MKB_id])])}
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

    # if save_to_excel:
    #     with pd.ExcelWriter(f'{results_files_path}amount_death_MKB_{results_files_suff}.xlsx', engine='openpyxl') as writer:
    #         df_operating.to_excel(writer, sheet_name=f'amount_death_MKB', header=True, index=False, encoding='1251')
########################################################################################################################
    print('Подготавливаем визуализацию по Липецкой области...')
    main_column = 'AmountDeath/Population*time_factor_month'
    fig = make_subplots(rows=len(MKB_GROUP_LIST_MAIN[:]), cols=1, specs=[[{}]] * len(MKB_GROUP_LIST_MAIN[:]),
                        subplot_titles=MKB_GROUP_LIST_MAIN, shared_yaxes=True,
                        vertical_spacing=0.02, horizontal_spacing=0.04)

    for MKB_index in range(len(MKB_GROUP_LIST_MAIN[:])):
        for Region in REGION:
            if MKB_index == 0:
                show_legend_flag = True
            else:
                show_legend_flag = False
            temp = df_operating[df_operating.MKB.isin([MKB_GROUP_LIST_MAIN[MKB_index]]) &
                                df_operating.Region.isin([Region])]
            fig.add_trace(go.Scatter(x=temp.date_period.values[:], y=temp[main_column].values[:],
                                     showlegend=show_legend_flag, legendgroup=Region,
                                     mode='lines+markers', name=Region, text=Region), row=MKB_index + 1, col=1)
            fig.update_layout(showlegend=True)

    fig.update_layout(height=2300,
                      legend=dict(yanchor='bottom', xanchor='left', y=0.73, x=1.0, font=dict(size=10)),
                      title=dict(text='Динамика смертности по группам МКБ в Липецкой области<br>(с учетом численности населения и временного коэффициента)',
                                 font=dict(size=16), x=0.01, y=0.98, xanchor='left', yanchor='top'))

    file_title2 = f'{attached_file_names_dict[2][1]}{results_files_suff}'
    file_path2 = str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{file_title2}').hex) + '.html'
    fig.write_html(f'{results_files_path}{file_path2}')
########################################################################################################################
    print('Подготавливаем визуализации для отдельных районов Липецкой области...')
    for Region in REGION:
        fig = make_subplots(rows=len(MKB_GROUP_LIST_MAIN[:]), cols=1, specs=[[{}]] * len(MKB_GROUP_LIST_MAIN[:]),
                            subplot_titles=(MKB_GROUP_LIST_MAIN),
                            shared_yaxes=True,
                            vertical_spacing=0.02, horizontal_spacing=0.04)

        corr = make_corr_for_recipient(Region)

        for MKB_index in range(len(MKB_GROUP_LIST_MAIN[:])):

            temp = df_operating[
                df_operating.MKB.isin([MKB_GROUP_LIST_MAIN[MKB_index]]) & df_operating.Region.isin([Region])]

            fig.add_trace(go.Scatter(x=temp.date_period.values[:-3], y=temp[main_column].values[:-3],
                                     legendgroup=Region, mode='lines+markers', marker_color='#69B987',
                                     name=Region, text=Region), row=MKB_index + 1, col=1)
            fig.add_trace(go.Scatter(x=temp.date_period.values[-4:], y=temp[main_column].values[-4:],
                                     legendgroup=Region, mode='lines+markers', marker_color='#CC5A76',
                                     name=Region, text=Region), row=MKB_index + 1, col=1)
            fig.add_trace(go.Scatter(x=[temp.date_period.values[-13]], y=[temp[main_column].values[-13]],
                                     legendgroup=Region, mode='markers', marker_color='#CC5A76',
                                     name=Region, text=Region), row=MKB_index + 1, col=1)
            fig.update_layout(showlegend=False)

        fig.update_layout(height=2300,
                          legend=dict(yanchor='bottom', xanchor='left', y=0.73, x=1.0, font=dict(size=10), ),
                          title=dict(text=f'<b>{Region}{corr}</b><br>Динамика смертности по группам МКБ<br>(с учетом численности населения и временного коэффициента)',
                                     font=dict(size=16), x=0.01, y=0.98, xanchor='left', yanchor='top'))

        file_title3 = f'{attached_file_names_dict[2][1]}{Region}_{results_files_suff}'
        file_path3 = str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{file_title3}').hex) + '.html'
        fig.write_html(f'{results_files_path}{file_path3}')
########################################################################################################################
    # Поиск аномалий. Рост смертности три периода подряд.
    print('Ищем ситуации роста на протяжении трех месяцев...')
    df_Results = pd.DataFrame(columns=['Region', 'MKB', 'date_period', 'Year', 'Month', 'meaning_last3'])
    k = 0
    for MKB_id in MKB_GROUP_LIST_MAIN:
        for region in REGION:
            temp = df_operating[df_operating.MKB.isin([MKB_id]) & df_operating.Region.isin([region])]
            for i in temp.index[3:]:
                if (temp.loc[i, main_column] > temp.loc[i - 1, main_column]) & (
                        temp.loc[i - 1, main_column] > temp.loc[i - 2, main_column]) & (
                        temp.loc[i - 2, main_column] > temp.loc[i - 3, main_column]):
                    df_Results.loc[k] = {'Region': temp.loc[i, 'Region'], 'MKB': temp.loc[i, 'MKB'],
                                         'date_period': temp.loc[i, 'date_period'], 'Year': temp.loc[i, 'Year'],
                                         'Month': temp.loc[i, 'Month'],
                                         'meaning_last3': '{}: {},{}: {},{}: {},{}: {}'.format(
                                             temp.loc[i - 3, 'date_period'], temp.loc[i - 3, main_column],
                                             temp.loc[i - 2, 'date_period'], temp.loc[i - 2, main_column],
                                             temp.loc[i - 1, 'date_period'], temp.loc[i - 1, main_column],
                                             temp.loc[i, 'date_period'], temp.loc[i, main_column])}
                    k += 1
    # за последний месяц
    last_year = YEARS[-1]
    last_date = DATES[-1]
    results_blowout = df_Results[df_Results.Year.isin([last_year]) & df_Results.date_period.isin([last_date])]
########################################################################################################################
    # Формируем результат работы и записываем в БД
    print('Формируем перечень задач, назначаем ответственных и сроки...')
    output = pd.DataFrame(columns=['recipient_uuid', 'message', 'deadline', 'release',
                                   'task_type_uuid', 'uuid', 'mo', 'title'])
    k = get_db_last_index('death_output')
    for i in results_blowout.index:
        mo = results_blowout.loc[i, 'Region']
        recipient = make_recipient(mo)
        fio = make_recipient_fio(recipient)
        recipient_uuid = make_recipient_uuid(recipient)

        release = make_release_date(results_blowout.loc[i, 'date_period'])

        MKB = results_blowout.loc[i, 'MKB']
        MKB_id = MKB_GROUP_LIST_MAIN.index(MKB)
        # task_type = f'Смертность_П2_3monthgrow_{MKB_id}'
        task_type_uuid = task_type_dict[f'Смертность_П2_3monthgrow_{MKB_id}'][0]
        # title = f'Рост смертности от заболеваний из группы {MKB}'
        title = task_type_dict[f'Смертность_П2_3monthgrow_{MKB_id}'][1]
        message = f'Проанализировать и принять меры по снижению смертности. На протяжении последних трех месяцев в районе наблюдается рост смертности от заболеваний из Группы {MKB}'

        output.loc[k] = {'recipient_uuid': recipient_uuid, 'message': f'ИСУ обычная {message}',
                         'deadline': str(date.today() + pd.Timedelta(days=14)), 'release': release,
                         'task_type_uuid': task_type_uuid,
                         'mo': mo,
                         'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, f'{recipient_uuid}{release}ИСУ обычная {message}'),
                         'title': title
                         }
        k += 1
########################################################################################################################
    attached_file = pd.DataFrame(columns=['uuid', 'task_uuid', 'file_title', 'file_path'])
    k = get_db_last_index('death_attached_files')
    for i in output.index:
        mo = output.loc[i, 'mo']
        task_uuid = output.loc[i, 'uuid']
        file_title1 = f'{attached_file_names_dict[2][0]}{results_files_suff}'
        file_title2 = f'{attached_file_names_dict[2][1]}{results_files_suff}'
        file_title3 = f'{attached_file_names_dict[2][1]}{mo}_{results_files_suff}'
        file_path1 = str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{file_title1}').hex)+'.xlsx'
        file_path2 = str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{file_title2}').hex)+'.html'
        file_path3 = str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{file_title3}').hex)+'.html'

        attached_file.loc[k] = {'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, str(task_uuid)+file_path1),
                                'task_uuid': task_uuid,
                                'file_title': file_title1,
                                'file_path': file_path1}
        attached_file.loc[k + 1] = {'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, str(task_uuid)+file_path2),
                                    'task_uuid': task_uuid,
                                    'file_title': file_title2,
                                    'file_path': file_path2}
        attached_file.loc[k + 2] = {'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, str(task_uuid)+file_path3),
                                    'task_uuid': task_uuid,
                                    'file_title': file_title3,
                                    'file_path': file_path3}
        k += 3
########################################################################################################################
    print('Сохраняем результаты...')
    if save_to_sql:
        output.drop('mo', axis=1).to_sql('death_output', cnx, if_exists='append', index_label='id')
        attached_file.to_sql('death_attached_files', cnx, if_exists='append', index_label='id')
    if save_to_excel:
        # with pd.ExcelWriter(f'{results_files_path}death_3monthgrow_{results_files_suff}.xlsx', engine='openpyxl') as writer:
        #     df_Results.to_excel(writer, sheet_name=f'3monthgrow', header=True, index=False, encoding='1251')
        with pd.ExcelWriter(f'{results_files_path}{file_path1}', engine='openpyxl') as writer:
            results_blowout.to_excel(writer, sheet_name=f'3monthgrow', header=True, index=False, encoding='1251')
        # with pd.ExcelWriter(f'{results_files_path}{attached_file_names_dict[2][2]}{results_files_suff}.xlsx', engine='openpyxl') as writer:
        #     output.to_excel(writer, sheet_name=f'3monthgrow_output', header=True, index=False, encoding='1251')
        # with pd.ExcelWriter(f'{results_files_path}{attached_file_names_dict[2][3]}{results_files_suff}.xlsx', engine='openpyxl') as writer:
        #     attached_file.to_excel(writer, sheet_name=f'attached_file_3monthgrow', header=True, index=False, encoding='1251')
########################################################################################################################
    print(f'{program} done. elapsed time {datetime.now() - start_time}')
    print(f'Number of generated tasks {len(output)}')
    logging.info(f'{program} done. elapsed time {datetime.now() - start_time}')
    logging.info(f'Number of generated tasks {len(output)}')

    start_time = datetime.now()
    program = 'death_sameperiod'
    logging.info(f'{program} started')
    print(f'{program} started')
########################################################################################################################
########################################################################################################################
    # Поиск аномалий. Сравнение с аналогичным периодом прошлого года.
    print('Ищем ситуации значительного роста по сравнению с аналогичным периодом прошлого года...')
    df_Results = pd.DataFrame(columns=['Region', 'MKB', 'date_period', 'Year', 'Month', 'AmountDeathMKB_T',
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
                    AmDeathMKB0 = temp[temp.date_period.isin([TheSamePeriodLastYear])]['AmountDeathMKB'].values[0]
                    AmountDeathMKB1 = temp.loc[i, 'AmountDeath/Population*time_factor_month']
                    AmountDeathMKB0 = temp[temp.date_period.isin([TheSamePeriodLastYear])]['AmountDeath/Population*time_factor_month'].values[0]
                    increase_deaths = round(AmountDeathMKB1 / AmountDeathMKB0, 2)

                    df_Results.loc[k] = {'Region': region, 'MKB': MKB_id, 'date_period': temp.loc[i, 'date_period'],
                                         'Year': temp.loc[i, 'Year'], 'Month': temp.loc[i, 'Month'],
                                         'AmountDeathMKB_T': AmDeathMKB1, 'AmountDeathMKB_T-1': AmDeathMKB0,
                                         'AmountDeath/Population*time_factor_month_T': AmountDeathMKB1,
                                         'AmountDeath/Population*time_factor_month_T-1': AmountDeathMKB0,
                                         'increase_deaths': increase_deaths}
                    k += 1
    # за последний месяц
    last_year = YEARS[-1]
    last_date = DATES[-1]
    UpperBound = 1.5
    MinimumAmountDeathMKBTheSamePeriodLastYear = 5
    results_blowout = df_Results[df_Results.Year.isin([last_year]) & df_Results.date_period.isin([last_date]) &
                                 (df_Results.increase_deaths > UpperBound) &
                                 (df_Results['AmountDeathMKB_T-1'] > MinimumAmountDeathMKBTheSamePeriodLastYear) &
                                 (df_Results.increase_deaths != np.inf)].sort_values('increase_deaths', ascending=False)
########################################################################################################################
    # Формируем результат работы и записываем в БД
    print('Формируем перечень задач, назначаем ответственных и сроки...')
    output = pd.DataFrame(columns=['recipient_uuid', 'message', 'deadline', 'release',
                                   'task_type_uuid', 'uuid', 'mo', 'title'])
    k = get_db_last_index('death_output')
    for i in results_blowout.index:
        mo = results_blowout.loc[i, 'Region']
        recipient = make_recipient(mo)
        fio = make_recipient_fio(recipient)
        recipient_uuid = make_recipient_uuid(recipient)

        release = make_release_date(results_blowout.loc[i, 'date_period'])

        MKB = results_blowout.loc[i, 'MKB']
        MKB_id = MKB_GROUP_LIST_MAIN.index(MKB)
        # task_type = f'Смертность_П2_sameperiod_{MKB_id}'
        task_type_uuid = task_type_dict[f'Смертность_П2_sameperiod_{MKB_id}'][0]
        # title = f'Рост смертности от заболеваний из группы {MKB} по сравнению с АППГ'
        title = task_type_dict[f'Смертность_П2_sameperiod_{MKB_id}'][1]
        message = f'Проанализировать и принять меры по снижению смертности. В районе по сравнению с аналогичным периодом прошлого года наблюдается значительный рост смертности от заболеваний из Группы {MKB}'

        output.loc[k] = {'recipient_uuid': recipient_uuid, 'message': f'ИСУ обычная {message}',
                         'deadline': str(date.today() + pd.Timedelta(days=14)), 'release': release,
                         'task_type_uuid': task_type_uuid,
                         'mo': mo,
                         'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, f'{recipient_uuid}{release}ИСУ обычная {message}'),
                         'title': title
                         }
        k += 1
########################################################################################################################
    attached_file = pd.DataFrame(columns=['uuid', 'task_uuid', 'file_title', 'file_path'])
    k = get_db_last_index('death_attached_files')
    for i in output.index:
        mo = output.loc[i, 'mo']
        task_uuid = output.loc[i, 'uuid']
        file_title1 = f'{attached_file_names_dict[3][0]}{results_files_suff}'
        file_title2 = f'{attached_file_names_dict[3][1]}{results_files_suff}'
        file_title3 = f'{attached_file_names_dict[3][1]}{mo}_{results_files_suff}'
        file_path1 = str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{file_title1}').hex)+'.xlsx'
        file_path2 = str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{file_title2}').hex)+'.html'
        file_path3 = str(uuid.uuid3(uuid.NAMESPACE_DNS, f'{file_title3}').hex)+'.html'

        attached_file.loc[k] = {'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, str(task_uuid)+file_path1),
                                'task_uuid': task_uuid,
                                'file_title': file_title1,
                                'file_path': file_path1}
        attached_file.loc[k + 1] = {'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, str(task_uuid)+file_path2),
                                    'task_uuid': task_uuid,
                                    'file_title': file_title2,
                                    'file_path': file_path2}
        attached_file.loc[k + 2] = {'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, str(task_uuid)+file_path3),
                                    'task_uuid': task_uuid,
                                    'file_title': file_title3,
                                    'file_path': file_path3}
        k += 3
########################################################################################################################
    print('Сохраняем результаты...')
    if save_to_sql:
        output.drop('mo', axis=1).to_sql('death_output', cnx, if_exists='append', index_label='id')
        attached_file.to_sql('death_attached_files', cnx, if_exists='append', index_label='id')
    if save_to_excel:
        # with pd.ExcelWriter(f'{results_files_path}death_sameperiod_{results_files_suff}.xlsx', engine='openpyxl') as writer:
        #     df_Results.to_excel(writer, sheet_name=f'sameperiod', header=True, index=False, encoding='1251')
        with pd.ExcelWriter(f'{results_files_path}{file_path1}', engine='openpyxl') as writer:
            results_blowout.to_excel(writer, sheet_name=f'sameperiod', header=True, index=False, encoding='1251')
        # with pd.ExcelWriter(f'{results_files_path}{attached_file_names_dict[3][2]}{results_files_suff}.xlsx', engine='openpyxl') as writer:
        #     output.to_excel(writer, sheet_name=f'sameperiod_output', header=True, index=False, encoding='1251')
        # with pd.ExcelWriter(f'{results_files_path}{attached_file_names_dict[3][3]}{results_files_suff}.xlsx', engine='openpyxl') as writer:
        #     attached_file.to_excel(writer, sheet_name=f'attached_file_sameperiod', header=True, index=False, encoding='1251')
########################################################################################################################
    print(f'{program} done. elapsed time {datetime.now() - start_time}')
    print(f'Number of generated tasks {len(output)}')
    logging.info(f'{program} done. elapsed time {datetime.now() - start_time}')
    logging.info(f'Number of generated tasks {len(output)}')
########################################################################################################################


if __name__ == '__main__':
    logging.basicConfig(filename='logfile.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    death_rule_second_new(save_to_sql=True, save_to_excel=True)
