# Эскалация задач
from datetime import date, datetime
import logging
import pandas as pd

from connect_PostGres import cnx
from ISU_death_lists_dict import FIO_dict, escalation_recipient_list, escalation_recipient_text
from ISU_death_functions import get_db_last_index


def death_escalation(save_to_sql=True, save_to_excel=False):
    start_time = datetime.now()
    program = 'death_escalation'
    logging.info(f'{program} started')
    print(f'{program} started')

    # Считаем задачи
    df_task_list = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)
    dates = sorted(df_task_list.release.unique())
    dates = dates[-4:]

    df_task_list = pd.read_sql_query('''SELECT * FROM public."test_output"''', cnx)
    df_task_list = df_task_list[df_task_list.release.isin(dates)]

    df_task_count_list = pd.DataFrame(columns=['task_type', 'recipient', 'task_count'])
    k = 0
    for i in df_task_list.task_type.unique():
        for j in df_task_list.recipient.unique():
            df_task_count_list.loc[k] = {'task_type': i, 'recipient': j,
                                         'task_count': len(df_task_list[df_task_list.task_type.isin([i]) &
                                                                        df_task_list.recipient.isin([j])])}
            k += 1
    # оставляем только задачи, которые выставлялись на исполнителя более одного раза
    df_task_count_list = df_task_count_list[df_task_count_list.task_count > 1]
    df_task_count_list = df_task_count_list.sort_values(['task_count'], ascending=False)
    df_task_count_list.index = range(df_task_count_list.shape[0])

    # Итоговая реализация алгоритма
    RESULTS = pd.DataFrame(columns=['task_type', 'recipient', 'message', 'release', 'escalation_level'])
    k = 0
    for i in range(len(df_task_count_list)):
        temp = df_task_list[df_task_list.recipient.isin([df_task_count_list.loc[i, 'recipient']]) &
                            df_task_list.task_type.isin([df_task_count_list.loc[i, 'task_type']])]
        temp_dates = sorted(temp.release.values)
        for j in range(len(temp_dates)):
            temp_dates_ = temp_dates[j:]
            if (len(temp_dates_) - 1) == 0:
                break
            if dates.index(temp_dates_[-1]) - dates.index(temp_dates_[-2]) > 1:
                escalation_level = 'Эскалация не требуется'
                break
            if dates.index(temp_dates_[-1]) - dates.index(temp_dates_[0]) == len(temp_dates_) - 1:
                if (len(temp_dates_) - 1) > 3:
                    escalation_level = 3
                else:
                    escalation_level = len(temp_dates_) - 1
                break
        RESULTS.loc[k] = {'task_type': temp.task_type.unique()[0],
                          'recipient': temp.recipient.unique()[0],
                          'message': df_task_list[df_task_list.task_type.isin([temp.task_type.unique()[0]]) &
                                                  df_task_list.recipient.isin([temp.recipient.unique()[0]]) &
                                                  df_task_list.release.isin([temp_dates[-1]])].message.values[0],
                          'release': temp_dates[-1],
                          'escalation_level': escalation_level}
        k += 1

    # Здесь задать период, за который делаем эскалацию (последний месяц, два месяца и т.д.)
    RESULTS = RESULTS[RESULTS.release.isin(dates[-1:]) & ~RESULTS.escalation_level.isin(['Эскалация не требуется'])]
    for i in RESULTS.index:
        # message = RESULTS.loc[i, 'message']
        removal_list = ['ИСУ обычная ', 'ИСУ предупреждение ',
                        'Проанализировать и принять меры по снижению смертности. ']
        for word in removal_list:
            RESULTS.loc[i, 'message'] = RESULTS.loc[i, 'message'].replace(word, '')

    doctors = []
    leaders = []
    for i in RESULTS.index:
        if RESULTS.loc[i, 'recipient'].find('Главный врач') == 0:
            doctors.append(i)
        if RESULTS.loc[i, 'recipient'].find('Глава МО') == 0:
            leaders.append(i)

    # Для главврачей
    RESULTS_doctors = RESULTS[RESULTS.index.isin(doctors)]
    output = pd.DataFrame(columns=['escalation_recipient', 'task_type', 'original_recipient', 'message', 'release',
                                   'deadline', 'title', 'fio_recipient'])
    k = get_db_last_index('death_escalation_output')
    # if len(pd.read_sql_query('''SELECT * FROM public."death_escalation_output"''', cnx)) == 0:
    #     k = 0
    # else:
    #     k = pd.read_sql_query('''SELECT * FROM public."death_escalation_output"''', cnx).id.max()+1

    for i in RESULTS_doctors.index:
        escalation_level = RESULTS_doctors.loc[i, 'escalation_level']
        original_recipient = RESULTS_doctors.loc[i, 'recipient']
        task = escalation_recipient_text[escalation_level]

        if escalation_recipient_list[escalation_level] in FIO_dict.keys():
            FIO = FIO_dict[escalation_recipient_list[escalation_level]]
        else:
            FIO = ''

        output.loc[k] = {'escalation_recipient': escalation_recipient_list[escalation_level],
                         'task_type': RESULTS_doctors.loc[i, 'task_type'],
                         'original_recipient': original_recipient,
                         'message': 'ИСУ эскалация Эскалированная задача. {} Первоначальный исполнитель: {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(task,
                                                                                                                                                                      original_recipient,
                                                                                                                                                                      RESULTS_doctors.loc[i, 'message'],
                                                                                                                                                                      (escalation_level+1)),
                         'release': RESULTS_doctors.loc[i, 'release'],
                         'deadline': str(date.today() + pd.Timedelta(days=14)),
                         'title': 'Эскалированная задача. {}'.format(task),
                         'fio_recipient': FIO}

        if escalation_level > 1:
            for j in range(1, escalation_level):
                if escalation_recipient_list[escalation_level-j] in FIO_dict.keys():
                    FIO = FIO_dict[escalation_recipient_list[escalation_level-j]]
                else:
                    FIO = ''
                output.loc[k+escalation_level-j] = {'escalation_recipient': escalation_recipient_list[escalation_level-j],
                                                    'task_type': RESULTS_doctors.loc[i, 'task_type'],
                                                    'original_recipient': original_recipient,
                                                    'message': 'ИСУ предупреждение Задача эскалирована на уровень - {}. Первоначальный исполнитель: {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_list[escalation_level],
                                                                                                                                                                                                                 original_recipient,
                                                                                                                                                                                                                 RESULTS_doctors.loc[i, 'message'],
                                                                                                                                                                                                                 (escalation_level+1)),
                                                    'release': RESULTS_doctors.loc[i, 'release'],
                                                    'deadline': str(date.today() + pd.Timedelta(days=14)),
                                                    'title': 'Задача эскалирована на уровень - {}.'.format(escalation_recipient_list[escalation_level]),
                                                    'fio_recipient': FIO}

        if RESULTS_doctors.loc[i, 'recipient'] in FIO_dict.keys():
            FIO = FIO_dict[RESULTS_doctors.loc[i, 'recipient']]
        else:
            FIO = ''
        output.loc[k+escalation_level] = {'escalation_recipient': RESULTS_doctors.loc[i, 'recipient'],
                                          'task_type': RESULTS_doctors.loc[i, 'task_type'],
                                          'original_recipient': RESULTS_doctors.loc[i, 'recipient'],
                                          'message': 'ИСУ предупреждение ' + 'Ваша задача эскалирована на уровень - {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_list[escalation_level],
                                                                                                                                                                                 RESULTS_doctors.loc[i, 'message'],
                                                                                                                                                                                 (escalation_level+1)),
                                          'release': RESULTS_doctors.loc[i, 'release'],
                                          'deadline': str(date.today() + pd.Timedelta(days=14)),
                                          'title': 'Ваша задача эскалирована на уровень - {}.'.format(escalation_recipient_list[escalation_level]),
                                          'fio_recipient': FIO}
        k = k+1+escalation_level

    if save_to_sql:
        # Сохраняем предобработанные данные в БД
        output.to_sql('death_escalation_output', cnx, if_exists='append', index_label='id')
    if save_to_excel:
        path = r'C:\Users\oganesyanKZ\PycharmProjects\ISU_death\Рассчеты/'
        with pd.ExcelWriter(f'{path}death_escalation_output_{str(date.today())}.xlsx', engine='openpyxl') as writer:
            output.to_excel(writer, sheet_name=f'escalation_doctors', header=True, index=False, encoding='1251')

    print(f'Number of generated escalation tasks for doctors {len(output)}')
    logging.info(f'Number of generated escalation tasks for doctors {len(output)}')

    # Для глав МО
    RESULTS_leaders = RESULTS[RESULTS.index.isin(leaders)]
    output = pd.DataFrame(columns=['escalation_recipient', 'task_type', 'original_recipient', 'message', 'release',
                                   'deadline', 'title', 'fio_recipient'])

    k = get_db_last_index('death_escalation_output')
    # if len(pd.read_sql_query('''SELECT * FROM public."death_escalation_output"''', cnx)) == 0:
    #     k = 0
    # else:
    #     k = pd.read_sql_query('''SELECT * FROM public."death_escalation_output"''', cnx).id.max()+1

    for i in RESULTS_leaders.index:
        escalation_level = RESULTS_leaders.loc[i, 'escalation_level']
        if escalation_level == 3:
            if escalation_recipient_list[escalation_level] in FIO_dict.keys():
                FIO = FIO_dict[escalation_recipient_list[escalation_level]]
            else:
                FIO = ''
            output.loc[k] = {'escalation_recipient': escalation_recipient_list[escalation_level],
                             'task_type': RESULTS_leaders.loc[i, 'task_type'],
                             'original_recipient': RESULTS_leaders.loc[i, 'recipient'],
                             'message': 'ИСУ эскалация ' + 'Эскалированная задача. {} Первоначальный исполнитель: {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_text[escalation_level],
                                                                                                                                                                               RESULTS_leaders.loc[i, 'recipient'],
                                                                                                                                                                               RESULTS_leaders.loc[i, 'message'],
                                                                                                                                                                               (escalation_level+1)),
                             'release': RESULTS_leaders.loc[i, 'release'],
                             'deadline': str(date.today() + pd.Timedelta(days=14)),
                             'title': 'Эскалированная задача. {}'.format(escalation_recipient_text[escalation_level]),
                             'fio_recipient': FIO}

            if RESULTS_leaders.loc[i, 'recipient'] in FIO_dict.keys():
                FIO = FIO_dict[RESULTS_doctors.loc[i, 'recipient']]
            else:
                FIO = ''
            output.loc[k+escalation_level] = {'escalation_recipient': RESULTS_leaders.loc[i, 'recipient'],
                                              'task_type': RESULTS_leaders.loc[i, 'task_type'],
                                              'original_recipient': RESULTS_leaders.loc[i, 'recipient'],
                                              'message': 'ИСУ предупреждение ' + 'Ваша задача эскалирована на уровень - {}. Задача: {}. Задача повторяется {} месяца подряд.'.format(escalation_recipient_list[escalation_level],
                                                                                                                                                                                     RESULTS_leaders.loc[i, 'message'],
                                                                                                                                                                                     (escalation_level+1)),
                                              'release': RESULTS_leaders.loc[i, 'release'],
                                              'deadline': str(date.today() + pd.Timedelta(days=14)),
                                              'title': 'Ваша задача эскалирована на уровень - {}.'.format(escalation_recipient_list[escalation_level]),
                                              'fio_recipient': FIO}
            k = k+2

    if save_to_sql:
        # Сохраняем предобработанные данные в БД
        output.to_sql('death_escalation_output', cnx, if_exists='append', index_label='id')
    if save_to_excel:
        path = r'C:\Users\oganesyanKZ\PycharmProjects\ISU_death\Рассчеты/'
        with pd.ExcelWriter(f'{path}death_escalation_output_{str(date.today())}.xlsx', engine='openpyxl', mode='a') as writer:
            output.to_excel(writer, sheet_name=f'escalation_leaders', header=True, index=False, encoding='1251')

    print(f'Number of generated escalation tasks for leaders {len(output)}')
    logging.info(f'Number of generated escalation tasks for leaders {len(output)}')

    print('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    logging.info('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))


if __name__ == '__main__':
    logging.basicConfig(filename='logfile.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    death_escalation(save_to_sql=False, save_to_excel=True)
