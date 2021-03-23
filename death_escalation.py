""" Алгоритм по эскалации задач """
from datetime import date, datetime
import logging
import pandas as pd
import uuid

from connect_PostGres import cnx
from ISU_death_lists_dict import FIO_dict, MONTHS_dict, MKB_GROUP_LIST_MAIN, escalation_recipient_text, escalation_recipient_list
from ISU_death_functions import get_db_last_index, make_escalation_recipient_fio, make_recipient_fio, make_recipient_uuid
from ISU_death_lists_dict import results_files_path, results_files_suff, task_type_dict, get_key, main_task_type

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)


def death_escalation(save_to_sql=True, save_to_excel=False):
    start_time = datetime.now()
    program = 'death_escalation'
    logging.info(f'{program} started')
    print(f'{program} started')
########################################################################################################################
    # Считаем задачи
    df_task_list = pd.read_sql_query('''SELECT "release" FROM public."death_output"''', cnx)
    dates = sorted(df_task_list.release.unique())
    dates = dates[-4:]

    df_task_list = pd.read_sql_query('''SELECT "recipient_uuid", "message", "release", "task_type_uuid" FROM public."death_output"''', cnx)
    df_task_list = df_task_list[df_task_list.release.isin(dates) & df_task_list.task_type_uuid.isin(main_task_type)]

    df_task_count_list = pd.DataFrame(columns=['task_type_uuid', 'recipient_uuid', 'task_count'])
    k = 0
    for i in df_task_list.task_type_uuid.unique():
        for j in df_task_list.recipient_uuid.unique():
            df_task_count_list.loc[k] = {'task_type_uuid': i, 'recipient_uuid': j,
                                         'task_count': len(df_task_list[df_task_list.task_type_uuid.isin([i]) &
                                                                        df_task_list.recipient_uuid.isin([j])])}
            k += 1
    # оставляем только задачи, которые выставлялись на исполнителя более одного раза
    df_task_count_list = df_task_count_list[df_task_count_list.task_count > 1]
    df_task_count_list = df_task_count_list.sort_values(['task_count'], ascending=False)
    df_task_count_list.index = range(df_task_count_list.shape[0])
########################################################################################################################
    # Итоговая реализация алгоритма
    RESULTS = pd.DataFrame(columns=['task_type_uuid', 'recipient_uuid', 'message', 'release', 'escalation_level'])
    k = 0
    for i in range(len(df_task_count_list)):
        temp = df_task_list[df_task_list.recipient_uuid.isin([df_task_count_list.loc[i, 'recipient_uuid']]) &
                            df_task_list.task_type_uuid.isin([df_task_count_list.loc[i, 'task_type_uuid']])]
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
        RESULTS.loc[k] = {'task_type_uuid': temp.task_type_uuid.unique()[0],
                          'recipient_uuid': temp.recipient_uuid.unique()[0],
                          'message': df_task_list[df_task_list.task_type_uuid.isin([temp.task_type_uuid.unique()[0]]) &
                                                  df_task_list.recipient_uuid.isin([temp.recipient_uuid.unique()[0]]) &
                                                  df_task_list.release.isin([temp_dates[-1]])].message.values[0],
                          'release': temp_dates[-1],
                          'escalation_level': escalation_level}
        k += 1
    # Здесь задать период, за который делаем эскалацию (последний месяц, два месяца и т.д.)
    RESULTS = RESULTS[RESULTS.release.isin(dates[-1:]) & ~RESULTS.escalation_level.isin(['Эскалация не требуется'])]
    for i in RESULTS.index:
        # message = RESULTS.loc[i, 'message']
        removal_list = ['ИСУ обычная ', 'ИСУ предупреждение ', 'Проанализировать и принять меры по снижению смертности. ']
        for word in removal_list:
            RESULTS.loc[i, 'message'] = RESULTS.loc[i, 'message'].replace(word, '')
########################################################################################################################
    output = pd.DataFrame(columns=['recipient_uuid', 'message', 'deadline', 'release',
                                   'task_type_uuid', 'uuid', 'title'])
    k = get_db_last_index('death_output')

    for i in RESULTS.index:
        escalation_level = RESULTS.loc[i, 'escalation_level']
        escalation_recipient = escalation_recipient_list[escalation_level]
        escalation_recipient_uuid = make_recipient_uuid(escalation_recipient)

        original_recipient_uuid = RESULTS.loc[i, 'recipient_uuid']
        original_recipient = get_key(FIO_dict, original_recipient_uuid)

        release = RESULTS.loc[i, 'release']
        message_insert = RESULTS.loc[i, 'message']

        # Предупреждение для первоначального исполнителя
        fio = make_recipient_fio(original_recipient)
        message = f'ИСУ предупреждение Ваша задача эскалирована на уровень - {escalation_recipient}. Задача: {message_insert}. Задача повторяется {escalation_level + 1} месяца подряд.'
        title = f'Ваша задача эскалирована на уровень - {escalation_recipient}.'
        output.loc[k] = {'recipient_uuid': original_recipient_uuid,
                         'message': message,
                         'deadline': str(date.today() + pd.Timedelta(days=14)),
                         'release': release,
                         'task_type_uuid': task_type_dict[get_key(task_type_dict, title)][0],
                         'title': title,
                         'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, f'{original_recipient_uuid}{release}{message}')}
        k += 1
    # Задача для получателя на уровень эскалации и уведомления на промежуточные уровни
    for escalation_level in RESULTS.escalation_level.unique():
        temp = RESULTS[RESULTS.escalation_level.isin([escalation_level])]
        for task_type in temp.task_type_uuid.unique():
            temp1 = temp[temp.task_type_uuid.isin([task_type])]
            escalation_recipient = escalation_recipient_list[escalation_level]

            original_recipient_list = [get_key(FIO_dict, original_recipient_uuid) for original_recipient_uuid in temp1.recipient_uuid.values]
            original_recipient = ', '.join(original_recipient_list)

            task = escalation_recipient_text[escalation_level]
            release = temp1.release.unique()[0]
            message_insert = temp1.message.unique()[0]
            # Задача для получателя на уровень эскалации
            fio = make_escalation_recipient_fio(escalation_level)
            message = f'ИСУ эскалация Эскалированная задача. {task} Первоначальные исполнители: {original_recipient}. Задача: {message_insert}. Задача повторяется {escalation_level + 1} месяца подряд.'
            title = f'Эскалированная задача. {task}'
            output.loc[k] = {'recipient_uuid': escalation_recipient_uuid,
                             'message': message,
                             'deadline': str(date.today() + pd.Timedelta(days=14)),
                             'release': release,
                             'task_type_uuid': task_type_dict[get_key(task_type_dict, title)][0],
                             'title': title,
                             'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, f'{escalation_recipient_uuid}{release}{message}')}
            # Предупреждения на промежуточные уровни
            if escalation_level > 1:
                for j in range(1, escalation_level):
                    fio = make_escalation_recipient_fio(escalation_level - j)
                    message = f'ИСУ предупреждение Задача эскалирована на уровень - {escalation_recipient}. Первоначальныe исполнители: {original_recipient}. Задача: {message_insert}. Задача повторяется {escalation_level + 1} месяца подряд.'
                    title = f'Задача эскалирована на уровень - {escalation_recipient}.'
                    output.loc[k + escalation_level - j] = {
                        'recipient_uuid': make_recipient_uuid(escalation_recipient_list[escalation_level - j]),
                        'message': message,
                        'deadline': str(date.today() + pd.Timedelta(days=14)),
                        'release': release,
                        'task_type_uuid': task_type_dict[get_key(task_type_dict, title)][0],
                        'title': title,
                        'uuid': uuid.uuid3(uuid.NAMESPACE_DNS, f'{make_recipient_uuid(escalation_recipient_list[escalation_level - j])}{release}{message}')}
            k += escalation_level
########################################################################################################################
    if save_to_sql:
        output.to_sql('death_output', cnx, if_exists='append', index_label='id')
    if save_to_excel:
        with pd.ExcelWriter(f'{results_files_path}death_escalation_output_{results_files_suff}.xlsx', engine='openpyxl') as writer:
            output.to_excel(writer, sheet_name=f'escalation_doctors', header=True, index=False, encoding='1251')
########################################################################################################################
    print(f'Number of generated escalation tasks {len(output)}')
    logging.info(f'Number of generated escalation tasks {len(output)}')
########################################################################################################################
    print('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
    logging.info('{} done. elapsed time {}'.format(program, (datetime.now() - start_time)))
########################################################################################################################


if __name__ == '__main__':
    logging.basicConfig(filename='logfile.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                        level=logging.INFO)
    death_escalation(save_to_sql=False, save_to_excel=True)
