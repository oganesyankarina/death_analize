from ISU_death_lists_dict import MKB_GROUP_LIST_MAIN, escalation_recipient_list, escalation_recipient_text
from connect_PostGres import cnx
import pandas as pd
import uuid
from sqlalchemy import types

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)


def make_task_list():
    """
    Функция для генерации списка типов задач, исходя из основных групп МКБ
    :return: список возможных типов задач
    """
    task_type = ['Смертность_П1_55+']
    task_title = ['Уровень смертности не соответствует возрастной структуре населения района']
    for ind, MKB in enumerate(MKB_GROUP_LIST_MAIN):
        task_type.extend([f'Смертность_П2_3monthgrow_{ind}',
                          f'Смертность_П2_sameperiod_{ind}'])
        task_title.extend([f'Рост смертности от заболеваний из группы {MKB}',
                           f'Рост смертности от заболеваний из группы {MKB} по сравнению с АППГ'])
    for ind, escalation_recipient in enumerate(escalation_recipient_list.values()):
        if ind == 0:
            task_type.extend([f'Уведомление исполнителя. Эскалация на уровень - {escalation_recipient}'])
            task_title.extend([f'Ваша задача эcкалирована на уровень - {escalation_recipient}'])
        else:
            task_type.extend([f'Уведомление исполнителя. Эскалация на уровень - {escalation_recipient}',
                              f'Уведомление. Эскалация на уровень - {escalation_recipient}'])
            task_title.extend([f'Ваша задача эcкалирована на уровень - {escalation_recipient}',
                               f'Задача эcкалирована на уровень - {escalation_recipient}'])
    for escalation_recipient in escalation_recipient_text.values():
        task_type.extend([f'Эскалированная задача. {escalation_recipient}'])
        task_title.extend([f'Эскалированная задача. {escalation_recipient}'])
    return task_type, task_title


def make_table(name_list, title_list):
    return pd.DataFrame({'name': name_list, 'title': title_list,
                         'uuid': [uuid.uuid3(uuid.NAMESPACE_DNS, x) for x in [' '.join(list(tup)) for tup in zip(name_list, title_list)]]},
                        index=[x for x in range(1, len(name_list)+1)])


if __name__ == '__main__':
    print(make_task_list())
    df_task_type = make_table(make_task_list()[0], make_task_list()[1])
    print(df_task_type)
    # print([' '.join(list(tup)) for tup in zip(make_task_list()[0], make_task_list()[1])])
    df_task_type.to_sql('death_task_type', cnx, if_exists='append', index_label='id')
    # print([x for x in escalation_recipient_list.values()])
    # print([x for x in escalation_recipient_text.values()])
    #
    # task_type = []
    # task_title = []
    # for ind, escalation_recipient in enumerate(escalation_recipient_list.values()):
    #     print(ind, escalation_recipient)
    #     if ind == 0:
    #         task_type.extend([f'Уведомление исполнителя. Эскалация на уровень - {escalation_recipient}'])
    #         task_title.extend([f'Ваша задача эcкалирована на уровень - {escalation_recipient}'])
    #     else:
    #         task_type.extend([f'Уведомление исполнителя. Эскалация на уровень - {escalation_recipient}',
    #                           f'Уведомление. Эскалация на уровень - {escalation_recipient}'])
    #         task_title.extend([f'Ваша задача эcкалирована на уровень - {escalation_recipient}',
    #                            f'Задача эcкалирована на уровень - {escalation_recipient}'])
    # print([list(tup) for tup in zip(task_type, task_title)])

