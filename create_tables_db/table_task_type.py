from ISU_death_lists_dict import MKB_GROUP_LIST_MAIN
from connect_PostGres import cnx
import pandas as pd
import uuid
from sqlalchemy import types


def make_task_type_list():
    """
    Функция для генерации списка типов задач, исходя из основных групп МКБ
    :return: список возможных типов задач
    """
    task_type = ['Смертность_П1_55+']
    for ind, MKB in enumerate(MKB_GROUP_LIST_MAIN):
        task_type.extend([f'Смертность_П2_3monthgrow_{ind}', f'Смертность_П2_sameperiod_{ind}'])
    return task_type


def make_table(name_list):
    """
    :param name_list: спосок возможных типов задач
    :return: df со списком возможных типов задач
    """
    return pd.DataFrame({'name': name_list, 'uuid': [uuid.uuid3(uuid.NAMESPACE_DNS, x) for x in name_list]},
                        index=[x for x in range(1, len(name_list)+1)])


if __name__ == '__main__':
    task_type = make_task_type_list()
    df_task_type = make_table(task_type)
    df_task_type.to_sql('death_task_type', cnx, if_exists='append', index_label='id')
