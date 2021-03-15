from ISU_death_lists_dict import cnx, MKB_GROUP_LIST_MAIN
import pandas as pd
from sqlalchemy import types


def make_task_type_list():
    """
    Функция для генерации списка типов задач, исходя из основных групп МКБ
    :return: список возможных типов задач
    """
    task_type = ['Смертность_П1_55+']
    for ind, MKB in enumerate(MKB_GROUP_LIST_MAIN):
        print(ind, MKB)
        task_type.extend([f'Смертность_П2_3monthgrow_{ind}', f'Смертность_П2_sameperiod_{ind}'])
    return task_type


def make_table_death_task_type(task_type_list):
    """
    :param task_type_list: спосок возможных типов задач
    :return: df со списком возможных типов задач
    """
    return pd.DataFrame({'name': task_type_list})


if __name__ == '__main__':
    task_type = make_task_type_list()
    print(f'\n{task_type}\n')
    df_task_type = make_table_death_task_type(task_type)
    print(df_task_type)
    # df_task_type.to_sql('death_task_type', cnx, if_exists='replace', index_label='id', dtype={'name': types.VARCHAR})
