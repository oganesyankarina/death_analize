from ISU_death_lists_dict import cnx, MKB_GROUP_LIST_MAIN
import pandas as pd
from sqlalchemy import types


def make_table_death_task_type():
    task_type = ['Смертность_П1_55+']
    for ind, MKB in enumerate(MKB_GROUP_LIST_MAIN):
        print(ind, MKB)
        task_type.extend([f'Смертность_П2_3monthgrow_{ind}', f'Смертность_П2_sameperiod_{ind}'])
    print(task_type)

    df_task_type = pd.DataFrame({'name': task_type})
    print(df_task_type)

    df_task_type.to_sql('death_task_type', cnx, if_exists='replace', index_label='id', dtype={'name': types.VARCHAR})


if __name__ == '__main__':
    # make_table_death_task_type()
    pass

