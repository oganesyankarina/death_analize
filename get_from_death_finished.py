import pandas as pd
from connect_PostGres import cnx

global df_death_finished, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS

def get_df_death_finished():

    # Данные после первоначальной предобработки из таблицы death_finished
    df_death_finished = pd.read_sql_query('''SELECT * FROM public."death_finished"''', cnx)
    YEARS = sorted(df_death_finished['year_death'].unique())
    MONTHS = sorted(df_death_finished['month_death'].unique())
    DATES = sorted(df_death_finished['DATE'].unique())
    GENDERS = sorted(df_death_finished['gender'].unique())
    AGE_GROUPS = sorted(df_death_finished['age_group_death'].unique())


if __name__ == '__main__':
    get_df_death_finished()
    print(YEARS, MONTHS, DATES[-3:], GENDERS, AGE_GROUPS)
