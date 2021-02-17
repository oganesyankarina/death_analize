# Вспомогательные функции
import warnings
import pandas as pd
import calendar
from datetime import date
from dateutil import relativedelta as rdelta
from ISU_death_lists_dict import AgeGroupList, EmployeeAgeList, Main_MKB_dict, MKB_CODE_LIST, df_MKB
from connect_PostGres import cnx

warnings.filterwarnings('ignore')


# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# Функции для предобработки данных
# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

def make_date(df, date_col):
    for col in date_col:
        df[col] = df[col].apply(pd.to_datetime)
    return df


def make_date_born_death(df, date_col):
    df['ДАТА_РОЖДЕНИЯ'] = 0
    df['ДАТА_СМЕРТИ'] = 0
    for i in df.index:
        df.loc[i, 'ДАТА_РОЖДЕНИЯ'] = df.loc[i, date_col[0]].date()
        df.loc[i, 'ДАТА_СМЕРТИ'] = df.loc[i, date_col[1]].date()
    return df


def make_day_week_month_year_death(df):
    df['ДЕНЬ_СМЕРТИ'] = 0
    df['НЕДЕЛЯ_СМЕРТИ'] = 0
    df['МЕСЯЦ_СМЕРТИ'] = 0
    df['ГОД_СМЕРТИ'] = 0
    for i in df.index:
        df.loc[i, 'ДЕНЬ_СМЕРТИ'] = int(df.loc[i, 'ДАТА_СМЕРТИ'].day)
        df.loc[i, 'НЕДЕЛЯ_СМЕРТИ'] = int(df.loc[i, 'ДАТА_СМЕРТИ'].week)
        df.loc[i, 'МЕСЯЦ_СМЕРТИ'] = int(df.loc[i, 'ДАТА_СМЕРТИ'].month)
        df.loc[i, 'ГОД_СМЕРТИ'] = int(df.loc[i, 'ДАТА_СМЕРТИ'].year)
    return df


def calculate_death_age(df):
    df['ВОЗРАСТ_СМЕРТИ'] = ''
    for i in df.index:
        if df['ДАТА_РОЖДЕНИЯ'].notna()[i]:
            df.loc[i, 'ВОЗРАСТ_СМЕРТИ'] = int('{0.years}'.format(rdelta.relativedelta(df.loc[i, 'ДАТА_СМЕРТИ'],
                                                                                      df.loc[i, 'ДАТА_РОЖДЕНИЯ'])))
    return df


def calculate_age_group(df):
    df['ВОЗРАСТНАЯ_ГРУППА'] = ''
    for i in df.index:
        if df.loc[i, 'ВОЗРАСТ_СМЕРТИ'] in AgeGroupList.keys():
            df.loc[i, 'ВОЗРАСТНАЯ_ГРУППА'] = AgeGroupList[df.loc[i, 'ВОЗРАСТ_СМЕРТИ']]
        else:
            df.loc[i, 'ВОЗРАСТНАЯ_ГРУППА'] = '100 и более'
    return df


def calculate_employee_group(df):
    df['ТРУДОСПОСОБНОСТЬ_ГРУППА'] = ''
    for i in df.index:
        if df.loc[i, 'ВОЗРАСТНАЯ_ГРУППА'] in EmployeeAgeList.keys():
            df.loc[i, 'ТРУДОСПОСОБНОСТЬ_ГРУППА'] = EmployeeAgeList[df.loc[i, 'ВОЗРАСТНАЯ_ГРУППА']]
    return df


def make_mkb(df, col_df, col_df_new1, col_df_new2):
    df[col_df_new1] = ''
    df[col_df_new2] = ''

    for i in df[df[col_df].isin(MKB_CODE_LIST)].index:
        df.loc[i, col_df_new1] = df_MKB.loc[df_MKB[df_MKB['MKB_CODE'] == df.loc[i, col_df]].index, 'MKB_NAME'].values
        df.loc[i, col_df_new2] = df_MKB.loc[df_MKB[df_MKB['MKB_CODE'] == df.loc[i, col_df]].index,
                                            'MKB_GROUP_NAME'].values

    return df


def make_address(df, col1, col2):
    for i in df.index:
        if df[col1].notna()[i]:
            address = df.loc[i, col1].split(':')
            address = address[1:]
            df.loc[i, 'РЕГИОН_{}'.format(col1)] = address[0][1:-7]
            df.loc[i, 'РАЙОН_{}'.format(col1)] = address[1][1:-18]
            df.loc[i, 'НАСЕЛЕННЫЙ ПУНКТ_{}'.format(col1)] = address[2][1:-7]
            df.loc[i, 'УЛИЦА_{}'.format(col1)] = address[3][1:]
        if df[col2].notna()[i]:
            address = df.loc[i, col2].split(':')
            address = address[1:]
            df.loc[i, 'НАСЕЛЕННЫЙ ПУНКТ_{}'.format(col2)] = address[0][1:-7]
            df.loc[i, 'УЛИЦА_{}'.format(col2)] = address[1][1:]
    return df


def find_original_reason_mkb_group_name(df):
    for i in df.index:
        temp = [df.loc[i, 'original_reason_a'], df.loc[i, 'original_reason_b'],
                df.loc[i, 'original_reason_v'], df.loc[i, 'original_reason_g']]
        if '1' in temp:
            main_mkb_index = temp.index('1')
            main_mkb_group = Main_MKB_dict[main_mkb_index]
            main_mkb_group_original_reason = df.loc[i, main_mkb_group]
            df.loc[i, 'MKB_GROUP_NAME_original_reason'] = main_mkb_group_original_reason
        else:
            df.loc[i, 'MKB_GROUP_NAME_original_reason'] = 'основная причина смерти не установлена'
    return df


def time_factor_calculation(year, month):
    year = int(year)
    month = int(month)
    amount_days = calendar.monthrange(year, month)[1]
    start = date(year, 1, 1)

    if month == 12:
        accumulation_period_length = (date(year + 1, 1, 1) - start).days
    else:
        accumulation_period_length = (date(year, month + 1, 1) - start).days

    accumulation_period_end_length = (date(year + 1, 1, 1) - start).days
    time_factor_month = round(accumulation_period_end_length / amount_days, 4)
    time_factor_period = round(accumulation_period_end_length / accumulation_period_length, 4)

    return time_factor_month, time_factor_period

# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# Функции для main (проверка на последний день месяца)
# /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
def get_df_death_finished():
    # Данные после первоначальной предобработки из таблицы death_finished
    df_death_finished = pd.read_sql_query('''SELECT * FROM public."death_finished"''', cnx)
    YEARS = sorted(df_death_finished['year_death'].unique())
    MONTHS = sorted(df_death_finished['month_death'].unique())
    DATES = sorted(df_death_finished['DATE'].unique())
    GENDERS = sorted(df_death_finished['gender'].unique())
    AGE_GROUPS = sorted(df_death_finished['age_group_death'].unique())

    return df_death_finished, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS

def amount_days_in_month(date_input):
    date_ = pd.to_datetime(date_input)
    year = date_.year
    month = date_.month
    day = date_.day

    if month in [1, 3, 5, 7, 8, 10, 12]:
        num = 31
    elif month in [4, 6, 9, 11]:
        num = 30
    elif month == 2:
        if calendar.isleap(year):
            num = 29
        else:
            num = 28
    else:
        print("Некорректный месяц")
        num = 0

    if 0 < day < num:
        is_the_end = False
    elif day == num:
        is_the_end = True
    else:
        print("Некорректный день месяца")
        is_the_end = False

    return is_the_end


def get_db_last_index(name_db):
    if len(pd.read_sql_query(f'''SELECT * FROM public.{name_db}''', cnx)) == 0:
        k = 0
    else:
        k = pd.read_sql_query(f'''SELECT * FROM public.{name_db}''', cnx).id.max()+1
    return k


if __name__ == '__main__':
    print(get_db_last_index('death'))

    df_death_finished, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS = get_df_death_finished()
    print(YEARS, MONTHS, DATES[-3:], GENDERS, AGE_GROUPS)
