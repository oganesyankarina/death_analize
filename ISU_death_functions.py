# Вспомогательные функции
import warnings
import pandas as pd
import calendar
from datetime import date
from dateutil import relativedelta as rdelta

from connect_PostGres import cnx
from ISU_death_lists_dict import AgeGroupList, EmployeeAgeList, Main_MKB_dict, MKB_CODE_LIST, df_MKB
from ISU_death_lists_dict import MONTHS_dict, FIO_dict, MKB_GROUP_LIST_MAIN, escalation_recipient_list

warnings.filterwarnings('ignore')
########################################################################################################################
# Функции для работы с базой данных


def get_db_last_index(name_db):
    """
    Функция ищет максимальное значение индекса в таблице из базы данных
    :param name_db: название таблицы из базы данных
    :return: следующее значение для индекса в таблице из базы данных
    """
    if len(pd.read_sql_query(f'''SELECT id FROM public.{name_db}''', cnx)) == 0:
        return 1
    else:
        return pd.read_sql_query(f'''SELECT id FROM public.{name_db}''', cnx).id.max()+1


def get_df_death_finished():
    """
    Функция получает данные после первоначальной предобработки из таблицы death_finished
    :return: df после предварительной обработки, основные списки
    """
    df = pd.read_sql_query('''SELECT * FROM public."death_finished"''', cnx)
    YEARS = sorted(df['year_death'].unique())
    MONTHS = sorted(df['month_death'].unique())
    DATES = sorted(df['date_period'].unique())
    GENDERS = sorted(df['gender'].unique())
    AGE_GROUPS = sorted(df['age_group_death'].unique())

    return df, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS
########################################################################################################################
# Функции для предобработки данных


def make_day_week_month_year_death(df):
    df['ДЕНЬ_СМЕРТИ'] = 0
    df['НЕДЕЛЯ_СМЕРТИ'] = 0
    df['МЕСЯЦ_СМЕРТИ'] = 0
    df['ГОД_СМЕРТИ'] = 0
    for i in df.index:
        df.loc[i, 'ДЕНЬ_СМЕРТИ'] = int(df.loc[i, 'Дата смерти'].day)
        df.loc[i, 'НЕДЕЛЯ_СМЕРТИ'] = int(df.loc[i, 'Дата смерти'].week)
        df.loc[i, 'МЕСЯЦ_СМЕРТИ'] = int(df.loc[i, 'Дата смерти'].month)
        df.loc[i, 'ГОД_СМЕРТИ'] = int(df.loc[i, 'Дата смерти'].year)
    return df


def calculate_death_age(df):
    df['ВОЗРАСТ_СМЕРТИ'] = ''
    for i in df.index:
        if df['Дата рождения'].notna()[i]:
            df.loc[i, 'ВОЗРАСТ_СМЕРТИ'] = int('{0.years}'.format(rdelta.relativedelta(df.loc[i, 'Дата смерти'],
                                                                                      df.loc[i, 'Дата рождения'])))
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
        df.loc[i, col_df_new1] = df_MKB.loc[df_MKB[df_MKB['mkb_code'] == df.loc[i, col_df]].index, 'mkb_name'].values
        df.loc[i, col_df_new2] = df_MKB.loc[df_MKB[df_MKB['mkb_code'] == df.loc[i, col_df]].index,
                                            'mkb_group_name'].values
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
    """
    Функция для определения первоначальной причины смерти
    :param df:
    :return: добавляет в df столбец 'MKB_GROUP_NAME_original_reason' с указанием названия Группы МКБ, в которую входит
    заболевание, послужившее первоначальной причиной смерти
    """
    for i in df.index:
        temp = [df.loc[i, 'original_reason_a'], df.loc[i, 'original_reason_b'],
                df.loc[i, 'original_reason_v'], df.loc[i, 'original_reason_g']]
        if '1' in temp:
            main_mkb_index = temp.index('1')
            main_mkb_group = Main_MKB_dict[main_mkb_index]
            main_mkb_group_original_reason = df.loc[i, main_mkb_group]
            df.loc[i, 'mkb_group_name_original_reason'] = main_mkb_group_original_reason
        else:
            df.loc[i, 'mkb_group_name_original_reason'] = 'основная причина смерти не установлена'
    return df
########################################################################################################################
# Функции для правил


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
########################################################################################################################
# Функции для main (проверка на последний день месяца)


def amount_days_in_month(date_input):
    """
    Функция для проверки является ли date_input последним днем месяца
    :param date_input: дата, которую необходимо проверить
    :return: is_the_end принимает значение True - конец месяца, False - не конец месяца
    """
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
########################################################################################################################
# Функции для подготовки таблиц с задачами


def make_corr_for_recipient(mo):
    if (mo == 'Липецк') | (mo == 'Елец'):
        return ''
    else:
        return ' район'


def make_recipient(mo):
    corr_ = make_corr_for_recipient(mo)
    return f'Главный врач ЦРБ {mo}{corr_}'


def make_recipient_fio(recipient_):
    if recipient_ in FIO_dict.keys():
        return FIO_dict[recipient_][0]
    else:
        return ''


def make_recipient_uuid(recipient_):
    if recipient_ in FIO_dict.keys():
        return FIO_dict[recipient_][1]
    else:
        return ''


def make_escalation_recipient_fio(escalation_level_):
        if escalation_recipient_list[escalation_level_] in FIO_dict.keys():
            return FIO_dict[escalation_recipient_list[escalation_level_]][0]
        else:
            return ''


def make_escalation_recipient_uuid(escalation_level_):
    if escalation_recipient_list[escalation_level_] in FIO_dict.keys():
        return FIO_dict[escalation_recipient_list[escalation_level_]][1]
    else:
        return ''


def make_release_date(date_):
    if date_.month == 12:
        return date(date_.year + 1, 1, 1)
    else:
        return date(date_.year, date_.month + 1, 1)


if __name__ == '__main__':
    print(get_db_last_index('death'))

    df_death_finished, YEARS, MONTHS, DATES, GENDERS, AGE_GROUPS = get_df_death_finished()
    print(YEARS, MONTHS, DATES[-3:], GENDERS, AGE_GROUPS)
