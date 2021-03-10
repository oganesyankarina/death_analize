# Анализ исходных данных от МедСофт - какой временной лаг между датой смерти и датой выдачи справки о смерти

import pandas as pd
import csv

from connect_PostGres import cnx
from ISU_death_functions import make_date

path = r'C:\Users\oganesyanKZ\PycharmProjects\ISU_death\Рассчеты/'
file = f'{path}death_202102011133.csv'


def read_csv(url):
    encoding_list = ['utf_8', 'utf_16', 'utf_32', 'cp1251', 'koi8_r', 'mac_cyrillic', 'iso8859_5', 'cp855', 'cp866']

    for enc in encoding_list:
        try:
            data = pd.read_csv(url, encoding=enc, delimiter='__', quoting=csv.QUOTE_NONE, error_bad_lines=False)
        except (UnicodeDecodeError, UnicodeError, LookupError):
            pass
        else:
            break

    if data.shape[1] == 1:
        data = pd.read_csv(url, encoding=enc, delimiter='__', quoting=csv.QUOTE_NONE, error_bad_lines=False)
    if data.shape[1] == 1:
        data = pd.read_csv(url, encoding=enc, delimiter=',', quoting=csv.QUOTE_NONE, error_bad_lines=False)

    return data


def MakeDateBornDeath(df, date_col):
    for i in df.index:
        df.loc[i, 'ДАТА_РОЖДЕНИЯ'] = df.loc[i, date_col[0]].date()
        df.loc[i, 'ДАТА_СМЕРТИ'] = df.loc[i, date_col[1]].date()
        df.loc[i, 'ДАТА_СЕРТИФИКАТА'] = df.loc[i, date_col[2]].date()
    return df

def make_month_year_death(df):
    df['МЕСЯЦ_СМЕРТИ'] = 0
    df['ГОД_СМЕРТИ'] = 0
    for i in df.index:
        df.loc[i, 'МЕСЯЦ_СМЕРТИ'] = int(df.loc[i, 'ДАТА_СМЕРТИ'].month)
        df.loc[i, 'ГОД_СМЕРТИ'] = int(df.loc[i, 'ДАТА_СМЕРТИ'].year)
    return df


def CalcDateDelta(df, date_col):
    for i in df.index:
        try:
            df.loc[i, 'дельта_свидетельствосмерть'] = abs((df.loc[i, date_col[1]] - df.loc[i, date_col[0]]).days)
        except:
            continue
    return df

if __name__ == '__main__':
    # df = read_csv(file)
    df = pd.read_sql_query('''SELECT * FROM death''', cnx)
    df.columns = ['id', 'sex', 'birth', 'death', 'address_full', 'locality', 'at_death', 'locality2',
                  'place_death', 'family', 'education', 'occupation', 'reason_death', 'reason_established',
                  'reason_a', 'period_reason_a', 'original_reasons_a', 'reason_b', 'period_reason_b',
                  'original_reasons_b', 'reason_c', 'period_reason_c', 'original_reasons_c', 'reason_d',
                  'period_reason_d', 'original_reasons_d', 'reason_2d', 'period_reason_2d', 'road_accident',
                  'date_issue_certificate']

    print('Обрабатываем данные...')
    df = make_date(df, ['birth', 'death', 'date_issue_certificate'])
    print('MakeDate done')
    df = MakeDateBornDeath(df, ['birth', 'death', 'date_issue_certificate'])
    print('MakeDateBornDeath done')
    df = make_month_year_death(df)
    print('MakeDayWeekMonthYearDeath done')
    df = CalcDateDelta(df, ['ДАТА_СМЕРТИ', 'ДАТА_СЕРТИФИКАТА'])
    print('CalcDateDelta done')

    print('Сохраняем данные...')
    with pd.ExcelWriter(f'{path}death1.xlsx', engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='death', header=True, index=False, encoding='1251')
