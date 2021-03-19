import pandas as pd
import uuid
from create_tables_db.table_task_type import make_table
from connect_PostGres import cnx


path = r'J:\~ 09_Машинное обучение_Прогноз показателей СЭР\!!!БАЗА ЗНАНИЙ!!!\Демография\Численность населения по МО Липецкой области'
Population_file_name = path+'/Возрастно-половой состав по МО ЛО.xlsx'
df = pd.read_excel(Population_file_name, sheet_name='Численность', header=0)

temp1 = sorted(df.Feature.unique())
temp2 = sorted(df.Region.unique())
temp3 = sorted(df.Territory.unique())
temp4 = sorted(df.Gender.unique())
df.Age = df.Age.astype('str')
temp5 = sorted(df.Age.unique())


if __name__ == '__main__':
    # print(temp1)
    # print(temp2)
    # print(temp3)
    # print(temp4)
    # print(temp5)

    # make_table(temp1).to_sql('features', cnx, if_exists='append', index_label='id')
    # make_table(temp2).to_sql('mo', cnx, if_exists='append', index_label='id')
    # make_table(temp3).to_sql('territory', cnx, if_exists='append', index_label='id')
    # make_table(temp4).to_sql('gender', cnx, if_exists='append', index_label='id')
    # make_table(temp5).to_sql('age', cnx, if_exists='append', index_label='id')
    print(df.columns)
    print(pd.read_sql_query('''SELECT * FROM public."population"''', cnx).columns)

    df_mo = pd.read_sql_query('''SELECT "name", "uuid" FROM public."mo"''', cnx)
    mo_dict = dict(zip(df_mo.name, df_mo.uuid))
    df['mo_uuid'] = [mo_dict[i] for i in df['Region'].values]

    df_ter = pd.read_sql_query('''SELECT "name", "uuid" FROM public."territory"''', cnx)
    ter_dict = dict(zip(df_ter.name, df_ter.uuid))
    df['territory_uuid'] = [ter_dict[i] for i in df['Territory'].values]

    df_gen = pd.read_sql_query('''SELECT "name", "uuid" FROM public."gender"''', cnx)
    gen_dict = dict(zip(df_gen.name, df_gen.uuid))
    df['gender_uuid'] = [gen_dict[i] for i in df['Gender'].values]

    df_age = pd.read_sql_query('''SELECT "name", "uuid" FROM public."age"''', cnx)
    age_dict = dict(zip(df_age.name, df_age.uuid))
    df['age_group_uuid'] = [age_dict[i] for i in df['Age'].values]
    df.rename(columns={'Year': 'year', 'Value': 'value'}, inplace=True)

    print(df.columns)
    df[['year', 'value', 'mo_uuid', 'territory_uuid',
        'gender_uuid', 'age_group_uuid']].to_sql('population', cnx, if_exists='append', index_label='id')

    pass
