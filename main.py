from urllib.parse import quote
from sqlalchemy import create_engine
import requests
import json
import pandas as pd
import config

NUM_OF_USERS = 4500

hostname = config.hostname
dbname = config.dbname
uname = config.uname
pwd = config.pwd

engine = create_engine(
    f'mysql+mysqlconnector://{uname}:%s@{hostname}:3306/{dbname}' % quote(pwd))


def create_df(num_of_users):
    print('start - create_df')
    results = requests.get(
        f'https://randomuser.me/api/?results={num_of_users}')
    users = json.loads(results.text)
    df = pd.DataFrame()
    for user in users.get('results'):
        df = df.append(pd.json_normalize(user, sep='_'))
    print('end - create_df')
    return df


def create_gender_tables(df):
    print('start - create_gender_tables')
    gender_op = ['male', 'female']
    for gender in gender_op:
        df_genders = df.loc[df['gender'] == gender]
        df_genders.to_sql(
            f"elinor_test_{gender}", engine, index=False, if_exists='replace')
    print('end - create_gender_tables')


def create_age_tables(df):
    print('start - create_age_tables')
    # generate fixed bins
    bin_range = [bin for bin in range(10, 120, 10)]
    labels = bin_range[:-1]
    for age_range in labels:
        bin = pd.cut(df.dob_age, bin_range, right=False, labels=labels)
        df_ages = df.loc[bin == age_range]
        df_ages.to_sql(
            f"elinor_test_{str(age_range)[:-1]}", engine, index=False, if_exists='replace')
    print('end - create_age_tables')


'''
1. SQL execution is on DB since there is no need to bring the data to pandas, 
   I pushed down all execution to the DB.
2. I didn't see that MySQL supports 'create or replace' option.
3. I tried using 'create table as', but got an error:
   'Error Code: 1786 Statement violates GTID consistency: CREATE TABLE ... SELECT'
4. The best way i found to create the table was using the 'LIKE' option
'''
def create_top_20():
    print('start - create_top_20')
    sqls = [(
            f"DROP TABLE IF EXISTS {dbname}.elinor_test_20 "
            ),
            (
            f"CREATE TABLE IF NOT EXISTS {dbname}.elinor_test_20 "
            f"LIKE {dbname}.elinor_test_female "
            ),
            (
            f"INSERT {dbname}.elinor_test_20 "
            f"(SELECT * "
            f"FROM {dbname}.elinor_test_female "
            f"ORDER BY registered_date DESC "
            f"LIMIT 10) "
            f"UNION ALL"
            f"(SELECT * "
            f"FROM {dbname}.elinor_test_male "
            f"ORDER BY registered_date DESC "
            f"LIMIT 10)"
            )
            ]
    with engine.connect() as con:
        for sql in sqls:
            con.execute(sql)
    print('end - create_top_20')


def union_dfs(query1, query2, drop_duplicates, file_name):
    df1 = pd.read_sql(query1, engine)
    df2 = pd.read_sql(query2, engine)
    if drop_duplicates:
        frame_union = pd.concat(
            [df1, df2], ignore_index=True).drop_duplicates()
    else:
        frame_union = pd.concat([df1, df2], ignore_index=True)
    create_json(frame_union, file_name)


def create_json(df, file_name):
    df.to_json(f"{file_name}.json",
               orient="table",
               index=False)
    print(f'created {file_name}.json succesfully')


def main():
    df = create_df(NUM_OF_USERS)
    create_gender_tables(df)
    create_age_tables(df)
    create_top_20()
    union_dfs(f"SELECT * FROM {dbname}.elinor_test_20",
              f"SELECT * FROM {dbname}.elinor_test_5", True, 'first')
    union_dfs(f"SELECT * FROM {dbname}.elinor_test_20",
              f"SELECT * FROM {dbname}.elinor_test_2", False, 'second')


if __name__ == "__main__":
    main()
