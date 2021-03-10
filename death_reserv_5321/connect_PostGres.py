from sqlalchemy import create_engine

POSTGRES_ADDRESS = '10.248.23.152'
POSTGRES_PORT = '5432'
POSTGRES_USERNAME = 'isu'
POSTGRES_PASSWORD = 'isupass'
POSTGRES_DBNAME = 'isu_db'

postgres_str = ('postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(username=POSTGRES_USERNAME,
                                                                                        password=POSTGRES_PASSWORD,
                                                                                        ipaddress=POSTGRES_ADDRESS,
                                                                                        port=POSTGRES_PORT,
                                                                                        dbname=POSTGRES_DBNAME))
cnx = create_engine(postgres_str)

if __name__ == '__main__':
    print(cnx)
