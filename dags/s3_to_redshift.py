import psycopg2
import configparser
import logging


def s3_to_redshfit():
    logging.info("Reading AWS Credentials")
    config = configparser.ConfigParser()
    config.read_file(open('/opt/airflow/credentials/aws.cfg'))

    AWS_ACCESS_KEY_ID = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
    DWH_ENDPOINT = config.get('DWH', 'DWH_ENDPOINT')
    DWH_PORT = config.get('DWH', 'DWH_PORT')
    DWH_DATABASE = config.get('DWH', 'DWH_DATABASE')
    DWH_DATABASE_USER = config.get('DWH', 'DWH_DATABASE_USER')
    DWH_DATABASE_PASSWORD = config.get('DWH', 'DWH_DATABASE_PASSWORD')

    con = psycopg2.connect(dbname= DWH_DATABASE, host=DWH_ENDPOINT, port= DWH_PORT, \
                            user= DWH_DATABASE_USER, password= DWH_DATABASE_PASSWORD)
    con.autocommit = True

    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS public.de_job")

    cur.execute(
    """CREATE TABLE IF NOT EXISTS public.de_job(
        job_title VARCHAR NOT NULL,
        salary_min INTEGER NOT NULL,
        salary_max INTEGER NOT NULL,
        company_name VARCHAR NOT NULL,
        rating REAL NOT NULL,
        location_city VARCHAR NOT NULL,
        location_state VARCHAR NOT NULL,
        hq_city VARCHAR NOT NULL,
        hq_state VARCHAR NOT NULL,
        founded INTEGER NOT NULL,
        industry VARCHAR NOT NULL
        );
    """)

    sql = """
    COPY %s FROM '%s'
    CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'
    DELIMITER ','
    IGNOREHEADER 1
    FORMAT CSV
    """ % ("de_job", 's3://tanawat-s3/clean-data/DataEngineer.csv', AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

    cur.execute(sql)

    cur.execute("SELECT count(*) FROM de_job")
    print(cur.fetchall())

