import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os

def create_db(db_credential_info):
    #pass db_info into variables
    db_host, db_user, db_password, db_name = db_credential_info
    #check if database exists
    if check_db_exists(db_credential_info):
        pass
    else:
        print('Creating new database')
        conn = psycopg2.connect(host=db_host, database=db_name,user=db_user, password = db_password)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("CREATE DATABASE %s ;" % db_name)
        cur.close()

def check_db_exists(db_credential_info):
    db_host, db_user, db_password, db_name = db_credential_info
    try:
        conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
        cur = conn.cursor()
        cur.close()
        print('Database exists.')
        return True
    except:
        print('Database does not exists.')
        return False

def create_mkt_tables(db_credential_info)
    db_host, db_user, db_password, db_name = db_credential_info
    con = None

    if check_db_exists(db_credential_info):
        commands = (
            """
            CREATE TABLE exchange(
                id SERIAL PRIMARY KEY,
                abbrev TEXT NOT NULL,
                name TEXT NOT NULL,
                currency VARCHAR(64) NULL,
                created_date TIMESTAMP NOT NULL,
                last_updated_date TIMESTAMP NOT NULL 
                )
            """
                ,
            """
            CREATE TABLE data_vendor(
                id,
                name,
                website_url,
                created_date,
                last_updated_date 
                )"""
                ,
            """
            CREATE TABLE symbol(
                id,
                exchange_id,
                ticker,
                instrument,
                name,
                sector,
                currency,
                created_date,
                last_updated_date,
                FOREIGN_KEY (exchange_id) REFERENCES 
                
                )""",
            """
            CREATE TABLE daily_data(
                id,
                date_vendor_id,
                stock_id,
                created_date,
                last_updated_date,
                date_price,
                open_price,
                high_price,
                low_price,
                adj_close_price,
                volume,
                FOREIGN KEY,
                FOREIGN KEY
            )   
            """
        )
        try:
            for command in commands:
                print('Building tables.')
                conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_password)
                cur = con.cursor()
                cur.execute(command)
                conn.commit()
                cur.close()
        except(Exeception,  psycopg2.DatabaseError) as error:
            print(error)
            cur.close()
        finally:
            if conn:
                conn.close()
    else:
        print("Database does not exists.")
        pass

def load_db_credential_info(f_name_path):
    cur_path = os.getcwd()
    #file with database name and user & password
    f = open(cur_path + f_name_path, 'r')
    lines = f.readlines()[1:]
    lines = lines[0].split(',')
    return lines

def main()
    #database credential filename
    db_credential_info = "database_info.txt"
    #create a path version
    db_credential_info_p = "\\" + db_credential_info

    #create instance variables.
    db_host,db_user,db_password,db_name = load_db_credential_info(db_credential_info)

    create_db(db_host,db_user,db_password,db_name)

    create_mkt_tables([db_host,db_user ,db_password,db_name])

if __name__ == "__main__":
    main()


