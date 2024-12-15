from utils.get_config_data import get_mysql_config_db
from utils.get_config_data import get_sql_server_config_db
from utils.get_config_data import get_gmail_config
import sqlalchemy as sal
import pymysql
import pyodbc
import pandas as pd
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
logging.basicConfig(filename='data_migration.log',level=logging.INFO, filemode='w',
                    format='%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    datefmt='%d:%b:%y %H:%M:%S')


def mysql_db_conn():
    try:
        cred = get_mysql_config_db()
        mysql_engine = sal.create_engine(f"mysql+pymysql://{cred['username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}")
        mysql_conn = mysql_engine.connect()
        logging.info('sucessfully connect to the mysql_db')
        return mysql_conn
    except Exception as e:
        logging.info(f"Error connecting to MySQL: {e}")

    
def sql_server_db_conn():
    try:
        sql_cred = get_sql_server_config_db()
        sql_server_engine = sal.create_engine(f"mssql+pyodbc://{sql_cred['host']}/{sql_cred['database']}?driver={sql_cred['driver'].replace(' ', '+')}")
        sql_server_conn= sql_server_engine.connect()
        logging.info('sucessfully connect to the sql_server_db')
        return sql_server_conn
    
    except Exception as e:
        logging.info(f"Error connecting to SQL Server: {e}")
        
    
    
def data_migration():
    try:
        mysql_conn = mysql_db_conn()
        sql_server_conn = sql_server_db_conn()
     
        query = "SELECT * FROM hdfc_customer"
        df = pd.read_sql(query, mysql_conn)
        logging.info('data read successfully')

        
        df.to_sql('hdfc_cust_tgt', sql_server_conn, if_exists='replace', index=False)
        logging.info('data successfully load to sql server database')
        mysql_conn.close()
        logging.info('close MYSQL connection')
        sql_server_conn.close()
        logging.info('close SQL SERVER connection')
        return True

    except Exception as e:
        logging.info('failed to load data into sql server db')
        return False
    

    
    
def send_email(status):
    gmail_cred = get_gmail_config()
    sender_email = "nitindoye470@gmail.com"
    receiver_email = "nitindoye@outlook.com"
    password = f"{gmail_cred['apppass']}"

    subject = "Data Migration Status"

    
    mysql_db = "hdfc_bank.hdfc_customer"
    sql_server_db = "HR.hdfc_cust_tgt"

    # Email content
    if status:
        body = f"Hi team,\n\n{mysql_db} from MySQL DB is successfully loaded to {sql_server_db} in SQL Server DB.\n\nBest regards,\nNitin"
        logging.info("format data migration success email body")
    else:
        body = f"Hi team,\n\nThe data migration from MySQL DB {mysql_db} to SQL Server DB {sql_server_db} failed. Please check the logs.\n\nBest regards,\nNitin"
        logging.info('format data migration failure email body')


    # Create the email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connect to SMTP server and send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            logging.info("Email sent successfully.")
    except Exception as e:
        logging.info(f"Failed to send email: {e}")
    

def main():
   status = data_migration()
   send_email(status)



if __name__ == "__main__":
    main()