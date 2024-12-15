import sqlalchemy as sal
import pymysql
import pyodbc
import pandas as pd
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
logging.basicConfig(filename='server.log',level=logging.INFO, filemode='w',
                    format='%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    datefmt='%d:%b:%y %H:%M:%S')




def migrate_data():
    try:
        with open('config_files\mysql_config.json') as fp:
            cred = json.load(fp)

        mysql_engine = sal.create_engine(f"mysql+pymysql://{cred['username']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['database']}")
        mysql_conn = mysql_engine.connect()
        logging.info("successfully connect to MYSQL database")
        
        
        with open('config_files\sql_server_configs.json') as s_fp:
            sql_cred = json.load(s_fp)
        
        sql_server_engine = sal.create_engine(f"mssql+pyodbc://{sql_cred['host']}/{sql_cred['database']}?driver={sql_cred['driver'].replace(' ', '+')}")
        sql_server_conn= sql_server_engine.connect()
        logging.info('successfully connect to SQL Server database')


        query = "SELECT * FROM hdfc_customer"
        df = pd.read_sql(query, mysql_conn)
        logging.info("read data from MYSQL database")
        df.to_sql('hdfc_cust_tgt', sql_server_conn, if_exists='replace', index=False)
        logging.info('load data into SQL SERVER database')

        mysql_conn.close()
        logging.info('close MYSQL connection')
        sql_server_conn.close()
        logging.info('close SQL SERVER connection')
        return True
    except Exception as e:
        logging.error(f"Error: {e}")
        return False
    
# send email 
def send_email(status):
    with open('config_files\gmail_configs.json') as email_fp:
        gmail_cred = json.load(email_fp)
        
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
        logging.error(f"Failed to send email: {e}")


def main():
    status = migrate_data()
    send_email(status)

if __name__ == "__main__":
    main()
