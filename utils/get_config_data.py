import json


def get_mysql_config_db():
    try:
        with open('config_files\mysql_config.json') as fp:
            mysql_cred = json.load(fp)
        
    except Exception as e:
        print(e)
    
    return mysql_cred
    

def get_sql_server_config_db():
    try:
        with open('config_files\sql_server_configs.json') as fp:
            sql_server_cred = json.load(fp)
        
    except Exception as e:
        print(e)

    return sql_server_cred


def get_gmail_config():
    try:
        with open('config_files\gmail_configs.json') as fp:
            gmail_cred = json.load(fp)
        
    except Exception as e:
        print(e)
    
    return gmail_cred

