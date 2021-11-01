import prestodb
import pickle

def create_table_dict():
    connection = prestodb.dbapi.connect(
        host='presto.apps.eu.idealo.com',
        port=443, user='YOUR.USERNAME.HERE',
        catalog='hive',
        http_scheme='https',
        schema='default')

    # create the cursor
    cur = connection.cursor()

    # fetch the table names of scheme dl_dwh_prod
    cur.execute(''' SHOW TABLES from "dl_dwh_prod" ''')
    records = cur.fetchall()

    tables = []

    for i in records:
        tables += i

    tables_dict = {}
    for table in tables:
        columns_name = []
        cur.execute('''
            SHOW COLUMNS FROM "dl_dwh_prod".
            ''' + table)
        records = cur.fetchall()
        for i in records:
            columns_name.append(i[0])
        tables_dict[table] = columns_name
    for key, value in tables_dict.items():
        print(key, ' : ', value)
    return tables_dict

if __name__ == '__main__':
    content = create_table_dict()
    file = open('table_dict', 'wb')
    pickle.dump(content, file)
    file.close()