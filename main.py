import pandas as pd
import nltk
import re
nltk.download('punkt')
from nltk.tokenize import word_tokenize
from collections import Counter
import prestodb

def connect_to_presto():
    conn = prestodb.dbapi.connect(
        host='presto.apps.eu.idealo.com',
        port=443, user='YOUR_USERNAME_HERE',
        catalog='hive',
        http_scheme='https',
        schema='default',
    )
    # create the cursor
    cursor = conn.cursor()
    return cursor

def get_queries(cur):
    cur.execute(''' 
    select * from prod_dl_presto_query_events_prod.dl_presto_events_prod_758373708967_eu_central_1
      where 1=1
        and date(partitioned_date) > date('2021-06-01')
      and querystate = 'FINISHED'
      -- Remove the system queries from the stats
      and lower(query) not like '%from system.%'
      -- Helps ignore the prepared statements (e.g. EXECUTE, ALLOCATE, DEALLOCATE, etc)
      and regexp_like(lower(query), '^(with|select)')
      and lower(query) like '%fact_leadouts%'
    limit 10000
    ''')
    records = cur.fetchall()
    colnames = [col[0] for col in cur.description]
    df = pd.DataFrame(records, columns=colnames)
    return df

# a list of sql operation that should be removed
sql_syntax = ['null', 'else', 'or', 'select', 'from', 'where', 'case', 'and', 'join', 'left', 'with', 'between', 'then',
              'true', 'false', 'in', 'as', 'sum', 'avg', 'count', 'end', 'limit', 'on', 'when', 'then']

def cleanQueries(queries):
    cleaned_lists = []
    merged_list = []
    # a list of a list of cleaned tokens from queries
    for query in queries:
        # remove special characters
        cleaned_text = re.sub('<[^<%*,=)(-/]+>?', '', query)
        # remove numbers
        text_nonumbers = ''.join(c for c in cleaned_text if not c.isdigit())
        # convert string lowercase
        text_lower = text_nonumbers.lower()
        # tokenziation a string into a list of tokens
        tokens = word_tokenize(text_lower)
        # remove sql syntax keywords
        for i in sql_syntax:
            try:
                tokens.remove(i)
            except ValueError:
                pass
        cleaned_lists.append(tokens)
    for list in cleaned_lists:
        merged_list += list
    # Count how often a token was found in the list
    counter = Counter(merged_list)
    counter_dict = dict(counter)  # convert Counter to Dict
    return counter_dict

def sorting_occurrencies(counter_dict, table_columns):
    # Using dict()
    # Extracting specific keys from dictionary: selecting the values of fields in leadouts_fields
    occurencies = dict((k, counter_dict[k]) for k in table_columns if k in counter_dict)
    sorted_keys = sorted(occurencies, key=occurencies.get)
    sorted_occurencies = {}
    for w in sorted_keys:
        sorted_occurencies[w] = occurencies[w]
    return sorted_occurencies


def get_table_colums(cur, table_name):
    cur.execute('''
    SHOW COLUMNS FROM "dl_dwh_prod".
    ''' + table_name)
    records = cur.fetchall()
    columns_name = []
    for i in records:
        columns_name.append(i[0])
    return columns_name


if __name__ == '__main__':
    cursor = connect_to_presto()
    query_records = get_queries(cursor)
    table_name = 'fact_leadouts'
    table_columns = get_table_colums(cursor,table_name)
    # convert the field query into a list
    queries = query_records["query"].values.tolist()
    counter_dict = cleanQueries(queries)
    #showing the unused fields in the table - which are normally empty
    #get_unused_fields = set(leadouts_fields) - set(merged_list)
    # print(get_unused_fields)
    sorted_occurencies = sorting_occurrencies(counter_dict, table_columns)
    for key, value in sorted_occurencies.items():
        print(key, ' : ', value)