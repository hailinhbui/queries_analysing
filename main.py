import pandas as pd
import nltk
import re
nltk.download('punkt')
from nltk.tokenize import word_tokenize
from collections import Counter
import prestodb
import pickle

def get_queries():
    conn = prestodb.dbapi.connect(
        host='presto.apps.eu.idealo.com',
        port=443, user='YOUR_USERNAME_HERE',
        catalog='hive',
        http_scheme='https',
        schema='default',
    )
    # create the cursor
    cursor = conn.cursor()
    cursor.execute(''' 
    select * from prod_dl_presto_query_events_prod.dl_presto_events_prod_758373708967_eu_central_1
      where 1=1
        and date(partitioned_date) > date('2021-06-01')
      and querystate = 'FINISHED'
      -- Remove the system queries from the stats
      and lower(query) not like '%from system.%'
      -- Helps ignore the prepared statements (e.g. EXECUTE, ALLOCATE, DEALLOCATE, etc)
      and regexp_like(lower(query), '^(with|select)')
      and lower(query) like '%isg%'
    limit 10000
    ''')
    records = cursor.fetchall()
    colnames = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=colnames)
    queries = df["query"].values.tolist()
    return queries

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

def sorting_occurrences(counter_dict, table_columns):
    # Using dict()
    # Extracting specific keys from dictionary: selecting the values of fields in leadouts_fields
    occurrences = dict((k, counter_dict[k]) for k in table_columns if k in counter_dict)
    sorted_keys = sorted(occurrences, key=occurrences.get)
    sorted_occurrences = {}
    for w in sorted_keys:
        sorted_occurrences[w] = occurrences[w]
    return sorted_occurrences

if __name__ == '__main__':
    table_name = 'isg'
    infile = open('table_dict', 'rb')
    table_dict = pickle.load(infile, encoding='latin1')
    table_columns = table_dict[table_name]
    infile.close()
    # convert queries into a list
    queries = get_queries()
    # a dict of all tokens was found in the queries
    counter_dict = cleanQueries(queries)
    # a sorted dict of how often the column names happen in the query
    sorted_occurrences = sorting_occurrences(counter_dict, table_columns)
    for key, value in sorted_occurrences.items():
        print(key, ' : ', value)