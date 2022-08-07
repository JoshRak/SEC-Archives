import psycopg2

def bulk_insert(df, table, pre_sql):
    execute_sql(pre_sql)
    df.to_csv("C:\\Users\\Public\\tmp.csv", index=False)
    query = f'''
                COPY secarchives.archive.{table} 
                FROM \'C:\\Users\\Public\\tmp.csv\'
                DELIMITER \',\'
                CSV HEADER;
            '''
    execute_sql(query)


def execute_sql(query):
    conn = psycopg2.connect(
        database='secarchives', user='testuser',
        password='testusergatech', host='localhost', port='5432',
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(query)
    conn.close()

def get_default_headers():
    return {
            'User-Agent': 'JR',
            'From': 'youremail@domain.example'
            }
