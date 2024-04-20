import json
import pypyodbc as odbc

DRIVER_NAME = 'SQL SERVER'
SERVER_NAME = r'SLACKY\SQLEXPRESS'
DATABASE_NAME = 'users'
TABLE_NAME = 'ETL'

connection_string = 'DRIVER={%s};SERVER=%s;DATABASE=%s;Trusted_Connection=yes' % (DRIVER_NAME, SERVER_NAME, DATABASE_NAME)

def print_table_data(cursor):

    try:
      
        cursor.execute("SELECT * FROM %s" % TABLE_NAME)
        rows = cursor.fetchall()

        for row in rows:
            print(row)


    except Exception as e:
        print("Error:", e)

def create_table(cursor):

    cursor.execute("""
        CREATE TABLE %s (
            id INT PRIMARY KEY,
            name NVARCHAR(255),
            age INT,
            city NVARCHAR(255),
            name_length INT
        )
    """ % TABLE_NAME)

def transform_data(data):

    for item in data:
        item['name_length'] = len(item['name'])
    return data

def insert_data(cursor, data):

    for item in data:
        cursor.execute("INSERT INTO %s VALUES (?, ?, ?, ?, ?)" % TABLE_NAME, (item['id'], item['name'], item['age'], item['city'], item['name_length']))

try:
    connection = odbc.connect(connection_string)
    cursor = connection.cursor()

    with open('users.json', 'r') as file:
        json_data = json.load(file)

    cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", (TABLE_NAME,))
    if not cursor.fetchone():
        create_table(cursor)

    transformed_data = transform_data(json_data)

    insert_data(cursor, transformed_data)

    connection.commit()

    print("Data inserted successfully.")
    print_table_data(cursor)

    connection.close()

except Exception as e:
    print("Error:", e)
