import psycopg2 
import csv 
 
def execute_sql_file(cursor, file_path): 
    with open(file_path, 'r') as file: 
        sql_script = file.read() 
        cursor.execute(sql_script) 
 
def insert_data_from_csv(cursor, table_name, csv_path, columns): 
    with open(csv_path, 'r') as file: 
        csv_reader = list(csv.reader(file))[1:] 
        placeholders = ', '.join(['%s'] * len(columns)) 
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})" 
        cursor.executemany(query, csv_reader) 
 
def main(): 
    host = "postgres" 
    database = "postgres" 
    user = "postgres" 
    pas = "postgres" 
     
    # Підключення до бази даних 
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas) 
     
    # Створення нових таблиць 
    cursor = conn.cursor() 
 
    # Використання функції для виконання SQL-скриптів з файлів 
    execute_sql_file(cursor, 'sql/accounts.sql') 
    execute_sql_file(cursor, 'sql/products.sql') 
    execute_sql_file(cursor, 'sql/transactions.sql') 
 
    # Додавання даних в таблиці 
    insert_data_from_csv(cursor, 'accounts', 'data/accounts.csv', 
                         ['account_id', 'first_name', 'last_name', 'address_1', 'address_2', 'city', 'state', 'zip_code', 'join_date']) 
     
    insert_data_from_csv(cursor, 'products', 'data/products.csv', 
                         ['product_id', 'product_code', 'product_description']) 
     
    with open('data/transactions.csv', 'r') as file: 
        csv_reader = list(csv.reader(file))[1:] 
        mydata = [[row[0], row[1], row[2], row[6], row[5]] for row in csv_reader] 
        cursor.executemany(''' 
            INSERT INTO transactions  
                (transaction_id,transaction_date,product_id,account_id,quantity)  
            VALUES (%s, %s, %s, %s, %s)''', mydata) 
 
    # Виведення в консоль вибраних даних 
    tables = ['accounts', 'products', 'transactions'] 
    for table in tables: 
        print(f"---------{table}---------") 
        cursor.execute(f'SELECT * FROM {table}') 
        print(cursor.fetchall()) 
 
    # Завершення транзакції і закриття підключення 
    conn.commit() 
    cursor.close() 
    conn.close() 
    print("Script finished successfully") 
 
if __name__ == "__main__": 
    main()