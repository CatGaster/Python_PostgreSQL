import psycopg2 


def create_db(conn):
    with conn.cursor() as cur:

        cur.execute("""DROP TABLE IF EXISTS phones;""")
        cur.execute("""DROP TABLE IF EXISTS clients;""")

        cur.execute("""
                    CREATE TABLE IF NOT EXISTS clients (
            client_id SERIAL PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            surname VARCHAR(50) NOT NULL,
            email VARCHAR(50) NOT NULL);
            """)
    
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS phones (
                phone_id SERIAL PRIMARY KEY,
                client_id integer REFERENCES clients(client_id),
                phone DECIMAL(10) NOT NULL);
                """)
        conn.commit()


def add_client(conn, name, surname, email, phone = None):
    cur.execute("""INSERT INTO clients (name, surname, email) VALUES (%s, %s, %s) 
                RETURNING client_id, name, surname, email""", (name, surname, email))
    return cur.fetchall()


def change_client(conn, client_id, name = None, surname = None, email = None, phones = None):
    cur.execute("""UPDATE clients SET name = %s, surname = %s, email = %s WHERE client_id = %s
                 RETURNING client_id, name, surname, email""", (name, surname, email, client_id))
    return cur.fetchall()


def delete_client(conn, client_id, name, surname, email, phone = None): 
    cur.execute("""DELETE FROM phones WHERE client_id = %s""", (client_id,))
    cur.execute("""DELETE FROM clients WHERE client_id = %s RETURNING client_id, name, surname, email""", (client_id,))
    return cur.fetchall()


def find_client(conn, name = None, surname = None, email = None, phone = None):
    cur.execute("""SELECT  name, surname, email phone FROM clients
                 LEFT JOIN phones ON clients.client_id = phones.client_id
                 WHERE name = %s OR surname = %s OR email = %s OR phone = %s""", (name, surname, email, phone))
    return cur.fetchall()

# PHONE functions

def add_phone(conn, client_id, phone):
    cur.execute("""INSERT INTO phones (client_id, phone) VALUES (%s, %s)
                 RETURNING client_id, phone""", (client_id, phone))
    return cur.fetchall()

    
def delete_phone(conn, client_id, phone):
    cur.execute("""DELETE FROM phones WHERE client_id = %s AND phone = %s""",
                 (client_id, phone))
    cur.execute("""SELECT * FROM phones""", (client_id, phone))
    return cur.fetchall()

conn = psycopg2.connect(database = "Netology_DB",   user = "postgres", password = "", host = "localhost", port = "5432")  

create_db(conn)
with conn.cursor() as cur:
    # Добавляем пользователей
    print(add_client(cur, "Anatoly", "Ivanov", "anatoly@ya.ru"))
    print(add_client(cur, "Petr", "Petrov", "petr@ya.ru"))
    print(add_client(cur, "Sergey", "Sergeev", "sergey@ya.ru"))
    # Добавляем Номера телефона
    print(add_phone(cur, 1, '7234567890'))
    print(add_phone(cur, 2, '7234567891'))
    print(add_phone(cur, 3, '7234567892'))
    print(add_phone(cur, 1, '7234567895'))
    # Изменяем данные пользователей
    print(change_client(cur, 3, name = "Boris", surname = "Sergeev", email = "borisy@ya.ru"))
    # Удаляем данные пользователей
    print(delete_phone(cur, 2, '7234567891'))
    print(delete_client(cur, "1", "Anatoly", "Ivanov", "anatoly@ya.ru"))
    # Поиск пользователей
    print(find_client(cur, "Sergey", "Sergeev", "sergey@ya.ru"))
    conn.commit()

conn.close()

    

