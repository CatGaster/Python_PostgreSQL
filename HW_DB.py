import psycopg2
from psycopg2.sql import SQL, Identifier

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


def change_client(conn, client_id, name = None, surname = None, email=None):
    arg_list = {'name': name, 'surname': surname, 'email': email}
    for key, arg in arg_list.items():
        if arg:
            cur.execute(SQL('UPDATE clients SET {}=%s WHERE client_id = %s').format(Identifier(key)), (arg,client_id))
    cur.execute("""
        SELECT * FROM clients
        WHERE client_id = %s;
        """, client_id)
    return cur.fetchall()


def delete_client(conn, client_id, name, surname, email, phone = None): 
    cur.execute("""DELETE FROM phones WHERE client_id = %s""", (client_id,))
    cur.execute("""DELETE FROM clients WHERE client_id = %s RETURNING client_id, name, surname, email""", (client_id,))
    return cur.fetchall()


def find_client(conn, name = "%", surname = "%", email = "%", phone = None):
    cur.execute("""SELECT  name, surname, email, phone FROM clients
                inner JOIN phones ON clients.client_id = phones.client_id
                WHERE name = %s AND surname = %s AND email = %s OR phone = %s""", (name, surname, email, phone))
    return cur.fetchall()

# PHONE functions

def add_phone(conn, client_id, phone):
    cur.execute("""INSERT INTO phones (client_id, phone) VALUES (%s, %s)
                 RETURNING client_id, phone""", (client_id, phone))
    return cur.fetchall()


def change_phone(conn, phone_id, phone):
    cur.execute("""UPDATE phones SET phone = %s WHERE phone_id = %s
                 RETURNING client_id, phone_id, phone""", (phone, phone_id))
    return cur.fetchall()

    
def delete_phone(conn, client_id, phone):
    cur.execute("""DELETE FROM phones WHERE client_id = %s AND phone = %s""",
                 (client_id, phone))
    cur.execute("""SELECT * FROM phones""", (client_id, phone))
    return cur.fetchall()

if __name__ == '__main__':
    with psycopg2.connect(database = "Netology_DB",   user = "postgres", password = "", host = "localhost", port = "5432") as conn:
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
                print(change_phone(cur, phone_id = 4, phone = '7234567899'))
                print(change_client(cur, client_id = '3', name = "Boris", surname = "Sergeev", email = "borisy@ya.ru"))
                # # Удаляем данные пользователей
                print(delete_phone(cur, 2, '7234567891'))
                print(delete_client(cur, "1", "Anatoly", "Ivanov", "anatoly@ya.ru"))
                # Поиск пользователей
                print(find_client(cur, name = "Boris", surname = "Sergeev", email = "borisy@ya.ru"))
                


    

