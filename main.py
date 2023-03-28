import psycopg2


def create_db(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS persons(
            id SERIAL PRIMARY KEY,
            name VARCHAR(30) NOT NULL,
            surname VARCHAR(30) NOT NULL,
            email VARCHAR(40) NOT NULL);
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS phones(
            id SERIAL PRIMARY KEY,
            id_person INTEGER NOT NULL REFERENCES persons(id),
            number VARCHAR(12) NOT NULL);
    """)

def find_phone(cur, id_person, number):
    cur.execute("""
        SELECT number FROM phones
        WHERE id_person=%s AND number=%s;
    """, (str(id_person), str(number)))
    cur.fetchall()


def add_phone(conn, cur, id_person, number):
    found_number = find_phone(cur, id_person, number)
    if found_number is None or len(found_number) == 0:
        cur.execute("""
            INSERT INTO phones(id_person, number) VALUES(%s, %s);
        """, (id_person, number))
        conn.commit()


def add_person(conn, cur, name, surname, email, number=None):
    if name == None or surname == None or email == None:
        print('Введены не все данные!')
        return
    cur.execute("""
        INSERT INTO persons(name, surname, email) VALUES(%s, %s, %s);
    """, (name, surname, email))
    if number is not None:
        cur.execute("""
            SELECT id FROM persons
        """)
        id_person = cur.fetchall()[0][-1]
        add_phone(conn, cur, id_person, number)

    conn.commit()


def change_person(conn, cur, id_person, name=None, surname=None, email=None, number=None):
    if name is not None:
        cur.execute("""
            UPDATE persons SET name=%s
            WHERE id=%s;
        """, (name, id_person))
    if surname is not None:
        cur.execute("""
            UPDATE persons SET surname=%s
            WHERE id=%s;
        """, (surname, id_person))
    if email is not None:
        cur.execute("""
            UPDATE persons SET email=%s
            WHERE id=%s;
        """, (email, id_person))
    if number is not None:
        add_phone(conn, cur, id_person, number)

    conn.commit()


def delete_phone(conn, cur, id_person, number):
    cur.execute("""
        DELETE FROM phones
        WHERE id_person=%s AND number=%s;
    """, (id_person, number))
    conn.commit()

def delete_person(conn, cur, id_person):
    cur.execute("""
        DELETE FROM phones
        WHERE id_person=%s;
    """, (id_person, ))
    cur.execute("""
        DELETE FROM persons
        WHERE id=%s;
    """, (id_person, ))
    conn.commit()


def find_person(cur, name=None, email=None, number=None):
    if name is not None:
        firstname, surname = name.split()

    if number is not None:
        cur.execute("""
            SELECT persons.id FROM persons
            JOIN phones ON phones.id_person = persons.id
            WHERE phones.number=%s;
        """, (number, ))
    elif email is not None:
        cur.execute("""
            SELECT id FROM persons
            WHERE email=%s;
        """, (email, ))
    elif name is not None:
        cur.execute("""
            SELECT id FROM persons
            WHERE name=%s AND surname=%s;
        """, (firstname, surname))

    res = cur.fetchall()
    if res:
        return list(res[0])
    else:
        return None


if __name__ == '__main__':
    conn = psycopg2.connect(database="DB_in_Python", user="postgres", password="Kempachi")
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE phones;
            DROP TABLE persons;
        """)

        create_db(conn, cur)
        print('База данных создана!')

        add_person(conn, cur, 'Евгений', 'Сиротин', 'sirotin@yandex.ru', 89197777777)
        add_person(conn, cur, 'Михаил', 'Печкин', 'pechkin@mail.ru', 89085555555)
        add_person(conn, cur, 'Александр', 'Дедков', 'dedal@rambler.ru')
        add_person(conn, cur, 'Денис', 'Бабушкин', 'babuden@google.ru', 89513429854)

        print('Результат заполнения: ')
        cur.execute("""
            SELECT persons.name, persons.surname, persons.email, phones.number FROM persons
            LEFT JOIN phones ON persons.id = phones.id_person
            ORDER BY persons.id;
        """)
        print(cur.fetchall())

        add_phone(conn, cur, 3, 89022394834)
        add_phone(conn, cur, 2, 89560268693)
        print('Результат изменения: ')
        cur.execute("""
            SELECT persons.name, persons.surname, persons.email, phones.number FROM persons
            LEFT JOIN phones ON persons.id = phones.id_person
            ORDER BY persons.id;
        """)
        print(cur.fetchall())

        change_person(conn, cur, 1, 'Артур', None, '16artur61@outlook.com', None)
        delete_phone(conn, cur, 2,'89085555555')
        print('Результат изменения: ')
        cur.execute("""
            SELECT persons.name, persons.surname, persons.email, phones.number FROM persons
            LEFT JOIN phones ON persons.id = phones.id_person
            ORDER BY persons.id;
        """)
        print(cur.fetchall())

        delete_person(conn, cur, 4)
        print('Результат изменения: ')
        cur.execute("""
            SELECT persons.name, persons.surname, persons.email, phones.number FROM persons
            LEFT JOIN phones ON persons.id = phones.id_person
            ORDER BY persons.id;
        """)
        print(cur.fetchall())

        print(find_person(cur, name='Денис Бабушкин'))
        print(find_person(cur, email='dedal@rambler.ru'))
        print(find_person(cur, name='Александр Дедков', email='dedal@rambler.ru', number='89022394834'))
    conn.close()
