from django.db import models
from django.db import connection

# Create your models here.


def dict_fetchall(cursor):
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def get_messages():
    data = []
    sql = "select * from qagile_home_messages"
    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def set_messages(bruce, sreeman):
    data = []
    fields = {'bruce': bruce, 'sreeman': sreeman}

    sql = "insert into qagile_home_messages (bruce, sreeman) values (%(bruce)s, %(sreeman)s)"
    with connection.cursor() as cursor:
        cursor.execute("delete from qagile_home_messages")
        cursor.execute(sql, fields)
        cursor.close()

    data = get_messages()

    return data
