import pdb

from django.db import models
from django.db import connection

# Create your models here.


def dict_fetchall(cursor):
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def profile(vnid):
    data = []

    return data


def update_capacity(vnid, fields):
    keys = list(fields.keys())

    for key in keys:
        if 'week_' in key:
            key_split = key.split("_")
            person_id = vnid
            week = key_split[1]
            hrs = float(fields[key]) if len(fields[key]) else None
            # hrs = 0 if hrs is None else hrs
            notes = fields['notes_' + week]
            delsql = "delete from qagile_capacity where vnid = %(vnid)s and week = %(week)s"
            sqlin = """
                        insert into qagile_capacity (vnid, week, hrs, notes) values(
                        %(vnid)s,
                        %(week)s,
                        %(hrs)s,
                        %(notes)s
                        )
                """
            with connection.cursor() as cursor:
                cursor.execute(delsql, {'vnid': vnid, 'week': week})
                if hrs is not None:
                    cursor.execute(sqlin, {'vnid': vnid, 'week': week, 'hrs': hrs, 'notes': notes})

    return True


def get_capacity(vnid, week_start=None, week_end=None):
    data = []
    condition = " "
    if week_start:
        condition = condition + " and week >= %(week_start)s "

    if week_end:
        condition = condition + " and week <= %(week_end)s "
    sql = "select vnid, DATE_FORMAT(week, '%%Y-%%m-%%d') as week, hrs, notes from qagile_capacity where vnid = %(vnid)s " + condition

    with connection.cursor() as cursor:
        cursor.execute(sql, {'vnid': vnid, 'week_start': week_start, 'week_end': week_end})
        data = dict_fetchall(cursor)
    return data


def has_access(vnid, access):
    sql = "select ifnull(" + access + ", 0) as access from qagile_person_accesses where vnid = %(vnid)s"
    data = []
    with connection.cursor() as cursor:
        cursor.execute(sql, {'vnid': vnid})
        data = dict_fetchall(cursor)

    if len(data):
        return data[0]['access']
    else:
        return 0


def get_accesses(vnid):
    sql = "select * from qagile_person_accesses where vnid = %(vnid)s limit 1"
    data = []
    with connection.cursor() as cursor:
        cursor.execute(sql, {'vnid': vnid})
        data = dict_fetchall(cursor)

    return data


def update_user_field(vnid, field, value):
    sql = "update qagile_person set " + field + " = %(value)s where vnid = %(vnid)s"
    with connection.cursor() as cursor:
        cursor.execute(sql, {'vnid': vnid, 'value': value})
        return vnid


def set_delegate(vnid, vnid_delegate, enddate):
    sql_delete = " delete from qagile_approver_delegation where vnid = %(vnid)s "
    sql = """insert into qagile_approver_delegation(vnid, vnid_delegate, enddate) 
        values (%(vnid)s, %(vnid_delegate)s, %(enddate)s)
            """
    with connection.cursor() as cursor:
        cursor.execute(sql_delete, {'vnid': vnid})
        if len(vnid_delegate):
            cursor.execute(sql, {'vnid': vnid, 'vnid_delegate': vnid_delegate, 'enddate': enddate})

    return


def get_delegated(vnid):
    data = []

    sql = "select vnid, vnid_delegate, enddate from qagile_approver_delegation where vnid = %(vnid)s"

    with connection.cursor() as cursor:
        cursor.execute(sql, {'vnid': vnid})
        data = dict_fetchall(cursor)

    return data

