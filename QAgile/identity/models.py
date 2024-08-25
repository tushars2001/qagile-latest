from django.db import models
from django.db import connection
from ..admin.models import dict_fetchall, isfloat
from datetime import datetime, timedelta


def log_user_out_qagile(user):
    sql = "update qagile_sessions set login_ts = '1900-01-01' where username = %(username)s"
    print(sql)
    with connection.cursor() as cursor:
        cursor.execute(sql, {'username': user.username})
        cursor.close()

    return True


def log_user_in_qagile(user):
    sql = "insert into qagile_sessions (username) values (%(username)s)"
    last_id = "SELECT LAST_INSERT_ID()"

    print(sql)
    with connection.cursor() as cursor:
        cursor.execute(sql, {'username': user.username})
        cursor.execute(last_id)
        cursor.close()
    print("!!!! LAST INSERT ID !!!!")
    print(data[0][0])
    return data[0][0]


def check_login(user):
    sql = "select * from qagile_sessions where username = %(username)s and login_ts > '1900-01-01' order by login_ts " \
          "desc "
    print(sql)
    with connection.cursor() as cursor:
        cursor.execute(sql, {'username': user.username})
        data = dict_fetchall(cursor)
        cursor.close()

    now = datetime.now()

    if data[0]['login_ts'] + timeout > now:
        log_user_out_qagile(user)
        return False
    else:
        update_login_ts(user)
        return True


def update_login_ts(user):
    sql = "update qagile_sessions set last_active = current_timestamp where username = %(username)s and sessionid=%(" \
          "sessionid)s "
    print(sql)
    with connection.cursor() as cursor:
        cursor.execute(sql, {'username': user.username, 'sessionid': user.sessionid})
        cursor.close()

    return True
