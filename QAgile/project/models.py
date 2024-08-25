from django.db import models
from django.db import connection
from ..admin.models import dict_fetchall, isfloat
# Create your models here.


def create(fields):
    record = {
        'ppmid': fields['ppmid'],
        'parent_ppmid': fields['parent_ppmid'],
        'domain_id': fields['domain_id'],
        'name': fields['name'],
        'source': fields['source'],
        'source_id': fields['source_id']

    }

    if record['ppmid'] == record['parent_ppmid']:
        sql = "SELECT count(*)+1 as nextid FROM qagile.qagile_rbs_projects where project_id like '"+record['ppmid']+"%'"
        with connection.cursor() as cursor:
            cursor.execute(sql)
            data = dict_fetchall(cursor)
            cursor.close()
        record['ppmid'] = record['ppmid'] + str(data[0]['nextid'])

    sql = """
        INSERT INTO `qagile`.`qagile_rbs_projects`
        (
        `ppmid`,
        `parent_ppmid`,
        `domain_id`,
        `name`,
        `source`,
        `source_id`,
        `updated`)
        VALUES
        (
        %(ppmid)s,
        %(parent_ppmid)s,
        %(domain_id)s,
        %(name)s,
        %(source)s,
        %(source_id)s,
        current_timestamp)
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor, record)
        cursor.close()

    return record['ppmid']
