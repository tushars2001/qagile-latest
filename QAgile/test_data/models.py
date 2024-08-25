import pdb

from django.db import models
from django.db import connection
from ..admin.models import dict_fetchall, isfloat
from datetime import datetime, timedelta


# Create your models here.


def jira_get_last_updated_tc_for_project(project_id):
    sql = """
        select DATE_FORMAT(last_updated_date, "%%Y-%%m-%%d %%h:%%i") as last_updated_date from (
        SELECT max(updated) as last_updated_date FROM qagile.qagile_test_cases where project_id =%(project_id)s
        ) as main
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'project_id': project_id})
        data = dict_fetchall(cursor)
        cursor.close()

    return data[0]['last_updated_date']


def jira_get_last_updated_defect_for_project(project_id):
    sql = """
        select DATE_FORMAT(last_updated_date, "%%Y-%%m-%%d %%h:%%i") as last_updated_date from (
        SELECT max(updated) as last_updated_date FROM qagile.qagile_defects where project_id =%(project_id)s
        ) as main
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'project_id': project_id})
        data = dict_fetchall(cursor)
        cursor.close()

    return data[0]['last_updated_date']


def load_tc_records(records, runid):
    # load into staging
    for record in records:
        sql = """
            INSERT INTO `qagile`.`qagile_test_cases_stage`
                (
                `tcid`,
                `source_id`,
                `title`,
                `project_id`,
                `created_by`,
                `created`,
                `updated`,
                `runid`)
                VALUES
                (
                %(tcid)s,
                %(source_id)s,
                %(title)s,
                %(project_id)s,
                %(created_by)s,
                %(created)s,
                %(updated)s,
                '""" + runid + """');

            """
        with connection.cursor() as cursor:
            cursor.execute(sql, record)
            cursor.close()

    # update what is different
    sql = """
        UPDATE qagile.qagile_test_cases as tc_dest,
        (
        SELECT tcid, source_id, title, project_id, created_by, replace(created,'T', ' ') created, replace(updated,'T', ' ') updated
         FROM qagile.qagile_test_cases_stage where runid = '""" + runid + """'
        ) as tc_src 
        set 
        tc_dest.source_id = tc_src.source_id,
        tc_dest.title = tc_src.title,
        tc_dest.project_id = tc_src.project_id,
        tc_dest.created_by = tc_src.created_by,
        tc_dest.created = tc_src.created,
        tc_dest.updated = tc_src.updated
        where tc_dest.tcid = tc_src.tcid ;
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    # insert what is not present
    sql = """
        insert into qagile.qagile_test_cases(tcid, source_id, title, project_id, created_by, created, updated) 
        SELECT tcid, source_id, title, project_id, created_by, replace(created,'T', ' '), replace(updated,'T', ' ') 
        FROM qagile.qagile_test_cases_stage where runid = '""" + runid + """'
        and tcid not in (select tcid from qagile.qagile_test_cases);
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    return runid


def load_tc_executions_records(records, runid):
    # load into staging
    for record in records:
        sql = """
            INSERT INTO `qagile`.`qagile_test_executions_stage`
                (
                `tcid`,
                `source_id`,
                `executedBy`,
                `executedOn`,
                `status`,
                `cycleName`,
                `source`,
                `runid`)
                VALUES
                (
                %(tcid)s,
                %(source_id)s,
                %(executedBy)s,
                %(executedOn)s,
                %(status)s,
                %(cycleName)s,
                %(source)s,
                '""" + runid + """');

            """
        with connection.cursor() as cursor:
            cursor.execute(sql, record)
            cursor.close()

    # update what is different
    sql = """
        UPDATE qagile.qagile_test_executions as tc_dest,
        (
        SELECT tcid, source_id, executedBy, 
        DATE_FORMAT(STR_TO_DATE(executedOn,'%m-%d-%Y %T'), '%Y-%m-%d %T') executedOn, 
        status, cycleName, source
         FROM qagile.qagile_test_executions_stage where runid = '""" + runid + """'
        ) as tc_src 
        set 
        tc_dest.source_id = tc_src.source_id,
        tc_dest.executedBy = tc_src.executedBy,
        tc_dest.executedOn = tc_src.executedOn,
        tc_dest.status = tc_src.status,
        tc_dest.cycleName = tc_src.cycleName,
        tc_dest.source = tc_src.source
        where tc_dest.tcid = tc_src.tcid ;
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    # insert what is not present
    sql = """
        insert into qagile.qagile_test_executions(tcid, source_id, executedBy, executedOn, status, cycleName, source) 
        SELECT tcid, source_id, executedBy, 
        DATE_FORMAT(STR_TO_DATE(executedOn,'%m-%d-%Y %T'), '%Y-%m-%d %T') executedOn, 
        status, cycleName, source
        FROM qagile.qagile_test_executions_stage where runid = '""" + runid + """'
        and tcid not in (select tcid from qagile.qagile_test_executions);
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    return runid


def load_log(runid, load_url, load_status, start_time, end_time, method, records_expected, records_processed, params=None):
    fields = {
        'runid': runid,
        'load_url': load_url,
        'load_status': load_status,
        'start_time': start_time,
        'end_time': end_time,
        'method': method,
        'records_expected': records_expected,
        'records_processed': records_processed,
        'params': params
    }
    sql = """
        INSERT INTO `qagile`.`qagile_load_log`
        (
        `load_url`,
        `load_status`,
        `start_time`,
        `end_time`,
        `records_expected`,
        `records_processed`,
        `runid`,
        `method`,
        `params`)
        VALUES
        (
        %(load_url)s,
        %(load_status)s,
        %(start_time)s,
        %(end_time)s,
        %(records_expected)s,
        %(records_processed)s,
        %(runid)s,
        %(method)s,
        %(params)s
        )
    """
    print(sql, fields)
    with connection.cursor() as cursor:
        cursor.execute(sql, fields)
        cursor.close()

    return True


def compress_load_log():
    sql = """
            delete from qagile.qagile_load_log where runid in 
        (
            select runid from (
            select max(rec_id), runid from qagile.qagile_load_log where runid in (
            SELECT runid FROM qagile.qagile_load_log where load_status = 'finished')
            group by runid ) main
        ) and rec_id not in 
        (
            select rec_id from (
            select max(rec_id) rec_id, runid from qagile.qagile_load_log where runid in (
            SELECT runid FROM qagile.qagile_load_log where load_status = 'finished')
            group by runid ) main
        
        )
        and rec_id != 0
        """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    return True


def get_all_projects(source):
    sql = """
            SELECT project_id FROM qagile.qagile_projects where `source`=%(source)s
        """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'source': source})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_final_log(runid):
    sql = """
        SELECT * FROM qagile.qagile_load_log where runid = %(runid)s order by rec_id desc limit 1;
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'runid': runid})
        data = dict_fetchall(cursor)
        cursor.close()

    return data[0]


def load_projects(records, runid):
    # load into staging
    for record in records:
        sql = """
            INSERT INTO `qagile`.`qagile_projects_stage`
            (
            `project_id`,
            `title`,
            `source`,
            `source_id`,
            runid
            )
            VALUES
            (
            %(project_id)s,
            %(title)s,
            %(source)s,
            %(source_id)s,
            '""" + runid + """'
            );

            """
        with connection.cursor() as cursor:
            cursor.execute(sql, record)
            cursor.close()

    # insert what is not present
    sql = """
        insert into qagile.qagile_projects(project_id, source_id, title, source) 
        SELECT project_id, source_id, title, source 
        FROM qagile.qagile_projects_stage where runid = '""" + runid + """'
        and project_id not in (select project_id from qagile.qagile_projects);
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    return runid


def get_project_name_by_id(project_id):
    sql = """
                SELECT title, count(*) as cnt FROM qagile.qagile_projects where `project_id`=%(project_id)s
            """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'project_id': project_id})
        data = dict_fetchall(cursor)
        cursor.close()
    return data[0]['title']


def load_defect_records(records, runid):
    # load into staging
    for record in records:
        sql = """
            INSERT INTO `qagile`.`qagile_defects_stage`
                (
                `defectid`,
                `source_id`,
                `title`,
                `project_id`,
                `created_by`,
                `created`,
                `updated`,
                `status`,
                `assignee`,
                `sev`,
                `environment`,
                `runid`)
                VALUES
                (
                %(defectid)s,
                %(source_id)s,
                %(title)s,
                %(project_id)s,
                %(created_by)s,
                %(created)s,
                %(updated)s,
                %(status)s,
                %(assignee)s,
                %(sev)s,
                'TBU',
                '""" + runid + """');

            """
        with connection.cursor() as cursor:
            try:
                cursor.execute(sql, record)
            except Exception as e:
                print(e)
            cursor.close()

    # update what is different
    sql = """
        UPDATE qagile.qagile_defects as tc_dest,
        (
        SELECT defectid, source_id, title, project_id, created_by, replace(created,'T', ' ') created, 
        replace(updated,'T', ' ') updated, `status`, `assignee`, `sev`,   `environment`
         FROM qagile.qagile_defects_stage where runid = '""" + runid + """'
        ) as tc_src 
        set 
        tc_dest.source_id = tc_src.source_id,
        tc_dest.title = tc_src.title,
        tc_dest.project_id = tc_src.project_id,
        tc_dest.created_by = tc_src.created_by,
        tc_dest.created = tc_src.created,
        tc_dest.updated = tc_src.updated,
        tc_dest.status = tc_src.status,
        tc_dest.assignee = tc_src.assignee,
        tc_dest.sev = tc_src.sev,
        tc_dest.environment = tc_src.environment
        where tc_dest.defectid = tc_src.defectid ;
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    # insert what is not present
    sql = """
        insert into qagile.qagile_defects(defectid, source_id, title, project_id, created_by, created, updated,
        `status`, `assignee`, `sev`,  `environment`) 
        SELECT defectid, source_id, title, project_id, created_by, replace(created,'T', ' '), replace(updated,'T', ' '),
        `status`, `assignee`, `sev`,  `environment`
        FROM qagile.qagile_defects_stage where runid = '""" + runid + """'
        and defectid not in (select defectid from qagile.qagile_defects);
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    return runid


def get_job_status_by_runid(runid):
    sql = """
                    SELECT title, count(*) as cnt FROM qagile.qagile_projects where `project_id`=%(project_id)s
                """
    print(sql, runid)
    with connection.cursor() as cursor:
        cursor.execute(sql, {'project_id': project_id})
        data = dict_fetchall(cursor)
        cursor.close()
    return data[0]['title']