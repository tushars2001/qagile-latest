import pdb

from django.db import connection
from ..admin.models import dict_fetchall


# Create your models here.


def jira_get_last_updated_tc_for_project(project_id):
    sql = """
        select DATE_FORMAT(last_updated_date, "%%Y-%%m-%%d %%H:%%i") as last_updated_date from (
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
        select DATE_FORMAT(last_updated_date, "%%Y-%%m-%%d %%H:%%i") as last_updated_date from (
        SELECT max(updated) as last_updated_date FROM qagile.qagile_defects where project_id =%(project_id)s
        ) as main
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'project_id': project_id})
        data = dict_fetchall(cursor)
        cursor.close()

    return data[0]['last_updated_date']


def jira_get_last_updated_execution_for_project(project_id):
    sql = """
        select DATE_FORMAT(last_updated_date, "%%Y-%%m-%%d %%H:%%i:%%s") as last_updated_date from (
        SELECT max(executedOn) as last_updated_date FROM qagile.qagile_test_executions where project_id =%(project_id)s
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
                `runid`,
                `project_id`,
                `assignedTo`,
                `cycleId`,
                `versionId`)
                VALUES
                (
                %(tcid)s,
                %(source_id)s,
                %(executedBy)s,
                %(executedOn)s,
                %(status)s,
                %(cycleName)s,
                %(source)s,
                '""" + runid + """',
                %(project_id)s,
                %(assignedTo)s,
                %(cycleId)s,
                %(versionId)s);

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
        status, cycleName, source, assignedTo, project_id, cycleId, versionId
         FROM qagile.qagile_test_executions_stage where runid = '""" + runid + """'
        ) as tc_src 
        set 
        tc_dest.source_id = tc_src.source_id,
        tc_dest.executedBy = tc_src.executedBy,
        tc_dest.executedOn = tc_src.executedOn,
        tc_dest.status = tc_src.status,
        tc_dest.cycleName = tc_src.cycleName,
        tc_dest.cycleId = tc_src.cycleId,
        tc_dest.versionId = tc_src.versionId,
        tc_dest.source = tc_src.source,
        tc_dest.assignedTo = tc_src.assignedTo,
        tc_dest.project_id = tc_src.project_id
        where tc_dest.source_id = tc_src.source_id ;
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()
    # insert what is not present
    sql = """
        insert into qagile.qagile_test_executions(tcid, source_id, executedBy, executedOn, status, cycleName, source,
        project_id, assignedTo, cycleId, versionId) 
         SELECT s.tcid, s.source_id, s.executedBy, 
        DATE_FORMAT(STR_TO_DATE(s.executedOn,'%m-%d-%Y %T'), '%Y-%m-%d %T') executedOn, 
        s.status, s.cycleName, s.source, s.project_id, s.assignedTo, s.cycleId, s.versionId
        FROM qagile.qagile_test_executions_stage s left join qagile.qagile_test_executions e using(source_id)
         where s.runid = '""" + runid + """'
        and  e.source_id is null
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    sql = """
           select count(*) as cnt from qagile_test_executions
        """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        print("total records in executions: " + str(data[0]['cnt']))
        cursor.close()
    return runid


def load_log(runid, load_url, load_status, start_time, end_time, method, records_expected, records_processed, params,
             pid, load_type, project_id, comments):
    fields = {
        'runid': runid,
        'load_url': load_url,
        'load_status': load_status,
        'start_time': start_time,
        'end_time': end_time,
        'method': method,
        'records_expected': records_expected,
        'records_processed': records_processed,
        'params': params,
        'pid': pid,
        'load_type': load_type,
        'project_id': project_id,
        'comments': comments
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
        `params`,
        `pid`,
        `load_type`,
        `project_id`,
        `comments`)
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
        %(params)s,
        %(pid)s,
        %(load_type)s,
        %(project_id)s,
        %(comments)s
        )
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, fields)
        cursor.close()

    return True


def compress_load_log():
    return True
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
            SELECT project_id FROM qagile.qagile_projects where `source`=%(source)s and track = 1 order by project_id
        """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'source': source})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_all_projects_with_no_exec_data(source):
    sql = """
            SELECT project_id FROM qagile.qagile_projects where `source`=%(source)s and track = 1 
            and project_id not in (
                    select project_id from qagile_test_executions group by project_id having count(*)>0
                )
            order by project_id
        """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'source': source})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_all_projects_with_exec_data(source):
    sql = """
                SELECT project_id FROM qagile.qagile_projects where `source`=%(source)s and track = 1 
                and project_id in (
                        select project_id from qagile_test_executions group by project_id having count(*)>0
                    )
                order by project_id
            """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'source': source})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_all_projects_with_no_defects_data(source):
    sql = """
            SELECT project_id FROM qagile.qagile_projects where `source`=%(source)s and track = 1 
            and project_id not in (
                    select project_id from qagile_defects group by project_id having count(*)>0
                )
            order by project_id
        """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'source': source})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_all_projects_with_defects_data(source):
    sql = """
                SELECT project_id FROM qagile.qagile_projects where `source`=%(source)s and track = 1 
                and project_id in (
                        select project_id from qagile_defects group by project_id having count(*)>0
                    )
                order by project_id
            """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'source': source})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_all_projects_with_no_tcs_data(source):
    sql = """
            SELECT project_id FROM qagile.qagile_projects where `source`=%(source)s and track = 1 
            and project_id not in (
                    select project_id from qagile_test_cases group by project_id having count(*)>0
                )
            order by project_id
        """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'source': source})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_all_projects_with_tcs_data(source):
    sql = """
                SELECT project_id FROM qagile.qagile_projects where `source`=%(source)s and track = 1 
                and project_id in (
                        select project_id from qagile_test_cases group by project_id having count(*)>0
                    )
                order by project_id
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


def load_users(records):

    for record in records:
        try:
            sql = """
                update `qagile`.`qagile_jira_users`
                set name = %(displayName)s,
                emailAddress = %(emailAddress)s
                where accountId = %(accountId)s
                """
            with connection.cursor() as cursor:
                cursor.execute(sql, record)
                cursor.close()
        except Exception as e:
            print(str(e))

    return True


def get_project_name_by_id(project_id):
    sql = """
                SELECT title, count(*) as cnt FROM qagile.qagile_projects where `project_id`=%(project_id)s
            """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'project_id': project_id})
        data = dict_fetchall(cursor)
        cursor.close()
    return data[0]['title']


def get_project_names_by_ids(project_ids_data):

    project_ids = []
    for rec in project_ids_data:
        project_ids.append(rec['project_id'])

    sql = """
                    SELECT distinct title FROM qagile.qagile_projects where `project_id` in %(project_ids)s
                """
    with connection.cursor() as cursor:
        cursor.execute(sql, {'project_ids': project_ids})
        data = dict_fetchall(cursor)
        cursor.close()
    return data


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
                pdb.set_trace()
            cursor.close()

    # update what is different
    sql = """
        UPDATE qagile.qagile_defects as tc_dest,
        (
        SELECT defectid, source_id, title, project_id, created_by, replace(created,'T', ' ') created, 
        replace(updated,'T', ' ') updated, `status`, `assignee`, `sev`,   `environment`
         FROM qagile.qagile_defects_stage where runid = '""" + str(runid) + """'
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
        FROM qagile.qagile_defects_stage where runid = '""" + str(runid) + """'
        and defectid not in (select defectid from qagile.qagile_defects);
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    return runid


def get_job_status_by_runid(runid):
    sql = """
                    SELECT * FROM qagile.qagile_load_log where `runid`=%(runid)s order by 1 desc limit 1
                """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'runid': runid})
        data = dict_fetchall(cursor)
        cursor.close()
    return data


def test_sql(runid=123):
    sql = """
            insert into qagile.qagile_defects(defectid2, source_id, title, project_id, created_by, created, updated,
            `status`, `assignee`, `sev`,  `environment`) 
            SELECT defectid, source_id, title, project_id, created_by, replace(created,'T', ' '), replace(updated,'T', ' '),
            `status`, `assignee`, `sev`,  `environment`
            FROM qagile.qagile_defects_stage where runid = '""" + str(runid) + """'
            and defectid not in (select defectid from qagile.qagile_defects);
        """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()


def log_error(log_id, id1, id2, id1_name, id2_name, log_data):
    sql = """
                INSERT INTO `qagile`.`qagile_logs`
            (
            `log_id`,
            `id1`,
            `id1_name`,
            `id2`,
            `id2_name`,
            `log_data`
            )
            VALUES
            (
            %(log_id)s,
            %(id1)s,
            %(id1_name)s,
            %(id2)s,
            %(id2_name)s,
            %(log_data)s
            );
            """

    with connection.cursor() as cursor:
        cursor.execute(sql, {
            'log_id': log_id,
            'id1': id1,
            'id1_name': id1_name,
            'id2': id2,
            'id2_name': id2_name,
            'log_data': log_data
        })
        cursor.close()


def manage_unexecuted(records, project_id, runid):
    sql_delete = "delete from qagile.qagile_test_executions where project_id = %(project_id)s and status='UNEXECUTED'"

    with connection.cursor() as cursor:
        cursor.execute(sql_delete, {
            'project_id': project_id
        })
        for i in range(records):
            sql = """
                        INSERT INTO `qagile`.`qagile_test_executions_stage`
                            (
                            `tcid`,
                            `status`,
                            `runid`,
                            `project_id`)
                            VALUES
                            (
                            'Fake-""" + str(i) + """',
                            'UNEXECUTED',
                            %(runid)s,
                            %(project_id)s
                            )
                        """
            cursor.execute(sql, {
                'project_id': project_id,
                'runid': runid
            })
            print("Faking: " + str(i))
        cursor.close()

    return True


def running_jobs(rng=(300, None)):
    duration = ''
    if rng[1]:
        duration = ' between ' + str(rng[0]) + ' AND ' + str(rng[1])
    else:
        duration = ' > ' + str(rng[0])

    sql = """
        select processings.runid, processings.project_id, ifnull(processings.start_time,0) as start_time, processings.pid,
        ifnull(TIMESTAMPDIFF(SECOND, processings.start_time, now()),100000) as running_time from 
       ( select runid, project_id, load_status, pid, start_time, concat(runid, ifnull(project_id, pid)) as pk 
    from qagile_load_log where load_status not in ('finished','finished-U','Abort', 'finished-E', 'OSError', 
    'force-abort')
    ) processings
    left join
       (select runid, project_id, load_status, pid, start_time, concat(runid, ifnull(project_id, pid)) as pk
            from qagile_load_log where load_status in ('finished','finished-U','Abort', 'finished-E', 'OSError', 
            'force-abort')
        ) finished
        using (pk)
    where finished.pk is null and ifnull(TIMESTAMPDIFF(SECOND, processings.start_time, now()),100000) """ + duration

    with connection.cursor() as cursor:
        cursor.execute(sql, {'duration': duration})
        data = dict_fetchall(cursor)
        cursor.close()
    return data


def set_force_abort(runid, project_id, start_time):
    sql = """update qagile_load_log set load_status = 'force-abort' where runid = %(runid)s and 
    project_id = %(project_id)s and ifnull(start_time,0) = %(start_time)s """
    with connection.cursor() as cursor:
        cursor.execute(sql, {'runid': runid, 'project_id': project_id, 'start_time': start_time})
        cursor.close()
    return True


def log_message(message):
    sql = "insert into qagile_log_log_detailed (message) values (%(message)s)"
    with connection.cursor() as cursor:
        cursor.execute(sql, {'message': message})
        cursor.close()
    return True


def get_last_pid(method):
    data = None
    sql = """ 
        select pid from qagile_load_log where method = %(method)s 
        order by rec_id desc limit 1
    """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'method': method})
        data = dict_fetchall(cursor)
        cursor.close()

    return data[0]['pid']


def get_runids_from_pids(pids):
    sql = "select distinct runid from qagile_load_log where pid in %(pids)s"
    with connection.cursor() as cursor:
        cursor.execute(sql, {'pids': pids})
        data = dict_fetchall(cursor)
        cursor.close()
    return data


def get_users_to_be_updated():
    data = []
    sql = """
        insert IGNORE into qagile_jira_users(accountId) SELECT DISTINCT users AS accountId FROM
        (SELECT DISTINCT
            executedBy AS users
        FROM
            qagile.qagile_test_executions UNION SELECT DISTINCT
            assignedTo AS users
        FROM
            qagile.qagile_test_executions) main
    WHERE
        users IS NOT NULL   
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    sql = "select distinct accountId from qagile_jira_users where name is null and accountId is not null"

    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()
    return data


def get_cycles_from_execution():
    data = []
    sql = """
        insert ignore into qagile_jira_test_cycles(project_src_id, versionId)
        select distinct p.source_id, e.versionId from qagile_test_executions e 
        left join qagile_projects p on e.project_id = p.project_id
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        cursor.close()

    sql = """
        select distinct project_src_id as projectId, versionId from qagile_jira_test_cycles where cycleId is null
    """

    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def load_cycles(records, runid):
    for record in records:
        sql = """
            update qagile_jira_test_cycles 
            set cycleId = %(cycleId)s,
            startDate = %(startDate)s,
            endDate = %(endDate)s,
            cycleName = %(cycleName)s
            where project_src_id = %(project_id)s and versionId = %(versionId)s
        """

        with connection.cursor() as cursor:
            cursor.execute(sql, record)
            cursor.close()

    return runid
