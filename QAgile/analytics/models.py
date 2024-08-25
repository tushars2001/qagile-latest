import pdb

from django.db import connection
from ..home.models import dict_fetchall, isfloat


# Create your models here.
def load_tenrox_data(tenrox_data):
    records = 0

    with connection.cursor() as cursor:
        cursor.execute("TRUNCATE qagile_tenrox_data")

    for rec in tenrox_data['Reports']['Detail']['Table1']:
        sql = """insert into qagile_tenrox_data (vnid, ppmid, week_end, hrs, first_name) values(
            %(GroupUser_Logon_Name)s, %(Project_Code)s, %(Week_Ending_Date)s, %(Total_Time)s, %(Full_Name)s
        )
        """
        records = records + 1
        # print("inserted")
        with connection.cursor() as cursor:
            cursor.execute(sql, rec)

    with connection.cursor() as cursor:
        cursor.execute("""
        INSERT INTO `qagile`.`qagile_tenrox_invalid_vnids`
        (`tenrox_vnid`,`tenrox_name`)
        select distinct vnid, first_name from qagile_tenrox_data where length(vnid) > 7
        and vnid not in (select tenrox_vnid from  qagile_tenrox_invalid_vnids)
        """)

    # with connection.cursor() as cursor:
    #    cursor.execute("""
    #        update qagile_tenrox_data set weekstart = DATE_SUB(str_to_date(SUBSTRING(week_end, 1, 10), '%m/%d/%Y'),
    #        INTERVAL 5 DAY) where record_id != 0
    #    """)
    #
    # with connection.cursor() as cursor:
    #    cursor.execute("""
    #        update qagile_tenrox_data set vnid = 'vn03616' where record_id != 0 and vnid='VN00016808'
    #    """)
    #
    # with connection.cursor() as cursor:
    #    cursor.execute("""
    #        update qagile_tenrox_data set vnid = 'VN03710' where record_id != 0 and vnid='VN00019650'
    #    """)
    #
    # with connection.cursor() as cursor:
    #    cursor.execute("""
    #        update qagile_tenrox_data set vnid = 'vn02185' where record_id != 0 and vnid='VN00014416'
    #    """)
    #
    # with connection.cursor() as cursor:
    #    cursor.execute("""
    #        update qagile_tenrox_data set vnid = 'vn03709' where record_id != 0 and vnid='VN00019658'
    #    """)
    #
    # with connection.cursor() as cursor:
    #    cursor.execute("""
    #        update qagile_tenrox_data set vnid = 'vn08613' where record_id != 0 and vnid='VN00023499'
    #    """)
    #
    # with connection.cursor() as cursor:
    #    cursor.execute("""
    #        update qagile_tenrox_data set vnid = 'rpaolin' where record_id != 0 and vnid='US00036912'
    #    """)

    return records


def load_jira_data(jira_data):
    records = 0

    for rec in jira_data['executions']['execution']:
        if 'executedBy' in rec:
            executedBy = rec['executedBy']
        else:
            executedBy = None

        if 'executedOn' in rec:
            executedOn = rec['executedOn']
        else:
            executedOn = None

        if 'assignedTo' in rec:
            assignedTo = rec['assignedTo']
        else:
            assignedTo = None

        record = {
            'tcid': rec['issueKey'],
            'source_id': rec['executionId'],
            'executedBy': executedBy,
            'executedOn': executedOn,
            'status': rec['executedStatus'],
            'cycleName': rec['cycleName'],
            'source': 'Jira',
            'project_id': None,
            'assignedTo': assignedTo
        }

        sql = """INSERT INTO `qagile`.`qagile_test_executions`
                (
                `tcid`,
                `source_id`,
                `executedBy`,
                `executedOnStr`,
                `status`,
                `cycleName`,
                `source`,
                `assignedTo`)
                VALUES
                (
                %(tcid)s,
                %(source_id)s,
                %(executedBy)s,
                %(executedOn)s,
                %(status)s,
                %(cycleName)s,
                %(source)s,
                %(assignedTo)s
                )

        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, record)
                records = records + 1
        except Exception as e:
            print(e)

    return records


def get_chart_data(filters):
    conditions = ' '

    if filters['domains']:
        domains = "and rfs.domain_id in (" + str(filters['domains']).replace("[", "").replace("]", "") + ")"
        conditions = conditions + domains + " "

    if filters['ppmids']:
        projects = "and rfs.project_id in (" + str(filters['ppmids']).replace("[", "").replace("]", "") + ")"
        conditions = conditions + projects + " "

    if filters['vnids']:
        vnids = "and p.vnid in (" + str(filters['vnids']).replace("[", "").replace("]", "") + ")"
        conditions = conditions + vnids + " "

    if filters['dt_range']:
        dt_range = """ and (week between 
                    from_unixtime(""" + filters['dt_range'][0].split(";")[0] + """/1000) 
                    and 
                    from_unixtime(""" + filters['dt_range'][0].split(";")[1] + """/1000)
                    ) """
        conditions = conditions + dt_range + " "

    sql_planned = """
        select weekstart, sum(hrs) as hrs from qagile_v_planned_approved 
        """ + conditions + """ 
        group by weekstart order by weekstart asc
    """

    sql_planned = """
                select  week as weekstart, sum(rl.hrs) as hrs
                from qagile_rfs_resourceloading rl, 
                qagile_person p, qagile_rfs_request rfs 
                where rfs.rfs_request_id = rl.rfs_request_id 
                and rl.person_id = p.vnid 
                and rfs.active=1
                and rl.hrs > 0
                """ + conditions + """ 
                group by week order by week asc

        """

    sql_actuals = """
        select weekstart, sum(hrs) as hrs from qagile_v_actuals 
        """ + " where 1=0 " + """ 
        group by weekstart order by weekstart asc
    """

    sql_capacity = """

    """

    print(sql_actuals, sql_planned)

    with connection.cursor() as cursor:
        cursor.execute(sql_planned)
        planned = dict_fetchall(cursor)
        cursor.execute(sql_actuals)
        actuals = dict_fetchall(cursor)
        cursor.close()

    chartdata = {
        'planned': planned,
        'actuals': actuals,
        'sqls': {'planned': sql_planned, 'actuals': sql_actuals}
    }

    return chartdata


def get_domains_info():
    with connection.cursor() as cursor:
        sql = """select distinct domain_id, domain_name from qagile_domains"""
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_project_person():
    # gives list of domains who's resources are involved in approved RFSs
    sql = """
                select prj.rfs_request_id, prj.project_id, res.vnid from (
                select project_id, rfs_request_id, approval_num from qagile_rfs_request_approved where concat(rfs_request_id, '-', (approval_num)) in (
                select concat(rfs_request_id, '-', max(approval_num)) from qagile_rfs_request_approved rfs
                where project_id is not null
                group by rfs_request_id
                )
            ) prj,
            (
                select  distinct person_id as vnid, rfs_request_id, approval_num  from qagile_rfs_resourceloading_approved where concat(rfs_request_id, '-', approval_num) in 
                (
                select concat(rfs_request_id, '-', max(approval_num)) as ver from qagile_rfs_request_approved rfs
                where project_id is not null
                group by rfs_request_id
                ) 

            ) res
            where prj.rfs_request_id = res.rfs_request_id and prj.approval_num = res.approval_num
            """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_domain_person():
    sql = """SELECT t.vnid, t.domain_id FROM qagile.qagile_person p right join qagile.qagile_domains_team t
                       on p.vnid = t.vnid"""
    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_project_info():
    with connection.cursor() as cursor:
        sql = """select distinct ra.project_id, ra.project_name from qagile.qagile_rfs_request_approved ra,
        qagile_rfs_request r where r.rfs_request_id = ra.rfs_request_id and r.active = 1"""
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_persons_info():
    with connection.cursor() as cursor:
        sql = """select distinct vnid, first_name, last_name from qagile_v_project_resource"""
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_resource_data(filters):
    conditions = 'and 1=1 '
    if filters['vnids'] and len(filters['vnids']):
        vnids = "and rl.person_id in (" + str(filters['vnids']).replace("[", "").replace("]", "") + ")"
        conditions = conditions + vnids + " "
    elif filters['domains']:
        domain_vnids = """and p.vnid in (select vnid from qagile_domains_team 
        where domain_id in (""" + str(filters['domains']).replace("[", "").replace("]", "") + "))"
        conditions = conditions + domain_vnids + " "

    if not (filters['plan']):
        status = " and rfs.rfs_status = 'APPROVED'"
        conditions = conditions + status + " "
    if 'dt_range' in filters:
        dt_range = """and (week between 
                    from_unixtime(""" + filters['dt_range'][0].split(";")[0] + """/1000) 
                    and 
                    from_unixtime(""" + filters['dt_range'][0].split(";")[1] + """/1000)
                    )"""
        conditions = conditions + dt_range + " "

    sql_planned = """
            select  rl.rfs_request_id, rl.person_id, date_format((rl.week),'%Y-%m-%d') as week, rl.hrs, 
            p.first_name, p.last_name, rfs.project_id, rfs.project_name
            from qagile_rfs_resourceloading rl, 
            qagile_person p, qagile_rfs_request rfs 
            where rfs.rfs_request_id = rl.rfs_request_id 
            and rl.person_id = p.vnid 
            and rfs.active=1
            and rl.hrs > 0
            """ + conditions + """ 
            order by  p.vnid, rfs.project_id, week 
    """

    print(sql_planned)
    with connection.cursor() as cursor:
        cursor.execute(sql_planned)
        chartdata = dict_fetchall(cursor)
        cursor.close()
    return chartdata


def get_datahead(filters):
    conditions = 'and 1=1 '

    if filters['vnids'] and len(filters['vnids']):
        vnids = "and rl.person_id in (" + str(filters['vnids']).replace("[", "").replace("]", "") + ")"
        conditions = conditions + vnids + " "
    elif filters['domains']:
        domain_vnids = """and p.vnid in (select vnid from qagile_domains_team 
        where domain_id in (""" + str(filters['domains']).replace("[", "").replace("]", "") + "))"
        conditions = conditions + domain_vnids + " "

    if not (filters['plan']):
        status = " and rfs.rfs_status = 'APPROVED'"
        conditions = conditions + status + " "

    if 'dt_range' in filters:
        dt_range = """and (week between 
                    from_unixtime(""" + filters['dt_range'][0].split(";")[0] + """/1000) 
                    and 
                    from_unixtime(""" + filters['dt_range'][0].split(";")[1] + """/1000)
                    )"""
        conditions = conditions + dt_range + " "

    sql_planned = """
    select floor(DATEDIFF(max(week), min(week))/7) as numofweeks, 
    date_format(min(week),'%Y-%m-%d') as weekstart, date_format(max(week),'%Y-%m-%d') as weeksend from (
        select rl.*, p.first_name, p.last_name, rfs.project_id, rfs.project_name
        from qagile_rfs_resourceloading rl,
        qagile_person p, qagile_rfs_request rfs 
        where rfs.rfs_request_id = rl.rfs_request_id 
        and rl.person_id = p.vnid 
        and rfs.active=1
        """ + conditions + """ 
        order by  p.vnid, rfs.project_id, week 
    ) data
    """

    print("***********************************************************8")
    print(sql_planned)

    with connection.cursor() as cursor:
        cursor.execute(sql_planned)
        datahead = dict_fetchall(cursor)
        cursor.close()

    print(datahead)

    return datahead


def get_statictbl(filters):
    conditions = 'and 1=1 '

    if filters['vnids'] and len(filters['vnids']):
        vnids = "and rl.person_id in (" + str(filters['vnids']).replace("[", "").replace("]", "") + ")"
        conditions = conditions + vnids + " "
    elif filters['domains']:
        domain_vnids = """and p.vnid in (select vnid from qagile_domains_team 
        where domain_id in (""" + str(filters['domains']).replace("[", "").replace("]", "") + "))"
        conditions = conditions + domain_vnids + " "

    if not (filters['plan']):
        status = " and rfs.rfs_status = 'APPROVED'"
        conditions = conditions + status + " "

    if 'dt_range' in filters:
        dt_range = """and (week between 
                    from_unixtime(""" + filters['dt_range'][0].split(";")[0] + """/1000) 
                    and 
                    from_unixtime(""" + filters['dt_range'][0].split(";")[1] + """/1000)
                    )"""
        conditions = conditions + dt_range + " "

    sql_planned = """
    select distinct concat(first_name, " ", last_name) as name, 
                project_id as project_id,
                '' as domain_name, 
                'Resource' as role,
                '0' as rate,
                rfs_request_id, person_id, project_name, link
    from (
        select rl.*, p.first_name, p.last_name, rfs.project_id, rfs.project_name, rfs.link
        from qagile_rfs_resourceloading rl, 
        qagile_person p, qagile_rfs_request rfs 
        where rfs.rfs_request_id = rl.rfs_request_id 
        and rl.person_id = p.vnid 
        and rl.hrs > 0
        and rfs.active=1
        """ + conditions + """ 
        order by p.vnid, rfs.project_id, week 
    ) data order by person_id, project_id
    """
    with connection.cursor() as cursor:
        cursor.execute(sql_planned)
        datahead = dict_fetchall(cursor)
        cursor.close()

    print("****STATIC*****")
    print(sql_planned)

    return datahead


def get_jobs_summary(duration=None):
    where = " where 1=1 "
    if duration:
        where = where + " and start_time >= '" + duration + "' "
    sql = """
        SELECT 
    runid,
    start_time,
    end_time,
    duration,
    SUM(records_expected) records_expected,
    SUM(records_processed) records_processed,
    log_steps,
    method,
    ifnull(load_status, 'Finished') load_status
FROM
    (SELECT 
        l.runid,
            MIN(l.start_time) AS start_time,
            MAX(l.end_time) AS end_time,
            SEC_TO_TIME(TIMESTAMPDIFF(SECOND, MIN(l.start_time), MAX(l.end_time))) AS `duration`,
            sub.records_expected,
            sub.records_processed,
            COUNT(*) AS log_steps,
            l.method,
            not_finished.load_status
    FROM
        qagile_load_log l
    LEFT JOIN (SELECT 
        runid,
            SUM(records_expected) records_expected,
            SUM(records_processed) records_processed
    FROM
        (SELECT 
        runid,
            MAX(records_expected) records_expected,
            MAX(records_processed) records_processed,
            project_id
    FROM
        qagile_load_log
    WHERE
        method NOT IN ('Status' , 'kill')
    GROUP BY runid , project_id) main
    GROUP BY runid) sub ON l.runid = sub.runid
        left join (
            select distinct runid, 'Not Finished' as load_status from (            
                SELECT 
                    runid, project_id, 'Not Finished' AS load_status
                FROM
                    qagile_load_log
                WHERE

                         method not in ('Status') and project_id is not null and CONCAT(runid, project_id) NOT IN (SELECT 
                            CONCAT(runid, project_id)
                        FROM
                            qagile_load_log
                        WHERE
                            load_status in ('finished', 'Abort-Exception','Starting-data','Completed-data',
                             'Starting-No-data','Completed-No-data') and method not in ('Status') and project_id 
                             is not null
                        GROUP BY runid , project_id , load_status)
                GROUP BY runid , project_id , 'Not Finished'
                ) unfinished
        ) not_finished on l.runid = not_finished.runid
    WHERE
        l.method NOT IN ('Status' , 'kill')
    GROUP BY l.runid , l.method , sub.records_expected , sub.records_processed, not_finished.load_status) summary
    """ + where + """
GROUP BY runid , start_time , end_time , duration , log_steps , method, load_status"""

    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def get_job_details(runid):
    sql = """
            select runid, l.project_id, p.title, method, load_type, start_time, end_time, 
            sec_to_time(TIMESTAMPDIFF(SECOND, start_time, end_time)) as `duration`, load_status, 
            records_expected, records_processed  from qagile_load_log l left join qagile_projects p 
               on p.project_id = l.project_id where runid = %(runid)s order by rec_id desc    
       """

    with connection.cursor() as cursor:
        cursor.execute(sql, {'runid': runid})
        data = dict_fetchall(cursor)
        cursor.close()
    return data


def get_all_projects():
    sql = """
    SELECT p.project_id, p.title, p.track, ifnull(tcs.cnt, 0) as test_cases, ifnull(defects.cnt, 0) as defects,
    exec.passed, exec.failed, exec.na, exec.unexecuted, exec.wip, exec.deferred, exec.blocked, exec.test_executions,
    last_runs.tcs_last_ran, last_runs.defect_last_ran, last_runs.execution_last_ran
    FROM qagile.qagile_projects p
    left join (select project_id, count(project_id) as cnt from qagile_test_cases group by project_id) tcs
    on p.project_id = tcs.project_id
    left join (select project_id, count(project_id) as cnt from qagile_defects group by project_id) defects
    on p.project_id = defects.project_id
    left join (select project_id, sum(passed) passed, sum(failed) failed, sum(na) na, sum(unexecuted) unexecuted,
    sum(wip) wip, sum(deferred) deferred, sum(blocked) blocked, count(*) test_executions from
    (
        select project_id, 
        case when status = 'PASS' then 1 else 0 end as passed,
        case when status = 'FAIL' then 1 else 0 end as failed,
        case when status = 'NOT APPLICABLE' then 1 else 0 end as na,
        case when status = 'UNEXECUTED' then 1 else 0 end as unexecuted,
        case when status = 'WIP' then 1 else 0 end as wip,
        case when status = 'DEFERRED' then 1 else 0 end as deferred,
        case when status = 'BLOCKED' then 1 else 0 end as blocked
        from qagile_test_executions
    ) m group by project_id) exec
    on p.project_id = exec.project_id
    left join (select project_id, max(tcs_last_ran) as tcs_last_ran, 
    max(defect_last_ran) as defect_last_ran, max(execution_last_ran) as execution_last_ran
    from (
        select project_id, 
        case when locate('tcs', method) > 0 then last_ran else NULL end as tcs_last_ran,
        case when locate('defect', method) > 0 then last_ran else NULL end as defect_last_ran,
        case when locate('execution', method) > 0 then last_ran else NULL end as execution_last_ran
        from (
        select project_id, method, max(end_time) as last_ran from qagile_load_log group by project_id, method
        ) m where project_id is not null
    ) main group by project_id) last_runs on p.project_id = last_runs.project_id
           """
    with connection.cursor() as cursor:
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()
    return data


def update_tracking(project_id, track):
    sql = "update qagile.qagile_projects set track = %(track)s where project_id = %(project_id)s"
    with connection.cursor() as cursor:
        cursor.execute(sql, {'project_id': project_id, 'track': track})
        cursor.close()
    return True


def get_org_chart_data(filters):
    conditions = ' '

    if filters['dt_range']:
        dt_range = """ and (week between 
                        from_unixtime(""" + filters['dt_range'][0].split(";")[0] + """/1000) 
                        and 
                        from_unixtime(""" + filters['dt_range'][0].split(";")[1] + """/1000)
                        ) """
        conditions = conditions + dt_range + " "

    sql_planned = """
                    select  week as weekstart, sum(rl.hrs) as hrs
                    from qagile_rfs_resourceloading rl, 
                    qagile_person p, qagile_rfs_request rfs 
                    where rfs.rfs_request_id = rl.rfs_request_id 
                    and rl.person_id = p.vnid 
                    and rfs.active=1
                    and rl.hrs > 0
                    """ + conditions + """ 
                    group by week order by week asc

            """

    sql_actuals = """
            select weekstart, sum(hrs) as hrs from qagile_v_actuals 
            """ + " where 1=0 " + """ 
            group by weekstart order by weekstart asc
        """

    sql_capacity = """
        SELECT week as weekstart, sum(hrs) as hrs FROM qagile.qagile_capacity
            where 1=1 """ + conditions + """
            group by week
        """

    print(sql_actuals, sql_planned, sql_capacity)

    with connection.cursor() as cursor:
        cursor.execute(sql_planned)
        planned = dict_fetchall(cursor)
        cursor.execute(sql_actuals)
        actuals = dict_fetchall(cursor)
        cursor.execute(sql_capacity)
        capacity = dict_fetchall(cursor)
        cursor.close()

    chartdata = {
        'planned': planned,
        'actuals': actuals,
        'capacity': capacity,
        'sqls': {'planned': sql_planned, 'actuals': sql_actuals, 'capacity': sql_capacity}
    }

    return chartdata


def get_domains_chart_data(filters):
    conditions = ' '
    data_vnids = []
    if filters['domains']:
        sql_vnid = """ SELECT distinct vnid FROM qagile.qagile_domains_team where domain_id in (""" + str(
            filters['domains']). \
            replace("[", "").replace("]", "") + ")"
        with connection.cursor() as cursor:
            cursor.execute(sql_vnid)
            data_vnids_dict = dict_fetchall(cursor)
            for rec in data_vnids_dict:
                if rec['vnid']:
                    print("data_vnid is:", data_vnids, "rec_vnid is: ", rec['vnid'])
                    data_vnids.append(rec['vnid'])

    if len(data_vnids):
        vnids = "and p.vnid in (" + str(data_vnids).replace("[", "").replace("]", "") + ")"
        conditions = conditions + vnids + " "

    if filters['dt_range']:
        dt_range = """ and (week between 
                        from_unixtime(""" + filters['dt_range'][0].split(";")[0] + """/1000) 
                        and 
                        from_unixtime(""" + filters['dt_range'][0].split(";")[1] + """/1000)
                        ) """
        conditions = conditions + dt_range + " "

    sql_planned = """
                    select  week as weekstart, sum(rl.hrs) as hrs
                    from qagile_rfs_resourceloading rl, qagile_person p,
                    qagile_rfs_request rfs 
                    where rfs.rfs_request_id = rl.rfs_request_id and p.vnid=rl.person_id
                    and rfs.active=1
                    and rl.hrs > 0
                    """ + conditions + """ 
                    group by week order by week asc

            """

    sql_actuals = """
            select weekstart, sum(hrs) as hrs from qagile_v_actuals 
            """ + " where 1=0 " + """ 
            group by weekstart order by weekstart asc
        """
    where = ""
    if data_vnids:
        where = " where vnid in (" + str(data_vnids).replace("[", "").replace("]", "") + ")"
    sql_capacity = "SELECT week as weekstart, sum(hrs) as hrs FROM qagile.qagile_capacity " + where + " group by week"

    print(sql_actuals, sql_planned, sql_capacity)

    with connection.cursor() as cursor:
        cursor.execute(sql_planned)
        planned = dict_fetchall(cursor)
        cursor.execute(sql_actuals)
        actuals = dict_fetchall(cursor)
        cursor.execute(sql_capacity)
        capacity = dict_fetchall(cursor)
        cursor.close()

    chartdata = {
        'planned': planned,
        'actuals': actuals,
        'capacity': capacity,
        'sqls': {'planned': sql_planned, 'actuals': sql_actuals, 'capacity': capacity}
    }

    return chartdata


def get_projects_chart_data(filters):
    conditions = ' '
    if filters['ppmids']:
        projects = "and rfs.project_id in (" + str(filters['ppmids']).replace("[", "").replace("]", "") + ")"
        conditions = conditions + projects + " "

    if filters['dt_range']:
        dt_range = """ and (week between 
                            from_unixtime(""" + filters['dt_range'][0].split(";")[0] + """/1000) 
                            and 
                            from_unixtime(""" + filters['dt_range'][0].split(";")[1] + """/1000)
                            ) """
        conditions = conditions + dt_range + " "

    sql_planned = """
                        select  week as weekstart, sum(rl.hrs) as hrs
                        from qagile_rfs_resourceloading rl, 
                        qagile_rfs_request rfs 
                        where rfs.rfs_request_id = rl.rfs_request_id 
                        and rfs.active=1
                        and rl.hrs > 0
                        """ + conditions + """ 
                        group by week order by week asc """

    sql_actuals = """
                select weekstart, sum(hrs) as hrs from qagile_v_actuals 
                """ + " where 1=0 " + """ 
                group by weekstart order by weekstart asc
            """

    print(sql_actuals, sql_planned)

    with connection.cursor() as cursor:
        cursor.execute(sql_planned)
        planned = dict_fetchall(cursor)
        cursor.execute(sql_actuals)
        actuals = dict_fetchall(cursor)
        cursor.close()

    chartdata = {
        'planned': planned,
        'actuals': actuals,
        'sqls': {'planned': sql_planned, 'actuals': sql_actuals}
    }

    return chartdata


def get_resources_chart_data(filters):
    conditions = ' '

    if filters['vnids']:
        vnids = "and p.vnid in (" + str(filters['vnids']).replace("[", "").replace("]", "") + ")"
        conditions = conditions + vnids + " "

    if filters['dt_range']:
        dt_range = """ and (week between 
                            from_unixtime(""" + filters['dt_range'][0].split(";")[0] + """/1000) 
                            and 
                            from_unixtime(""" + filters['dt_range'][0].split(";")[1] + """/1000)
                            ) """
        conditions = conditions + dt_range + " "

    sql_planned = """
                        select  week as weekstart, sum(rl.hrs) as hrs
                        from qagile_rfs_resourceloading rl, 
                        qagile_rfs_request rfs , qagile_person p
                        where rfs.rfs_request_id = rl.rfs_request_id  and p.vnid = rl.person_id
                        and rfs.active=1
                        and rl.hrs > 0
                        """ + conditions + """ 
                        group by week order by week asc

                """

    sql_actuals = """
                select weekstart, sum(hrs) as hrs from qagile_v_actuals 
                """ + " where 1=0 " + """ 
                group by weekstart order by weekstart asc
            """

    where = ""
    if len(filters['vnids']):
        where = " where vnid in %(vnids)s"
    sql_capacity = "SELECT week as weekstart, sum(hrs) as hrs FROM qagile.qagile_capacity " + where + " group by week"

    print(sql_actuals, sql_planned, sql_capacity)

    with connection.cursor() as cursor:
        cursor.execute(sql_planned)
        planned = dict_fetchall(cursor)
        cursor.execute(sql_actuals)
        actuals = dict_fetchall(cursor)
        cursor.execute(sql_capacity, {'vnids': filters['vnids']})
        capacity = dict_fetchall(cursor)
        cursor.close()

    chartdata = {
        'planned': planned,
        'actuals': actuals,
        'capacity': capacity,
        'sqls': {'planned': sql_planned, 'actuals': sql_actuals, 'capacity': capacity}
    }

    return chartdata
