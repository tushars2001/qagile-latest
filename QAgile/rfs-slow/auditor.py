from django.db import models
from django.db import connection
from ..admin.models import dict_fetchall, isfloat
from datetime import datetime, timedelta
import pdb


def check_estimates_resources_hrs(rfs_request_id):
    sql = """
        select ifnull(res_load_total_hrs,0) res_load_total_hrs, ifnull(est_totals_hrs,0) est_totals_hrs from 
        (SELECT sum(hrs) as res_load_total_hrs FROM qagile.qagile_rfs_resourceloading where rfs_request_id = %(rfs_request_id)s) rlhrs,
        (SELECT sum(effort) as est_totals_hrs FROM qagile.qagile_rfs_estimates where rfs_request_id = %(rfs_request_id)s and activity_code != 'TRVCO') esthr
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, {'rfs_request_id': rfs_request_id})
        data = dict_fetchall(cursor)
        cursor.close()

    return data[0]


def check_estimates_resources_pom_hrs(rfs_request_id):
    sql = """
            select ifnull(est_pom_hrs,0) est_pom_hrs, ifnull(rl_pom_hrs,0) rl_pom_hrs from
            (SELECT sum(effort) as est_pom_hrs FROM qagile.qagile_rfs_estimates 
                where rfs_request_id = %(rfs_request_id)s and activity_code = 'POM') est,
            (
                SELECT sum(hrs) as rl_pom_hrs FROM qagile.qagile_rfs_resourceloading rl left join qagile.qagile_person p
                 on p.vnid = rl.person_id 
             join qagile.qagile_person_role pr on p.role_id = pr.role_id and pr.role_code = 'POM'
            where rl.rfs_request_id = %(rfs_request_id)s) res_l
        """
    with connection.cursor() as cursor:
        cursor.execute(sql, {'rfs_request_id': rfs_request_id})
        data = dict_fetchall(cursor)
        cursor.close()

    return data[0]


def check_resources_enddated(rfs_request_id):
    sql = """
            select p.vnid, p.first_name, p.last_name, 
            date_format(rl.week,'%%Y-%%M-%%d') as week, date_format(p.enddate, '%%Y-%%M-%%d') as enddate
             from qagile.qagile_rfs_resourceloading rl, qagile.qagile_person p 
            where p.vnid = rl.person_id and rl.rfs_request_id = %(rfs_request_id)s
            and p.enddate is not null
            and p.enddate < rl.week
        """
    with connection.cursor() as cursor:
        cursor.execute(sql, {'rfs_request_id': rfs_request_id})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def check_resources_overloaded(rfs_request_id):
    sql = """
            select person_id as vnid, date_format(week,'%%Y-%%M-%%d') as week, sum(hrs) as weekly_hrs, first_name, last_name from (
                select rl.person_id, date_format(rl.week, '%%Y-%%m-%%d') as week, rl.hrs, p.first_name, p.last_name  
                from qagile.qagile_rfs_resourceloading rl, qagile.qagile_person p 
                where p.vnid = rl.person_id and rl.rfs_request_id = %(rfs_request_id)s
                union
                select rl.person_id, date_format(rl.week, '%%Y-%%m-%%d') as week, rl.hrs , p.first_name, p.last_name 
                from qagile.qagile_rfs_resourceloading rl, qagile.qagile_rfs_request r, qagile.qagile_person p 
                where r.rfs_request_id = rl.rfs_request_id and p.vnid=rl.person_id 
                and r.active = 1 and r.rfs_request_id != %(rfs_request_id)s
                and concat(rl.person_id,date_format(rl.week, '%%Y-%%m-%%d'))  in 
                    (   
                        select concat(person_id,date_format(week, '%%Y-%%m-%%d')) as keystring 
                        from qagile.qagile_rfs_resourceloading where rfs_request_id = %(rfs_request_id)s
                    )
                ) 
            main group by person_id, week, first_name, last_name
            having sum(hrs) > 45
        """
    with connection.cursor() as cursor:
        cursor.execute(sql, {'rfs_request_id': rfs_request_id})
        data = dict_fetchall(cursor)
        cursor.close()

    return data


def check_racis(rfs_request_id):
    sql = """
            select sit, uat from 
            (select count(raci_scope) as sit from qagile.qagile_rfs_estimates_raci 
            where rfs_request_id = %(rfs_request_id)s and raci_scope = 'SIT' group by raci_scope) s,
            (select count(raci_scope) as uat from qagile.qagile_rfs_estimates_raci 
            where rfs_request_id = %(rfs_request_id)s and raci_scope = 'UAT' group by raci_scope) u 
        """
    with connection.cursor() as cursor:
        cursor.execute(sql, {'rfs_request_id': rfs_request_id})
        data = dict_fetchall(cursor)
        cursor.close()
    if len(data):
        return data[0]
    else:
        return None
