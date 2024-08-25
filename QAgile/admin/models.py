from django.db import connection
from ..home.models import dict_fetchall, isfloat
import json
from django.core.serializers.json import DjangoJSONEncoder
from ..util import models as utils


# Create your models here.
def get_all_person():
    with connection.cursor() as cursor:
        sql = """SELECT p.*, u.username, l.city, l.location_id, cast(IFNULL(r.rate,0) as char) as rate, IFNULL(ro.role,'') as role, 
                IFNULL(dom_name.domain_name,'') as domain_name, 
                (case when country = 'USA' then 'Onsite' else 'Offshore' end) as loc
                FROM qagile_person p left join qagile_person_location l on p.location_id=l.location_id
                left join qagile_person_rate r on p.rate_id=r.rate_id
                left join qagile_person_role ro on p.role_id = ro.role_id
                left join (select dt.vnid, group_concat(d.domain_name) as domain_name from qagile_domains_team dt
                left join qagile_domains d on d.domain_id = dt.domain_id
                group by dt.vnid)  dom_name on dom_name.vnid = p.vnid
                left join auth_user u on p.vnid = u.username
                where p.enddate is null
                order by p.first_name asc"""
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()
        # return "{} - {}".format("self.title", "self.artist")

        return data


def get_all_domain_leads():
    with connection.cursor() as cursor:
        sql = """SELECT p.first_name, p.last_name, p.vnid FROM qagile_person p
                , qagile_person_role r where p.role_id = r.role_id 
                and (r.`role` = 'Domain Lead' or r.`role` = 'Domain Lead - Sec') """
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()
        # return "{} - {}".format("self.title", "self.artist")

        return data


def get_all_portfolio_managers():
    with connection.cursor() as cursor:
        sql = """SELECT p.first_name, p.last_name, p.vnid FROM qagile_person p
                        , qagile_person_role r where p.role_id = r.role_id 
                        and (r.`role` = 'Portfolio Manager') """
        cursor.execute(sql)
        data = dict_fetchall(cursor)
        cursor.close()
        # return "{} - {}".format("self.title", "self.artist")

        return data


def get_person_by_id(vnid):
    with connection.cursor() as cursor:
        sql = """SELECT p.*, l.city, r.rate, ro.role, dom_name.domain_id
                FROM qagile_person p left join qagile_person_location l on p.location_id=l.location_id
                left join qagile_person_rate r on p.rate_id=r.rate_id
                left join qagile_person_role ro on p.role_id = ro.role_id
                left join (select dt.vnid, group_concat(d.domain_id) as domain_id from qagile_domains_team dt
                left join qagile_domains d on d.domain_id = dt.domain_id
                group by dt.vnid)  dom_name on dom_name.vnid = p.vnid
                where p.vnid = %(vnid)s
                order by p.first_name asc"""
        cursor.execute(sql, {'vnid': vnid})
        data = dict_fetchall(cursor)
        cursor.close()
        return data


def get_domain_by_id(domain_id):

    fields = {'domain_id': domain_id}

    with connection.cursor() as cursor:
        sql = """SELECT d.*, concat(p.first_name , ' ' , p.last_name ) as dl_name, 
                concat(pom.first_name , ' ' , pom.last_name) as pom_name,
                concat(dl2.first_name , ' ' , dl2.last_name) as dl_name_2  
                FROM qagile_domains d 
                left join qagile_person p on d.domain_lead = p.vnid 
                left join qagile_person pom on d.pom = pom.vnid
                left join qagile_person dl2 on d.domain_lead_two = dl2.vnid
                where d.domain_id= %(domain_id)s"""
        cursor.execute(sql, fields)
        data = dict_fetchall(cursor)
        cursor.close()
        return data


def get_role_by_id(role_id):

    fields = {'role_id': role_id}

    with connection.cursor() as cursor:
        sql = """SELECT * from qagile_person_role
                where role_id= %(role_id)s """
        cursor.execute(sql, fields)
        data = dict_fetchall(cursor)
        cursor.close()
        return data


def get_location_by_id(location_id):

    fields = {'location_id': location_id}

    with connection.cursor() as cursor:
        sql = "SELECT * from qagile_person_location where location_id= %(location_id)s"
        cursor.execute(sql, fields)
        data = dict_fetchall(cursor)
        cursor.close()
        return data


def get_rate_by_id(rate_id):

    fields = {'rate_id': rate_id}

    with connection.cursor() as cursor:
        sql = "SELECT * from qagile_person_rate where rate_id= %(rate_id)s"
        cursor.execute(sql, fields)
        data = dict_fetchall(cursor)
        cursor.close()
        return data


def update_person(postdata):
    vnid = postdata['vnid']
    first_name = postdata['first_name']
    last_name = postdata['last_name']
    email = postdata['email']
    category = postdata['category']
    rate_id = postdata['rate']
    phone = postdata['phone']
    role_id = postdata['role']
    location_id = postdata['location']
    domains = postdata.getlist('domain')
    org = postdata.getlist('org')

    if not rate_id.isnumeric():
        rate_id = None

    if not role_id.isnumeric():
        role_id = None

    if not location_id.isnumeric():
        location_id = None

    exist = get_person_by_id(vnid)

    fields = {'first_name': first_name, 'last_name': last_name, 'email': email, 'category': category,
                'phone': phone, 'role_id': role_id, 'location_id': location_id,
                'vnid': vnid, 'rate_id': rate_id, 'domains': domains, 'org': org}

    if not exist:
        sql = """insert into qagile_person (
                        vnid,
                        first_name,
                        last_name,
                        email,
                        category,
                        rate_id,
                        phone,
                        role_id,
                        location_id,
                        org
                    ) values(
                        %(vnid)s,
                        %(first_name)s,
                        %(last_name)s,
                        %(email)s,
                        %(category)s,
                        %(rate_id)s,
                        %(phone)s,
                        %(role_id)s,
                        %(location_id)s,
                        %(org)s
                    )
              """

    else:
        sql = """update qagile_person 
                    set vnid = %(vnid)s , 
                        first_name = %(first_name)s ,
                        last_name = %(last_name)s ,
                        email = %(email)s ,
                        category = %(category)s ,
                        phone = %(phone)s ,
                        role_id = %(role_id)s , 
                        location_id = %(location_id)s, 
                        rate_id = %(rate_id)s,
                        org = %(org)s
                    where vnid = %(vnid)s
                """
    print(sql)

    with connection.cursor() as cursor:
        cursor.execute(sql, fields)
        cursor.close()

    with connection.cursor() as cursor:
        cursor.execute("delete from qagile_domains_team where vnid = %(vnid)s", fields)
        cursor.close()

    for domain in domains:
        sql = "insert into qagile_domains_team (domain_id,vnid) values(%(domain)s, %(vnid)s)"
        print(sql)
        with connection.cursor() as cursor:
            cursor.execute(sql, {'domain': domain, 'vnid': vnid})
            cursor.close()

        if role_id == str(2):
            sql = "update qagile_domains set pom = %(vnid)s where domain_id = %(domain)s"
            print("*** UPDATE DOMAIN****")
            print(sql)
            with connection.cursor() as cursor:
                cursor.execute(sql, {'domain': domain, 'vnid': vnid})
                cursor.close()

    print("********** returning from update **********")

    return vnid


def delete_object(idfield, idvalue, username, tbl):
    fields = {idfield: idvalue, 'userid': username, 'tbl': tbl}
    sql_rec = "select * from " + tbl + " where " + idfield + " = %(" + idfield + ")s"

    with connection.cursor() as cursor:
        cursor.execute(sql_rec, fields)
        data = dict_fetchall(cursor)
        cursor.close()

    fields["obj"] = json.dumps(data, cls=DjangoJSONEncoder)

    sql_in = """insert into qagile_deleted_objects (tbl, obj, userid)
            value (%(tbl)s, %(obj)s, %(userid)s  )
    """

    sql_del = "delete from " + tbl + " where " + idfield + " = %(" + idfield + ")s"

    with connection.cursor() as cursor:
        cursor.execute(sql_in, fields)
        cursor.execute(sql_del, fields)
        cursor.close()

    return idvalue


def update_domain(postdata):
    domain_id = postdata['domain_id']
    domain_name = postdata['domain_name']
    dl_name = postdata['dl_name']
    dl_name_2 = postdata['dl_name_2']
    # pom = postdata['pom']

    if not domain_id.isnumeric():
        domain_id = None

    if dl_name == '':
        dl_name = None

    if dl_name_2 == '':
        dl_name_2 = None

    # if pom == '':
    pom = None

    fields = {'domain_id': domain_id, 'domain_name': domain_name, 'dl_name': dl_name, 'dl_name_2': dl_name_2,
                'pom': pom
                }

    exist = get_domain_by_id(domain_id)
    last_id = ''

    if not exist:
        sql = """insert into qagile_domains (
                        domain_name,
                        domain_lead,
                        domain_lead_two,
                        pom
                    ) values(
                        %(domain_name)s,
                        %(dl_name)s,
                        %(dl_name_2)s,
                        %(pom)s
                    )
              """
        last_id = "SELECT LAST_INSERT_ID()"
    else:
        sql = """update qagile_domains 
                    set 
                        domain_name = %(domain_name)s,
                        domain_lead = %(dl_name)s,
                        domain_lead_two = %(dl_name_2)s,
                        pom = %(pom)s
                    where domain_id = %(domain_id)s    
                """
    print(sql)
    with connection.cursor() as cursor:
        cursor.execute(sql, fields)
        data = cursor.fetchall()
        cursor.close()

    print("********** returning from update **********")
    print(data)

    if not exist:
        with connection.cursor() as cursor:
            cursor.execute(last_id)
            data = cursor.fetchall()
            cursor.close()
        print("!!!! LAST INSERT ID !!!!")
        domain_id = data[0][0]
        print(data)

    return domain_id


def update_role(postdata):
    role_id = postdata['role_id']
    role = postdata['role']

    if not role_id.isnumeric():
        role_id = None

    if role == '':
        role = None

    exist = get_role_by_id(role_id)
    last_id = ''

    fields = {'role': role, 'role_id': role_id}

    if not exist:
        sql = "insert into qagile_person_role (role) values(%(role)s)"
        last_id = "SELECT LAST_INSERT_ID()"
    else:
        sql = "update qagile_person_role set role = %(role)s where role_id = %(role_id)s"
    print(sql)

    with connection.cursor() as cursor:
        cursor.execute(sql, fields)
        data = cursor.fetchall()
        cursor.close()

    print("********** returning from update **********")
    print(data)

    if not exist:
        with connection.cursor() as cursor:
            cursor.execute(last_id)
            data = cursor.fetchall()
            cursor.close()
        print("!!!! LAST INSERT ID !!!!")
        role_id = data[0][0]
        print(data)

    return role_id


def update_location(postdata):
    location_id = postdata['location_id']
    city = postdata['city']
    state = postdata['state']
    zipcode = postdata['zip']
    country = postdata['country']

    if not location_id.isnumeric():
        location_id = None

    if city == '':
        city = None

    if state == '':
        state = None

    if zipcode == '':
        zipcode = None

    if country == '':
        country = None

    exist = get_location_by_id(location_id)
    last_id = ''

    fields = {'city': city, 'state': state, 'zipcode': zipcode, 'country': country, 'location_id': location_id}

    if not exist:
        sql = """insert into qagile_person_location (
                        city,
                        state,
                        zip,
                        country
                    ) values(
                       %(city)s,
                        %(state)s,
                        %(zipcode)s,
                        %(country)s
                    )
              """
        last_id = "SELECT LAST_INSERT_ID()"
    else:
        sql = """update qagile_person_location
                    set 
                        city = %(city)s,
                        state = %(state)s,
                        zip = %(zipcode)s,
                        country = %(country)s
                    where location_id = %(location_id)s    
                """
    print(sql)
    with connection.cursor() as cursor:
        cursor.execute(sql, fields)
        data = cursor.fetchall()
        cursor.close()

    print("********** returning from update **********")
    print(data)

    if not exist:
        with connection.cursor() as cursor:
            cursor.execute(last_id)
            data = cursor.fetchall()
            cursor.close()
        print("!!!! LAST INSERT ID !!!!")
        location_id = data[0][0]
        print(data)

    return location_id


def update_rate(postdata):
    rate_id = postdata['rate_id']
    rate = postdata['rate']
    category = postdata['category']

    if not rate_id.isnumeric():
        rate_id = None

    if not isfloat(rate):
        rate = None

    if category == '':
        category = None

    exist = get_rate_by_id(rate_id)
    last_id = ''

    fields = {
        'rate': rate,
        'category': category,
        'rate_id': rate_id
    }

    if not exist:
        sql = "insert into qagile_person_rate (rate, category) values(%(rate)s, %(category)s )"
        last_id = "SELECT LAST_INSERT_ID()"
    else:
        sql = "update qagile_person_rate set rate = %(rate)s, category = %(category)s  where rate_id = %(rate_id)s"
    print(sql)

    with connection.cursor() as cursor:
        cursor.execute(sql, fields)
        data = cursor.fetchall()
        cursor.close()

    print("********** returning from update **********")
    print(data)

    if not exist:
        with connection.cursor() as cursor:
            cursor.execute(last_id)
            data = cursor.fetchall()
            cursor.close()
        print("!!!! LAST INSERT ID !!!!")
        rate_id = data[0][0]
        print(data)

    return rate_id


def get_all_domain():
    with connection.cursor() as cursor:
        cursor.execute("""SELECT d.*, concat(p.first_name , ' ' , p.last_name ) as dl_name, 
                concat(pom.first_name , ' ' , pom.last_name) as pom_name,
                concat(dl2.first_name , ' ' , dl2.last_name) as dl_name_2  
                FROM qagile_domains d 
                left join qagile_person p on d.domain_lead = p.vnid 
                left join qagile_person pom on d.pom = pom.vnid
                left join qagile_person dl2 on d.domain_lead_two = dl2.vnid""")
        data = dict_fetchall(cursor)
        return data


def get_all_rates():
    with connection.cursor() as cursor:
        cursor.execute("select * from qagile_person_rate")
        data = dict_fetchall(cursor)
        return data


def get_all_roles():
    with connection.cursor() as cursor:
        cursor.execute("select * from qagile_person_role")
        data = dict_fetchall(cursor)
        return data


def get_all_locations():
    with connection.cursor() as cursor:
        cursor.execute("select * from qagile_person_location")
        data = dict_fetchall(cursor)
        return data


def load_raw_qmo(records):
    sql = "delete from qagile_person_qmo where recid !=0"
    with connection.cursor() as cursor:
        cursor.execute(sql)

    for idx in range(len(records)):
        sql = """
        INSERT INTO `qagile`.`qagile_person_qmo`
            (
            `name`,
            `email_ibm`,
            `email_rbs`,
            `location`,
            `type`,
            `billrate`,
            `phone`,
            `domain`,
            `ahid`,
            `vnid`)
            VALUES
            (%(name)s,
            %(email_ibm)s,
            %(email_rbs)s,
            %(location)s,
            %(type)s,
            %(billrate)s,
            %(phone)s,
            %(domain)s,
            %(ahid)s,
            %(vnid)s)
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, records[idx])

    return None


def delete_person(vnid, enddate):
    if not utils.is_date(enddate):
        enddate =datetime.now().strftime('%Y-%m-%d')

    sql = "update qagile.qagile_person set enddate = %(enddate)s where vnid = %(vnid)s"

    with connection.cursor() as cursor:
        cursor.execute(sql, {'vnid': vnid, 'enddate': enddate})

    return vnid
