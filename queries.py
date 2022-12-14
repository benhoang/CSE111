import sqlite3
from sqlite3 import Error
import time
from datetime import datetime,timedelta

def openConnection(_dbFile):
    print("++++++++++++++++++++++++++++++++++")
    print("Open database: ", _dbFile)

    conn = None
    try:
        conn = sqlite3.connect(_dbFile)
        print("success")
    except Error as e:
        print(e)

    print("++++++++++++++++++++++++++++++++++")

    return conn


def closeConnection(_conn, _dbFile):
    print("++++++++++++++++++++++++++++++++++")
    print("Close database: ", _dbFile)

    try:
        _conn.close()
        print("success")
    except Error as e:
        print(e)

    print("++++++++++++++++++++++++++++++++++")


def createTable(_conn):
    print("++++++++++++++++++++++++++++++++++")
    print("Create table")
    mycursor = _conn.cursor()

    sql = """CREATE TABLE IF NOT EXISTS orders
    (o_orderkey decimal(9,0) not null,
    o_custkey decimal(9,0) not null,
    o_totalprice decimal(8,2) not null,
    o_orderdate date not null,
    o_tripkey decimal(9,0) not null
    )"""
    mycursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS customer
    (c_custkey decimal(9,0) not null,
    c_memberstatus char(1) not null,
    c_memberkey decimal(9,0) not null,
    c_name varchar(25) not null,
    c_address varchar(40) not null,
    c_phone char(15) not null,
    c_email varchar(40) not null
    )"""
    mycursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS member
    (m_custkey decimal(9,0) not null,
    m_memberkey decimal(9,0) not null,
    m_discounts decimal(7,2) not null
    )"""
    mycursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS seats
    (s_trainkey decimal(9,0) not null,
    s_seatkey decimal(9,0) not null,
    s_status char(1) not null,
    s_seattype char(9) not null
    )"""
    mycursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS trip
    (t_tripkey decimal(9,0) not null,
    t_orderkey decimal(9,0) not null,
    t_trainkey decimal(9,0) not null,
    t_buskey decimal(9,0) not null,
    t_seatkey decimal(9,0) not null,
    t_stationkey decimal(9,0) not null
    )"""
    mycursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS location
    (l_stationkey decimal(9,0) not null,
    l_city char(15) not null,
    l_buskey decimal(9,0) not null,
    l_trainkey decimal(9,0) not null
    )"""
    mycursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS train
    (tn_trainkey decimal(9,0) not null,
    tn_stationkey decimal(9,0) not null,
    tn_departurecity char(15) not null,
    tn_departure time not null,
    tn_arrivalcity char(15) not null,
    tn_arrival time not null
    )"""
    mycursor.execute(sql)

    sql = """CREATE TABLE IF NOT EXISTS bus
    (b_buskey decimal(9,0) not null,
    b_stationkey decimal(9,0) not null,
    b_departurecity char(15) not null,
    b_departure time not null,
    b_arrivalcity char(15) not null,
    b_arrival time not null
    )"""
    mycursor.execute(sql)

    _conn.commit()
    print("++++++++++++++++++++++++++++++++++")


def dropTable(_conn):
    print("++++++++++++++++++++++++++++++++++")
    print("Drop tables")
    mycursor = _conn.cursor()

    sql = """DROP TABLE IF EXISTS orders"""
    mycursor.execute(sql)

    sql = """DROP TABLE IF EXISTS customer"""
    mycursor.execute(sql)

    sql = """DROP TABLE IF EXISTS member"""
    mycursor.execute(sql)

    sql = """DROP TABLE IF EXISTS seats"""
    mycursor.execute(sql)

    sql = """DROP TABLE IF EXISTS trip"""
    mycursor.execute(sql)

    sql = """DROP TABLE IF EXISTS location"""
    mycursor.execute(sql)

    sql = """DROP TABLE IF EXISTS train"""
    mycursor.execute(sql)

    sql = """DROP TABLE IF EXISTS bus"""
    mycursor.execute(sql)

    _conn.commit()
    print("++++++++++++++++++++++++++++++++++")


def populateTable(_conn):
    print("++++++++++++++++++++++++++++++++++")
    print("Populate table")

    # populate orders table
    c = _conn.cursor()
    sql = (""" 
    INSERT INTO orders (o_orderkey,
    o_custkey,
    o_totalprice,
    o_orderdate,
    o_tripkey
    )
    VALUES ('1', '1', '35.21', '2022-11-20', '3'),
    ('2', '3', '24.07', '2022-11-21', '3'),
    ('3', '29', '28.39', '2022-09-12', '26'),
    ('4', '13', '11.37', '2022-07-05', '22'),
    ('5', '11', '10.30', '2022-10-12', '30'),
    ('6', '5', '22.33', '2022-09-12', '16'),
    ('7', '19', '27.38', '2022-03-05', '18'),
    ('8', '13', '26.16', '2022-08-12', '6'),
    ('9', '28', '35.19', '2022-02-12', '22'),
    ('10', '10', '12.29', '2022-01-11', '24'),
    ('11', '19', '26.15', '2022-04-06', '22'),
    ('12', '13', '22.21', '2022-10-04', '10'),
    ('13', '26', '24.16', '2022-01-11', '1'),
    ('14', '1', '37.21', '2022-02-11', '33'),
    ('15', '3', '26.12', '2022-04-07', '9'),
    ('16', '7', '38.33', '2022-06-03', '19'),
    ('17', '26', '22.19', '2022-05-10', '17'),
    ('18', '7', '34.27', '2022-06-05', '36'),
    ('19', '17', '35.33', '2022-06-08', '35'),
    ('20', '19', '30.27', '2022-04-11', '11'),
    ('21', '15', '21.16', '2022-05-04', '10'),
    ('22', '12', '21.10', '2022-01-02', '43'),
    ('23', '5', '19.36', '2022-09-02', '38'),
    ('24', '29', '32.37', '2022-04-12', '39'),
    ('25', '30', '16.21', '2022-05-01', '22'),
    ('26', '28', '17.33', '2022-03-20', '13'),
    ('27', '30', '28.11', '2022-04-20', '5'),
    ('28', '5', '38.10', '2022-11-07', '7'),
    ('29', '19', '28.13', '2022-08-21', '2'),
    ('30', '23', '12.31', '2022-04-12', '35')
    """)
    c.execute(sql)
    _conn.commit()

    # populate customer table
    c = _conn.cursor()
    sql = (""" 
    INSERT INTO customer (c_custkey,
    c_memberstatus,
    c_memberkey,
    c_name,
    c_address,
    c_phone,
    c_email
    )
    VALUES ('1', 'F', '0', 'Customer#000000001', '532 Asedxcv St', '594-351-2589','lkdhsfu@email.com'),
    ('3', 'F', '0', 'Customer#000000003', '281 Lsaoij Ave', '952-841-6594', 'rjwjrwe@email.com'),
    ('5', 'T', '1', 'Customer#000000005', '132 Ywkarw St', '116-399-3780', 'wejriwjra@email.com'),
    ('7', 'T', '2', 'Customer#000000007', '713 Vasjfd Ct', '941-419-3409', 'aosfjs@email.com'),
    ('10', 'T', '3', 'Customer#000000010', '174 Ukawnfj Ave', '149-248-8581', 'jworjwo@email.com'),
    ('11', 'T', '4', 'Customer#000000011', '606 Rkansds Blvd', '111-740-6754', 'hfsdafkd@email.com'),
    ('12', 'F', '0', 'Customer#000000012', '451 Mjkdafh Ct', '524-886-3612', 'rqwjdqwjid@email.com'),
    ('13', 'F', '0', 'Customer#000000013', '578 Nrhwo St', '242-663-7523', 'hafwow@email.com'),
    ('15', 'T', '5', 'Customer#000000015', '569 Qaskhfd Ave', '532-601-4097', 'mvadlfs@email.com'),
    ('16', 'F', '0', 'Customer#000000016', '56147 Hfijoudrsye Ave', '948-468-4768', 'sdeufhijy@email.com'),
    ('17', 'T', '6', 'Customer#000000017', '963 Tfalsk Ave', '908-779-3096', 'ewajfsa@email.com'),
    ('19', 'F', '0', 'Customer#000000019', '262 Clajwlr Blvd', '721-874-6008', 'vnlafjw@email.com'),
    ('23', 'F', '0', 'Customer#000000023', '249 Xarwjl Ave', '452-528-3349', 'wqkjafaks@email.com'),
    ('26', 'F', '0', 'Customer#000000026', '375 Oasljfd St', '838-848-1164', 'qflwajfa@email.com'),
    ('28', 'T', '7', 'Customer#000000028', '931 Balfjsdl Blvd', '924-133-9470', 'balkjfs@email.com'),
    ('29', 'F', '0', 'Customer#000000029', '513 Fwlejfe Ct', '129-905-3317', 'tpoajsf@email.com'),
    ('30', 'F', '0', 'Customer#000000030', '369 Kdalsd Ave', '876-644-3001', 'riwjroj@email.com')
    """)
    c.execute(sql)
    _conn.commit()

    # populate member table
    c = _conn.cursor()
    sql = (""" 
    INSERT INTO member (m_custkey,
    m_memberkey,
    m_discounts
    )
    VALUES 
    ('5', '1', '-7.25'),
    ('7', '2', '-7.25'),
    ('10', '3', '-7.25'),
    ('11', '4', '-7.25'), 
    ('15', '5', '-7.25'), 
    ('17', '6', '-7.25'), 
    ('28', '7', '-7.25')
    """)
    c.execute(sql)
    _conn.commit()

    # populate seats table
    c = _conn.cursor()
    keys = ('1', '2', '101', '102', '21', '22',
            '201', '202', '31', '32', '301', '302')
    for k in keys:
        for i in range(1, 51):
            if i < 11:
                sql = f"""INSERT INTO seats (s_trainkey,
                s_seatkey,
                s_status,
                s_seattype
                )
                VALUES ({k}, {i}, 'T', 'PREFERRED')"""
                c.execute(sql)
                _conn.commit()
            else:
                sql = f"""INSERT INTO seats (s_trainkey,
                s_seatkey,
                s_status,
                s_seattype
                )
                VALUES ({k}, {i}, 'T', 'GENERAL')"""
                c.execute(sql)
                _conn.commit()

    # populate trip table
    c = _conn.cursor()
    sql = (""" 
    INSERT INTO trip (t_tripkey,
    t_orderkey,
    t_trainkey, 
    t_buskey,
    t_seatkey,
    t_stationkey)
    VALUES ('3', '1', '22','0','5','3,1'),
    ('3', '2', '101','0','10','12,10'),
    ('26', '3', '1','0','35','12,10'),
    ('22', '4', '302','0','28','28,22'),
    ('30', '5', '32','0','14','28,19'),
    ('16', '6', '202','0','19','3,17'),
    ('18', '7', '32','0','26','20,25'),
    ('6', '8', '21','0','8','6,7'),
    ('22', '9', '2','0','29','10,5'),
    ('24', '10', '102','0','11','2,5'),
    ('22', '11', '302','0','18','17,25'),
    ('10', '12', '1','0','17','5,10'),
    ('1', '13', '22','0','16','9,15'),
    ('33', '14', '301','0','23','22,17'),
    ('9', '15', '0','64','44','42,16'),
    ('19', '16', '1','0','27','12,2'),
    ('17', '17', '31','0','11','20,26'), 
    ('36', '18', '201','0','29','1,4'),
    ('35', '19', '0','55','44','14,34'),  
    ('11', '20', '102','0','1','10,13'),
    ('10', '21', '101','0','3','5,10'),
    ('43', '22', '0','4','48','12,33'),   
    ('38', '23', '201','0','18','6,17'),
    ('39', '24', '302','0','16','25,30'),
    ('22', '25', '302','0','28','26,27'),
    ('13', '26', '301','0','30','19,21'),
    ('5', '27', '2','0','22','10,5'),
    ('7', '28', '1','0','14','12,10'),
    ('2', '29', '0','11','41','14,31'),  
    ('35', '30', '21','0','7','11,7')
    """)
    c.execute(sql)
    _conn.commit()

    # populate location table
    # San Joaquin / Pacific Surfliner
    c = _conn.cursor()
    sql = (""" 
    INSERT INTO location (l_stationkey, l_city, l_buskey, l_trainkey)
    VALUES ('1', 'MERCED', '0', '21,22,201,202'),
    ('2', 'ANTIOCH', '0', '1,2,101,102'),
    ('3', 'BAKERSFIELD', '1,51,7,57', '21,22,201,202'),
    ('4', 'CORCORAN', '0', '21,22,201,202'),
    ('5', 'EMERYVILLE', '0', '1,2,101,102'),
    ('6', 'FRESNO', '0', '21,22,201,202'),
    ('7', 'HANFORD', '0', '21,22,201,202'),
    ('8', 'LODI', '0', '21,22,201,202'),
    ('9', 'MADERA', '0', '21,22,201,202'),
    ('10', 'MARTINEZ', '0', '1,2,101,102'),
    ('11', 'MODESTO', '0', '21,22,201,202'),
    ('12', 'OAKLAND', '4,54,9,59', '1,2,101,102'),
    ('13', 'RICHMOND', '0', '1,2,101,102'),
    ('14', 'SACRAMENTO', '11,61,62,5,55,6,56', '21,22,201,202'),
    ('15', 'STOCKTON', '0', '21,22,201,202,1,2,101,102'),
    ('16', 'TURLOCK-DENAIR', '14,64', '21,22,201,202'),
    ('17', 'WASCO', '0', '21,22,201,202'),
    
    ('18', 'ANAHEIM', '0', '31,32,301,302'),
    ('19', 'BURBANK', '0', '31,32,301,302'),
    ('20', 'FULLERTON', '10,60,71,81,13,63', '31,32,301,302'),
    ('21', 'GLENDALE', '0', '31,32,301,302'),
    ('22', 'IRVINE', '0', '31,32,301,302'),
    ('23', 'LOS ANGELES', '1,51', '31,32,301,302'),
    ('24', 'NORTHRIDGE', '15,65', '31,32,301,302'),
    ('25', 'OCEANSIDE', '0', '31,32,301,302'),
    ('26', 'OXNARD', '0', '31,32,301,302'),
    ('27', 'SANTA ANA', '0', '31,32,301,302'),
    ('28', 'SANTA BARBARA', '0', '31,32,301,302'),
    ('29', 'SAN DIEGO', '0', '31,32,301,302'),
    ('30', 'SAN LUIS OBISPO', '0', '31,32,301,302'),
    ('31', 'VENTURA', '0', '31,32,301,302'),
    
    ('32', 'AUBURN', '11,61,62', '0'),
    ('33', 'BARSTOW', '16,66', '0'),
    ('34', 'BERKELEY', '4,54', '0'),
    ('35', 'CHICO', '5,55', '0'),
    ('36', 'DAVIS', '6,56', '0'),
    ('37', 'FREMONT', '9,59', '0'),
    ('38', 'HAYWARD', '9,59', '0'),
    ('39', 'ONTARIO', '10,60', '0'),
    ('40', 'RIVERSIDE', '71,81', '0'),
    ('41', 'ROSEVILLE', '11,61,62', '0'),
    ('42', 'SAN BERNARDINO', '13,63,16,66', '0'),
    ('43', 'SAN JOSE', '14,64', '0'),
    ('44', 'SIMI VALLEY', '15,65', '0'),
    ('45', 'VICTORVILLE', '16,66', '0')
    """)

    c.execute(sql)
    _conn.commit()

    # populate train table // 1 is southbound 2 is northbound
    c = _conn.cursor()
    sql = (""" 
    INSERT INTO train (tn_trainkey,
    tn_stationkey,
    tn_departurecity,
    tn_departure,
    tn_arrivalcity,
    tn_arrival
    )
    VALUES ('1', '12', 'OAKLAND', '06:00', 'EMERYVILLE', '06:05'),
    ('1', '5', 'EMERYVILLE', '06:10', 'RICHMOND', '06:15'),
    ('1', '13', 'RICHMOND', '06:20', 'MARTINEZ', '06:45'),
    ('1', '10', 'MARTINEZ', '06:50', 'ANTIOCH', '07:15'),
    ('1', '2', 'ANTIOCH', '07:20', 'STOCKTON', '07:50'),
    ('2', '15', 'STOCKTON', '13:00', 'ANTIOCH', '13:25'),
    ('2', '2', 'ANTIOCH', '13:30', 'MARTINEZ', '13:55'),
    ('2', '10', 'MARTINEZ', '14:00', 'RICHMOND', '14:05'),
    ('2', '13', 'RICHMOND', '14:10', 'EMERYVILLE', '14:15'),
    ('2', '5', 'EMERYVILLE', '14:20', 'OAKLAND', '14:30'),
    ('101', '12', 'OAKLAND', '11:00', 'EMERYVILLE', '11:05'),
    ('101', '5', 'EMERYVILLE', '11:10', 'RICHMOND', '11:15'),
    ('101', '13', 'RICHMOND', '11:20', 'MARTINEZ', '11:45'),
    ('101', '10', 'MARTINEZ', '11:50', 'ANTIOCH', '12:15'),
    ('101', '2', 'ANTIOCH', '12:20', 'STOCKTON', '12:50'),
    ('102', '15', 'STOCKTON', '19:00', 'ANTIOCH', '19:25'),
    ('102', '2', 'ANTIOCH', '19:30', 'MARTINEZ', '19:55'),
    ('102', '10', 'MARTINEZ', '20:00', 'RICHMOND', '20:05'),
    ('102', '13', 'RICHMOND', '20:10', 'EMERYVILLE', '20:15'),
    ('102', '5', 'EMERYVILLE', '20:20', 'OAKLAND', '20:30'),
    ('21', '14', 'SACRAMENTO', '07:30', 'LODI', '07:55'),
    ('21', '8', 'LODI', '08:00', 'STOCKTON', '08:15'),
    ('21', '15', 'STOCKTON', '08:20', 'MODESTO', '08:45'),
    ('21', '11', 'MODESTO', '08:50', 'TURLOCK-DENAIR', '09:05'),
    ('21', '16', 'TURLOCK-DENAIR', '09:10', 'MERCED', '09:35'),
    ('21', '1', 'MERCED', '09:40', 'MADERA', '10:05'),
    ('21', '9', 'MADERA', '10:10', 'FRESNO', '10:35'),
    ('21', '6', 'FRESNO', '10:40', 'HANFORD', '11:15'),
    ('21', '7', 'HANFORD', '11:20', 'CORCORAN', '11:35'),
    ('21', '4', 'CORCORAN', '11:40', 'WASCO', '12:15'),
    ('21', '17', 'WASCO', '12:20', 'BAKERSFIELD', '12:50'),
    ('22', '3', 'BAKERSFIELD', '16:00', 'WASCO', '16:25'),
    ('22', '17', 'WASCO', '16:30', 'CORCORAN', '17:05'),
    ('22', '4', 'CORCORAN', '17:10', 'HANFORD', '17:25'),
    ('22', '7', 'HANFORD', '17:30', 'FRESNO', '18:05'),
    ('22', '6', 'FRESNO', '18:10', 'MADERA', '18:35'),
    ('22', '9', 'MADERA', '18:40', 'MERCED', '19:05'),
    ('22', '1', 'MERCED', '19:10', 'TURLOCK-DENAIR', '19:35'),
    ('22', '16', 'TURLOCK-DENAIR', '19:40', 'MODESTO', '20:15'),
    ('22', '11', 'MODESTO', '20:20', 'STOCKTON', '20:45'),
    ('22', '15', 'STOCKTON', '20:50', 'LODI', '21:05'),
    ('22', '8', 'LODI', '21:10','SACRAMENTO', '21:40'),
    ('201', '14', 'SACRAMENTO', '12:30', 'LODI', '12:55'),
    ('201', '8', 'LODI', '13:00', 'STOCKTON', '13:15'),
    ('201', '15', 'STOCKTON', '13:20', 'MODESTO', '13:45'),
    ('201', '11', 'MODESTO', '13:50', 'TURLOCK-DENAIR', '14:05'),
    ('201', '16', 'TURLOCK-DENAIR', '14:10', 'MERCED', '14:35'),
    ('201', '1', 'MERCED', '14:40', 'MADERA', '15:05'),
    ('201', '9', 'MADERA', '15:10', 'FRESNO', '15:35'),
    ('201', '6', 'FRESNO', '15:40', 'HANFORD', '16:15'),
    ('201', '7', 'HANFORD', '16:20', 'CORCORAN', '16:35'),
    ('201', '4', 'CORCORAN', '16:40', 'WASCO', '17:15'),
    ('201', '17', 'WASCO', '17:20', 'BAKERSFIELD', '17:50'),
    ('202', '3', 'BAKERSFIELD', '22:00', 'WASCO', '22:25'),
    ('202', '17', 'WASCO', '22:30', 'CORCORAN', '23:05'),
    ('202', '4', 'CORCORAN', '23:10', 'HANFORD', '23:25'),
    ('202', '7', 'HANFORD', '23:30', 'FRESNO', '00:05'),
    ('202', '6', 'FRESNO', '00:10', 'MADERA', '00:35'),
    ('202', '9', 'MADERA', '00:40', 'MERCED', '01:05'),
    ('202', '1', 'MERCED', '01:10', 'TURLOCK-DENAIR', '01:35'),
    ('202', '16', 'TURLOCK-DENAIR', '01:40', 'MODESTO', '02:15'),
    ('202', '11', 'MODESTO', '02:20', 'STOCKTON', '02:45'),
    ('202', '15', 'STOCKTON', '02:50', 'LODI', '03:05'),
    ('202', '8', 'LODI', '03:10','SACRAMENTO', '03:40'),
    ('31', '30', 'SAN LUIS OBISPO', '06:00', 'SANTA BARBARA', '08:45'),
    ('31', '28', 'SANTA BARBARA', '08:50', 'VENTURA', '09:35'),
    ('31', '31', 'VENTURA', '09:40', 'OXNARD', '09:55'),
    ('31', '26', 'OXNARD', '10:00', 'NORTHRIDGE', '11:55'),
    ('31', '24', 'NORTHRIDGE', '12:00', 'BURBANK', '13:05'),
    ('31', '19', 'BURBANK', '13:10', 'GLENDALE', '13:15'),
    ('31', '21', 'GLENDALE', '13:20', 'LOS ANGELES', '13:35'),
    ('31', '23', 'LOS ANGELES', '13:40', 'FULLERTON', '14:05'),
    ('31', '20', 'FULLERTON', '14:10', 'ANAHEIM', '14:15'),
    ('31', '18', 'ANAHEIM', '14:20', 'SANTA ANA', '14:25'),
    ('31', '27', 'SANTA ANA', '14:30', 'IRVINE', '14:40'),
    ('31', '22', 'IRVINE', '14:45', 'OCEANSIDE', '15:40'),
    ('31', '25', 'OCEANSIDE', '15:45', 'SAN DIEGO', '16:45'),
    ('32', '29', 'SAN DIEGO', '19:00', 'OCEANSIDE', '20:00'),
    ('32', '25', 'OCEANSIDE', '20:05', 'IRVINE', '21:00'),
    ('32', '22', 'IRVINE', '21:05', 'SANTA ANA', '21:15'),
    ('32', '27', 'SANTA ANA', '21:20', 'ANAHEIM', '21:25'),
    ('32', '18', 'ANAHEIM', '21:30', 'FULLERTON', '21:35'),
    ('32', '20', 'FULLERTON', '21:40', 'LOS ANGELES', '22:05'),
    ('32', '23', 'LOS ANGELES', '22:10', 'GLENDALE', '22:25'),
    ('32', '21', 'GLENDALE', '22:30', 'BURBANK', '22:35'),
    ('32', '19', 'BURBANK', '22:40', 'NORTHRIDGE', '22:45'),
    ('32', '24', 'NORTHRIDGE', '23:50', 'OXNARD', '23:45'),
    ('32', '26', 'OXNARD', '01:50', 'VENTURA', '01:45'),
    ('32', '31', 'VENTURA', '02:10', 'SANTA BARBARA', '02:55'),
    ('32', '28', 'SANTA BARBARA', '03:00', 'SAN LUIS OBISPO', '05:50'),
    ('301', '30', 'SAN LUIS OBISPO', '16:00', 'SANTA BARBARA', '18:45'),
    ('301', '28', 'SANTA BARBARA', '18:50', 'VENTURA', '19:35'),
    ('301', '31', 'VENTURA', '19:40', 'OXNARD', '19:55'),
    ('301', '26', 'OXNARD', '20:00', 'NORTHRIDGE', '21:55'),
    ('301', '24', 'NORTHRIDGE', '22:00', 'BURBANK', '22:05'),
    ('301', '19', 'BURBANK', '22:10', 'GLENDALE', '22:05'),
    ('301', '21', 'GLENDALE', '22:20', 'LOS ANGELES', '22:35'),
    ('301', '23', 'LOS ANGELES', '22:40', 'FULLERTON', '23:05'),
    ('301', '20', 'FULLERTON', '23:10', 'ANAHEIM', '23:15'),
    ('301', '18', 'ANAHEIM', '23:20', 'SANTA ANA', '23:25'),
    ('301', '27', 'SANTA ANA', '23:30', 'IRVINE', '23:40'),
    ('301', '22', 'IRVINE', '23:45', 'OCEANSIDE', '00:40'),
    ('301', '25', 'OCEANSIDE', '00:45', 'SAN DIEGO', '01:45'),
    ('302', '29', 'SAN DIEGO', '04:00', 'OCEANSIDE', '05:00'),
    ('302', '25', 'OCEANSIDE', '05:05', 'IRVINE', '06:00'),
    ('302', '22', 'IRVINE', '06:05', 'SANTA ANA', '06:15'),
    ('302', '27', 'SANTA ANA', '06:20', 'ANAHEIM', '06:25'),
    ('302', '18', 'ANAHEIM', '06:30', 'FULLERTON', '06:35'),
    ('302', '20', 'FULLERTON', '06:40', 'LOS ANGELES', '07:05'),
    ('302', '23', 'LOS ANGELES', '07:10', 'GLENDALE', '07:25'),
    ('302', '21', 'GLENDALE', '07:30', 'BURBANK', '07:35'),
    ('302', '19', 'BURBANK', '07:40', 'NORTHRIDGE', '08:45'),
    ('302', '24', 'NORTHRIDGE', '08:50', 'OXNARD', '10:45'),
    ('302', '26', 'OXNARD', '10:50', 'VENTURA', '11:05'),
    ('302', '31', 'VENTURA', '11:10', 'SANTA BARBARA', '11:55'),
    ('302', '28', 'SANTA BARBARA', '12:00', 'SAN LUIS OBISPO', '14:50')
    """)
    c.execute(sql)
    _conn.commit()

    # populate bus table
    c = _conn.cursor()
    sql = (""" 
    INSERT INTO bus (b_buskey,
    b_stationkey,
    b_departurecity,
    b_departure,
    b_arrivalcity,
    b_arrival
    )
    VALUES ('11', '14', 'SACRAMENTO', '05:00', 'ROSEVILLE', '05:30'),
    ('11', '41', 'ROSEVILLE', '05:35', 'AUBURN', '05:55'),
    ('11', '32', 'AUBURN', '06:25', 'ROSEVILLE', '06:45'),
    ('11', '41', 'ROSEVILLE', '06:50', 'SACRAMENTO', '07:20'),
    ('61', '32', 'AUBURN', '11:00', 'ROSEVILLE', '11:30'),
    ('61', '41', 'ROSEVILLE', '11:50', 'SACRAMENTO', '12:20'),
    ('62', '14', 'SACRAMENTO', '22:00', 'ROSEVILLE', '22:30'),
    ('62', '41', 'ROSEVILLE', '22:35', 'AUBURN', '22:55'),
    
    ('5', '14', 'SACRAMENTO', '04:00', 'CHICO', '05:30'),
    ('5', '35', 'CHICO', '05:55', 'SACRAMENTO', '07:25'),
    ('55', '14', 'SACRAMENTO', '22:00', 'CHICO', '23:30'),
    ('55', '35', 'CHICO', '10:40', 'SACRAMENTO', '12:10'),
    ('6', '14', 'SACRAMENTO', '06:00', 'DAVIS', '06:20'),
    ('6', '36', 'DAVIS', '07:00', 'SACRAMENTO', '07:20'),
    ('56', '14', 'SACRAMENTO', '22:00', 'DAVIS', '22:20'),
    ('56', '36', 'DAVIS', '11:40', 'SACRAMENTO', '12:00'),
    ('4', '12', 'OAKLAND', '15:00', 'BERKELEY', '15:10'),
    ('4', '34', 'BERKELEY', '16:10', 'OAKLAND', '16:20'),
    ('54', '12', 'OAKLAND', '21:00', 'BERKELEY', '21:10'),
    ('54', '34', 'BERKELEY', '04:10', 'OAKLAND', '04:20'),
    ('9', '12', 'OAKLAND', '07:55', 'HAYWARD', '08:15'),
    ('9', '38', 'HAYWARD', '08:20', 'FREMONT', '08:40'),
    ('9', '37', 'FREMONT', '09:30', 'HAYWARD', '09:50'),
    ('9', '38', 'HAYWARD', '10:10', 'OAKLAND', '10:30'),
    ('59', '12', 'OAKLAND', '20:50', 'HAYWARD', '21:10'),
    ('59', '38', 'HAYWARD', '21:20', 'FREMONT', '21:40'),
    ('59', '37', 'FREMONT', '22:00', 'HAYWARD', '22:20'),
    ('59', '38', 'HAYWARD', '22:40', 'OAKLAND', '23:00'),
    ('14', '16', 'TURLOCK-DENAIR', '09:30', 'SAN JOSE', '11:40'),
    ('14', '43', 'SAN JOSE', '12:10', 'TURLOCK-DENAIR', '14:20'),
    ('64', '16', 'TURLOCK-DENAIR', '02:00', 'SAN JOSE', '04:10'),
    ('64', '43', 'SAN JOSE', '06:00', 'TURLOCK-DENAIR', '08:10'),
    ('1', '3', 'BAKERSFIELD', '13:20', 'LOS ANGELES', '15:20'),
    ('1', '23', 'LOS ANGELES', '19:00', 'BAKERSFIELD', '21:00'),
    ('51', '3', 'BAKERSFIELD', '16:00', 'LOS ANGELES', '18:00'),
    ('51', '23', 'LOS ANGELES', '08:00', 'BAKERSFIELD', '10:00'),
    ('15', '24', 'NORTHRIDGE', '12:00', 'SIMI VALLEY', '12:25'),
    ('15', '44', 'SIMI VALLEY', '18:00', 'NORTHRIDGE', '18:25'),
    ('65', '24', 'NORTHRIDGE', '00:00', 'SIMI VALLEY', '00:25'),
    ('65', '44', 'SIMI VALLEY', '07:00', 'NORTHRIDGE', '07:25'),
    ('10', '20', 'FULLERTON', '14:20', 'ONTARIO', '14:50'),
    ('10', '39', 'ONTARIO', '15:00', 'FULLERTON', '15:30'),
    ('60', '20', 'FULLERTON', '07:00', 'ONTARIO', '07:30'),
    ('60', '39', 'ONTARIO', '11:00', 'FULLERTON', '11:30'),
    ('81', '20', 'FULLERTON', '07:00', 'RIVERSIDE', '07:40'),
    ('81', '40', 'RIVERSIDE', '15:00', 'FULLERTON', '15:40'),
    ('71', '20', 'FULLERTON', '23:30', 'RIVERSIDE', '00:10'),
    ('71', '40', 'RIVERSIDE', '05:00', 'FULLERTON', '05:40'),
    ('13', '20', 'FULLERTON', '07:00', 'SAN BERNARDINO', '07:50'),
    ('13', '42', 'SAN BERNARDINO', '13:10', 'FULLERTON', '14:00'),
    ('63', '20', 'FULLERTON', '22:00', 'SAN BERNARDINO', '22:50'),
    ('63', '42', 'SAN BERNARDINO', '03:00', 'FULLERTON', '03:50'),
    ('16', '42', 'SAN BERNARDINO', '08:00', 'VICTORVILLE', '08:50'),
    ('16', '45', 'VICTORVILLE', '09:00', 'BARSTOW', '09:30'),
    ('16', '33', 'BARSTOW', '11:00', 'VICTORVILLE', '11:30'),
    ('16', '45', 'VICTORVILLE', '12:00', 'SAN BERNARDINO', '12:50'),
    ('66', '42', 'SAN BERNARDINO', '23:10', 'VICTORVILLE', '00:00'),
    ('66', '45', 'VICTORVILLE', '00:10', 'BARSTOW', '00:40'),
    ('66', '33', 'BARSTOW', '00:50', 'VICTORVILLE', '01:00'),
    ('66', '45', 'VICTORVILLE', '01:10', 'SAN BERNARDINO', '02:00')
    """)
    c.execute(sql)
    _conn.commit()

    print("++++++++++++++++++++++++++++++++++")

# gets all customer keys
def getCustKey(_conn):
    c = _conn.cursor()
    customers = []
    sql = ("""
    SELECT c_custkey
    FROM customer
    """)
    c.execute(sql)
    results = c.fetchall()
    for row in results:
        customers.append(row[0])
    return customers

# gets all member customer keys
def getMemberCustKey(_conn):
    c = _conn.cursor()
    members = []
    sql = ("""
    SELECT m_custkey
    FROM member
    """)
    c.execute(sql)
    results = c.fetchall()
    for row in results:
        members.append(row[0])
    return members

# show all station locations
def printCity(_conn):
    c = _conn.cursor()
    cities = []
    sql = (""" 
    select l_stationkey, l_city 
    from location 
    order by l_city
    """)
    c.execute(sql)
    _conn.commit()
    results = c.fetchall()
    for row in results:
        cities.append(row)
    return cities

# gets newest member key
def getmaxMemberCustKey(_conn):
    c = _conn.cursor()
    sql = (""" 
    SELECT max(m_memberkey)
    FROM member
    """)
    c.execute(sql)
    results = c.fetchone()
    member = results
    return member

# Customer wants to change their member status from non-member to member
def insertMember(_conn, custkey, memkey):
    c = _conn.cursor()
    sql = (f""" 
    DROP TRIGGER IF EXISTS im;
    """)
    c.execute(sql)
    sql = (f""" 
    CREATE TRIGGER im AFTER INSERT ON member
    FOR EACH ROW
    BEGIN
        UPDATE customer
        SET c_memberstatus = 'T'
        WHERE c_custkey  = {custkey};
    END;
    """)
    c.execute(sql)
    c.execute(f""" 
    INSERT INTO member (m_custkey,
    m_memberkey,
    m_discounts) 
    VALUES ({custkey}, {memkey}, '-7.25')
    """)
    _conn.commit()
    

def deleteMember(_conn, custkey):
    c = _conn.cursor()
    sql = (f""" 
    DROP TRIGGER IF EXISTS dm;
    """)
    c.execute(sql)
    sql = (f""" 
    CREATE TRIGGER dm AFTER DELETE ON member
    FOR EACH ROW
    BEGIN
        UPDATE customer
        SET c_memberstatus = 'F'
        WHERE c_custkey  = {custkey};
    END;
    """)
    c.execute(sql)
    c.execute(f""" 
    DELETE FROM member 
    WHERE m_custkey = {custkey}
    """)
    _conn.commit()

date_now = time.localtime(time.time())
# customer makes an order then corresponding trip is made
def insertOrder(_conn, c_key, td, ta, s_key, price):
    c = _conn.cursor()
    sql = (f""" 
    DROP TRIGGER IF EXISTS ii;
    """)
    c.execute(sql)

    # insert into trip(tripkey, orderkey, trainkey, buskey, seatkey, stationkey)
    # select o_tripkey, orderkey, 0 , 0, seat_key (possible fill in where we check if a seat is "T" and "s_type matches their status" , stationkey)

    sql = (f""" 
    CREATE TRIGGER ii AFTER INSERT ON orders
    FOR EACH ROW
    BEGIN
        INSERT INTO trip(t_tripkey,t_orderkey,t_trainkey,t_buskey,t_seatkey,t_stationkey)
        SELECT o_tripkey, o_orderkey,0,0,{s_key}, '{td},{ta}'
        FROM orders
        WHERE o_orderkey = NEW.o_orderkey;

        UPDATE trip
        SET t_trainkey = 
            CASE WHEN t_tripkey = '3' THEN '101'
                WHEN t_tripkey = '17' THEN '31'
            ELSE '0'
            END,
            t_buskey = 
            CASE WHEN t_tripkey = '3' THEN '0'
                WHEN t_tripkey = '17' THEN '0'
            ELSE '0'
            END
        WHERE t_orderkey = NEW.o_orderkey;
    END;
    """)
    c.execute(sql)

    sql = (f""" 
        INSERT INTO orders(o_orderkey, o_custkey, o_totalprice, o_orderdate, o_tripkey)
        SELECT max(o_orderkey) + 1, '{c_key}','{price}', '{time.strftime("%Y-%m-%d", date_now)}','0' FROM orders
    """)
    c.execute(sql)
    _conn.commit()


# customer cancels an order then corresponding trip also deleted
def deleteOrder(_conn, o_key):
    c = _conn.cursor()
    sql = (""" 
    DROP TRIGGER IF EXISTS dd;
    """)
    c.execute(sql)
    sql = (""" 
    CREATE TRIGGER dd AFTER DELETE ON orders
    FOR EACH ROW
    BEGIN
        DELETE FROM trip
        WHERE t_orderkey NOT IN (SELECT o_orderkey FROM orders);
    END;
    """)
    c.execute(sql)
    sql = (f""" 
    DELETE FROM orders
    WHERE o_orderkey = {o_key}
    """)
    c.execute(sql)
    _conn.commit()


# show all customer's orders 
def showCustOrders(_conn, custkey): 
    c = _conn.cursor()
    sql = (f""" 
    SELECT o_orderkey, t_tripkey, t_trainkey, t_seatkey, o_totalprice, o_orderdate
    FROM orders, trip
    WHERE o_custkey = {custkey} AND o_orderkey = t_orderkey
    ORDER BY o_orderdate DESC
    """)
    c.execute(sql)
    results = c.fetchall()
    orderlist = []
    for row in results:
        orderlist.append(row)
    
    return orderlist


# show all general train seats
def showSeats(_conn, train_num):
    c = _conn.cursor()
    sql = (f"""
    SELECT s_seatkey, s_seattype
    FROM seats
    WHERE s_trainkey = (SELECT tn_trainkey FROM train WHERE tn_stationkey = {train_num}) AND s_status = 'T' AND s_seattype = "GENERAL"
    """)
    c.execute(sql)
    results = c.fetchall()
    seats = []
    for row in results:
        seats.append(row[0])
        print(row[0], row[1])
    return seats

# show all preferred and general train seats
def showPrefSeats(_conn, train_num):
    c = _conn.cursor()
    sql = (f"""
    SELECT s_seatkey, s_seattype
    FROM seats
    WHERE s_trainkey = (SELECT tn_trainkey FROM train WHERE tn_stationkey = {train_num}) AND s_status = 'T'
    """)
    c.execute(sql)
    results = c.fetchall()
    seats = []
    for row in results:
        seats.append(row[0])
        print(row[0], row[1])
    return seats

# get all general train seats
def getSeats(_conn, train_num):
    c = _conn.cursor()
    sql = (f"""
    SELECT s_seatkey, s_seattype
    FROM seats
    WHERE s_trainkey = (SELECT tn_trainkey FROM train WHERE tn_stationkey = {train_num}) AND s_status = 'T' AND s_seattype = "GENERAL"
    """)
    c.execute(sql)
    results = c.fetchall()
    seats = []
    for row in results:
        seats.append(row)
    return seats

# get all preferred and general train seats
def getPrefSeats(_conn, train_num):
    c = _conn.cursor()
    sql = (f"""
    SELECT s_seatkey, s_seattype
    FROM seats
    WHERE s_trainkey = (SELECT tn_trainkey FROM train WHERE tn_stationkey = {train_num}) AND s_status = 'T'
    """)
    c.execute(sql)
    results = c.fetchall()
    seats = []
    for row in results:
        seats.append(row)
    return seats

# updates available seats based on orders made
def updateTripSeat(_conn, o_key, s_key, t_key):
    c = _conn.cursor()
    sql = (""" 
    DROP TRIGGER IF EXISTS us;
    """)
    c.execute(sql)
    sql = (f""" 
    CREATE TRIGGER us AFTER UPDATE ON trip
    FOR EACH ROW
    BEGIN
        UPDATE seats
        SET s_status = 'F'
        WHERE s_seatkey = '{s_key}' AND s_trainkey = '{t_key}';
    END;
    """)
    c.execute(sql)
    sql = (f""" 
    UPDATE trip 
    SET t_seatkey = '{s_key}'
    WHERE t_orderkey = '{o_key}';
    """)
    c.execute(sql)
    _conn.commit()


# gets all locations based on departure city

# for choosing train trip
def tripTrainDepart(_conn, trip_departure):
    c = _conn.cursor()
    sql = (f""" 
        SELECT l_trainkey
        FROM location
        WHERE l_stationkey = {trip_departure} 
        """)
    c.execute(sql)
    station_key = c.fetchall()
    x = station_key[0][0]
    x = x.split(",")

    cities = []

    for i in x:
        sql = (f""" 
            SELECT DISTINCT tn_stationkey, tn_departurecity
            FROM train
            WHERE tn_trainkey = {i}
            """)
        c.execute(sql)
        results = c.fetchall()
    
        for row in results:
            # skip departure city, print avail stations
            if row[0] == int(trip_departure):
                pass
            elif row in cities:
                pass
            else:
                cities.append(row)

    return cities

# for choosing bus trip
def tripBusDepart(_conn, trip_departure):
    c = _conn.cursor()
    sql = (f""" 
        SELECT l_buskey
        FROM location
        WHERE l_stationkey = {trip_departure} 
        """)
    c.execute(sql)
    station_key = c.fetchall()
    x = station_key[0][0]

    x = x.split(",")

    cities = []
    
    for i in x:
        sql = (f""" 
            SELECT DISTINCT b_stationkey, b_departurecity
            FROM bus
            WHERE b_buskey = '{i}'
            """)
        c.execute(sql)
        results = c.fetchall()

        for row in results:
            # skip departure city, print avail stations
            if row[0] == int(trip_departure):
                pass
            elif row in cities:
                pass
            else:
                cities.append(row)

    return cities

# get station key of user input city num
def getArrCity(_conn, location_num):
    c = _conn.cursor()
    city = []
    cities = []
    sql = (f""" 
    SELECT DISTINCT tn_trainkey
        FROM train
        WHERE tn_departurecity = (SELECT l_city FROM location WHERE l_stationkey = {location_num})
        OR tn_arrivalcity = (SELECT l_city FROM location WHERE l_stationkey = {location_num})
    """)
    c.execute(sql)
    tl = c.fetchall()

    for row in tl:
        # skip departure city, print avail stations
        if row[0] == int(location_num):
            pass
        else:
            city.append(row[0])
    
    sql = (f""" 
    SELECT DISTINCT b_buskey
        FROM bus
        WHERE b_departurecity = (SELECT l_city FROM location WHERE l_stationkey = {location_num})
        OR b_arrivalcity = (SELECT l_city FROM location WHERE l_stationkey = {location_num})
    """)
    c.execute(sql)
    bl = c.fetchall()

    for row in bl:
        # skip departure city, print avail stations
        if row[0] == int(location_num):
            pass
        else:
            city.append(row[0])


    for i in city:
        sql = (f""" 
        SELECT DISTINCT tn_stationkey, tn_departurecity
            FROM train
            WHERE tn_trainkey = {i}
        """)
        c.execute(sql)
        results = c.fetchall()
        for row in results:
            # skip departure city, print avail stations
            if row[0] == int(location_num):
                pass
            elif row[0] in cities:
                pass
            else:
                cities.append(row[0])

    for i in city:
        sql = (f""" 
        SELECT DISTINCT b_stationkey, b_departurecity
            FROM bus
            WHERE b_buskey = {i}
        """)
        c.execute(sql)
        results = c.fetchall()
        for row in results:
            # skip departure city, print avail stations
            if row[0] == int(location_num):
                pass
            elif row[0] in cities:
                pass
            else:
                cities.append(row[0])

    return cities

        



# get city name of user input city num
def cityName(_conn, location_num):
    c = _conn.cursor()
    sql = (f""" 
    SELECT l_city
    FROM location
    WHERE l_stationkey = {location_num} 
    """)
    c.execute(sql)

    results = c.fetchall()
    for row in results:
        city = row

    return city[0]

def showTrainStations(_conn, train):
    c = _conn.cursor()
    sql = (f"""
    SELECT tn_stationkey, tn_departurecity
    FROM train 
    WHERE tn_trainkey = {train}
    """)
    c.execute(sql)
    results = c.fetchall()
    for row in results:
        print(row[0], row[1])

def showAllTrainStations(_conn):
    c = _conn.cursor()
    sql = (f"""
    SELECT DISTINCT tn_trainkey
    FROM train
    """)
    c.execute(sql)
    results = c.fetchall()
    trains = []
    for row in results:
        
        trains.append(row[0])

    print(trains)
        
    return trains

def showTrainStations(_conn, train):
    c = _conn.cursor()
    sql = (f"""
    SELECT tn_stationkey, tn_departurecity
    FROM train 
    WHERE tn_trainkey = {train}
    """)
    c.execute(sql)
    results = c.fetchall()
    trains = []
    for row in results:
        print(row[0], row[1])
        trains.append(row[0])
    return trains


def computeTime(time_now, trackTrain): 

    # start time
    cust_time = time_now        # start time 
    train_time = trackTrain     # end time 

    # convert time string to datetime
    t1 = datetime.strptime(cust_time, "%H:%M")
    # print('Start time:', t1.time())

    t2 = datetime.strptime(train_time, "%H:%M")
    # print('End time:', t2.time())

    # # get difference
    delta = t2 - t1
    sec = delta.total_seconds()
    min = sec / 60

    # print("Difference in minutes: ", min)
    # print("Difference in hours: ", hours)
    # print("delta ", delta)
    s = str(delta)
    x = []
    x = s.split(",")
    # print("x " , x)
    # print("x delta ", x[0])
    if delta < timedelta(0):
        delta = -delta 
    # print("- to + delta ", delta)
    return delta 


def trackTrainArrival(_conn, train, departureCity, time_now):
    c = _conn.cursor()
    sql = (f""" 
    SELECT DISTINCT tn_departure
    FROM train
    WHERE tn_trainkey = {train} AND
    tn_departurecity LIKE '%{departureCity}%'
    """)
    c.execute(sql)
    results = c.fetchall()
    x = []
    for row in results:
        x = (row[0])
    # calculate time difference
    timediff = computeTime(time_now, x)
    print("\nYour train leaves in " + str(timediff) + "\n")


def trackBusArrival(_conn, bus, departureCity, time_now):
    c = _conn.cursor()
    sql = (f""" 
    SELECT DISTINCT b_departure
    FROM bus
    WHERE b_buskey = {bus} AND
    b_departurecity LIKE '%{departureCity}%'
    """)
    c.execute(sql)
    results = c.fetchall()
    x = []
    for row in results:
        x = (row[0])

    # calculate time difference 
    timediff = computeTime(time_now, x)
    print("\nYour bus leaves in " + str(timediff) + "\n")


def showAllBusStations(_conn):
    c = _conn.cursor()
    sql = (f"""
    SELECT DISTINCT b_buskey
    FROM bus
    """)
    c.execute(sql)
    results = c.fetchall()
    buses = []
    for row in results:
        buses.append(row[0])
    print(buses)
    return buses

def showBusStations(_conn, bus):
    c = _conn.cursor()
    sql = (f"""
    SELECT b_stationkey, b_departurecity
    FROM bus 
    WHERE b_buskey = {bus}
    """)
    c.execute(sql)
    results = c.fetchall()
    buses = []
    for row in results:
        print(row[0], row[1])
        buses.append(row[0])
    return buses
    
# insert customer when they make first order or create an account
def insertCustomer(_conn):
    c = _conn.cursor()
    sql = (""" 
    INSERT INTO customer (c_custkey,
    c_memberstatus,
    c_memberkey, 
    c_name,
    c_address,
    c_phone,
    c_email)
    VALUES ('31', 'F', '8','Customer#000000031','9745 Baskhjdf Dr','398-741-5156','iopsdfanu@email.com')
    """)
    c.execute(sql)
    _conn.commit()

    sql = (""" 
    SELECT *
    FROM customer
    WHERE c_custkey = '31'
    """)
    c.execute(sql)
    results = c.execute(sql)

    file = open("16.out", "w")
    file.write("{:>10} {:<20} {:>20} {:>30} {:>30} {:>20} {:>30}\n" .format("c_custkey",
                                                                            "c_memberstatus", "c_memberkey", "c_name", "c_address", "c_phone", "c_email"))

    for row in results:
        file.write("{:>10} {:<20} {:>20} {:>30} {:>30} {:>20} {:>30}\n"
                   .format(row[0], row[1], row[2], row[3], row[4], row[5], row[5]))

    file.close()


# show all bus information
def showBus(_conn):
    c = _conn.cursor()
    sql = ("""
    SELECT * 
    FROM bus
    ORDER BY b_buskey 
    """)
    c.execute(sql)
    results = c.fetchall()

    file = open("3.out", "w")
    file.write("{:>10} {:<20} {:>20} {:>20} {:>20} {:>20}\n" .format("b_buskey",
                                                                     "b_stationkey", "b_departurecity", "b_departure",
                                                                     "b_arrivalcity", "b_arrival"))

    for row in results:
        file.write("{:>10} {:<20} {:>20} {:>20} {:>20} {:>20}\n" .format(
            row[0], row[1], row[2], row[3], row[4], row[5]))

    file.close()

# show all station locations
def showLocations(_conn):
    c = _conn.cursor()
    sql = """
    SELECT * 
    FROM location 
    ORDER BY l_city 
    """
    c.execute(sql)
    results = c.fetchall()

    file = open("4.out", "w")
    file.write("{:>10} {:<20} {:>20} {:>20}\n" .format("l_buskey",
                                                       "l_city", "l_stationkey", "l_trainkey"))

    for row in results:
        file.write("{:>10} {:<20} {:>20} {:>20}\n" .format(
            row[0], row[1], row[2], row[3]))

    file.close()





# update seat as unavailable when customer makes order/trip with that seat


def updateSeats(_conn):
    c = _conn.cursor()
    sql = """
    SELECT t_trainkey, t_seatkey 
    FROM trip
    """
    c.execute(sql)
    results = c.fetchall()
    for i in results:
        sql = (f"""
        UPDATE seats
        SET s_status = 'F'
        WHERE seats.s_trainkey = {i[0]} AND seats.s_seatkey = {i[1]}
        """)
        c.execute(sql)
        _conn.commit()

    sql = """
    SELECT * 
    FROM seats
    WHERE s_status = 'T'
    """
    c.execute(sql)
    results = c.fetchall()

    file = open("7.out", "w")
    file.write("{:>10} {:<10} {:>10} {:>20}\n" .format("s_trainkey",
                                                       "s_seatkey", "s_status", "s_seattype"))

    for row in results:
        file.write("{:>10} {:<10} {:>10} {:>20}\n" .format(
            row[0], row[1], row[2], row[3]))

    file.close()

# show customer existing trip information
# def cust_info(_conn):
#     c = _conn.cursor()
#     sql = ("""
#     SELECT *
#     FROM trip
#     """)
#     c.execute(sql)
#     _conn.commit()

#     results = c.fetchall()
#     file = open("24.out", "w")
#     file.write("{:>10} {:>10} {:>10} {:>10} {:>10} {:>10}\n" .format("tripkey", "orderkey", "trainkey", "buskey", "seatkey", "stationkey"))

#     for row in results:
#         file.write("{:>10} {:>10} {:>10} {:>10} {:>10} {:>10}\n" .format(row[0], row[1], row[2], row[3], row[4], row[5]))

#     file.close()


def main():
    database = r"tpch.sqlite"

    # create a database connection
    conn = openConnection(database)
    with conn:
        dropTable(conn)
        createTable(conn)
        populateTable(conn)
        updateSeats(conn)
        

    closeConnection(conn, database)


if __name__ == '__main__':
    main()
