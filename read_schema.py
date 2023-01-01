# encoding: utf-8
import pymysql
import json

class Database():

    def __init__(self):
        self.conn = pymysql.connect(
            host="localhost",
            user="root",
            passwd="TestUser_20221222",
            database="goods",
            port=3306,
            charset='utf8')

    def execute_sql(self, sql):
        fail = 1
        cur = self.conn.cursor()
        i = 0
        cnt = 3
        while fail == 1 and i < cnt:
            try:
                fail = 0
                cur.execute(sql)
            except BaseException:
                fail = 1
            res = []
            if fail == 0:
                res = cur.fetchall()
            i = i + 1
        if fail == 1:
            # print("SQL Execution Fatal!!", sql)
            return 0, ''
        elif fail == 0:
            return 1, res

    def mysql_get_tables(self):
        success, res = self.execute_sql(
            "select table_name from information_schema.tables where table_schema = (select database()) order by create_time desc;")
        tables = []
        if success == 1:
            for i in res:
                tables.append(i[0])
            return tables
        else:
            print("*******************mysql_get_tables执行失败")
            return tables

    def mysql_get_columns(self, table_name):
        success, res = self.execute_sql(
            "select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS where TABLE_NAME='{}';".format(table_name))
        columns = []
        column_names = []
        if success == 1:
           for i in res:
               if i[0] not in column_names:
                   column_names.append(i[0])
                   columns.append({'name': i[0], 'type': i[1]})
           return columns
        else:
            print("*******************mysql_get_columns执行失败")
            return columns

    def mysql_get_rows(self, table_name):
        success, res = self.execute_sql(
            "select count(*) from {};".format(table_name))
        if success == 1:
            return res[0][0]
        else:
            print("*******************mysql_get_rows执行失败")
            return 0


if __name__ == '__main__':
    db = Database()

    tables = db.mysql_get_tables()
    result_list = []
    for table_name in tables:
        rows = db.mysql_get_rows(table_name)
        map = {
            'table': table_name,
            'rows': rows,
            'columns': []
        }
        columns = db.mysql_get_columns(table_name)
        map['columns'] = columns
        result_list.append(map)
    json_file = open('schema.json', 'w+')
    json_file.write(json.dumps(result_list))
    json_file.close()
    print("获取成功,写入schema.json文件")
