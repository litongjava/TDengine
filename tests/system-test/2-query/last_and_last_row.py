import datetime
import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import tdDnodes
from math import inf


class TDTestCase:
    def init(self, conn, logSql, replicaVer=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), True)

    def check_explain_res_has_row(self, plan_str_expect: str, rows, sql):
        plan_found = False
        for row in rows:
            if str(row).find(plan_str_expect) >= 0:
                tdLog.debug("plan: [%s] found in: [%s]" % (plan_str_expect, str(row)))
                plan_found = True
                break
        if not plan_found:
            tdLog.exit("plan: %s not found in res: [%s] in sql: %s" % (plan_str_expect, str(rows), sql))

    def check_explain_res_no_row(self, plan_str_not_expect: str, res, sql):
        for row in res:
            if str(row).find(plan_str_not_expect) >= 0:
                tdLog.exit('plan: [%s] found in: [%s] for sql: %s' % (plan_str_not_expect, str(row), sql))

    def explain_sql(self, sql: str):
        sql = "explain " + sql
        tdSql.query(sql, queryTimes=1)
        return tdSql.queryResult

    def explain_and_check_res(self, sqls, hasLastRowScanRes):
        for sql, has_last in zip(sqls, hasLastRowScanRes):
            res = self.explain_sql(sql)
            if has_last == 1:
                self.check_explain_res_has_row("Last Row Scan", res, sql)
            else:
                self.check_explain_res_no_row("Last Row Scan", res, sql)

    def none_model_test(self):
        tdSql.execute("drop database if exists last_test_none_model ;")
        tdSql.execute("create database last_test_none_model cachemodel 'none';")
        tdSql.execute("use last_test_none_model;")
        tdSql.execute("create stable st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table test_t1 using st tags(1);")
        
        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select last_row(ts), last(*)  from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)     
        tdSql.checkData(0, 2, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, None)    
        tdSql.checkData(0, 3, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, maxRange - 1)  
        tdSql.checkData(0, 2, last_ts)    
        tdSql.checkData(0, 3, maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_no_row("Last Row Scan", explain_res, sql)

        tdSql.error(f'select last(*), last_row(ts), ts from test_t1;')

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, maxRange + 1) 
        tdSql.checkData(0, 3, None)    
        tdSql.checkData(0, 4, maxRange - 1)
        tdSql.checkData(0, 5, last_ts)  
        tdSql.checkData(0, 6, maxRange - 1)  

        tdSql.execute("drop table if exists test_t1 ;")
        tdSql.execute("drop stable if exists st;")
        tdSql.execute("drop database if exists last_test_none_model;")

    def last_value_model_test(self):
        tdSql.execute("create database last_test_last_value_model cachemodel 'last_value' ;")
        tdSql.execute("use last_test_last_value_model;")
        tdSql.execute("create stable st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table test_t1 using st tags(1);")

        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select last_row(ts), last(*)  from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)     
        tdSql.checkData(0, 2, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, None)    
        tdSql.checkData(0, 3, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, maxRange - 1)  
        tdSql.checkData(0, 2, last_ts)    
        tdSql.checkData(0, 3, maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, maxRange + 1) 
        tdSql.checkData(0, 3, None)    
        tdSql.checkData(0, 4, maxRange - 1)
        tdSql.checkData(0, 5, last_ts)  
        tdSql.checkData(0, 6, maxRange - 1)  

        tdSql.execute("drop table if exists test_t1 ;")
        tdSql.execute("drop stable if exists st;")
        tdSql.execute("drop database if exists last_test_last_value_model;")
    
    def last_row_model_test(self):
        tdSql.execute("create database last_test_last_row_model cachemodel 'last_row';")
        tdSql.execute("use last_test_last_row_model;")
        tdSql.execute("create stable st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table test_t1 using st tags(1);")

        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select last_row(ts), last(*)  from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)     
        tdSql.checkData(0, 2, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, None)    
        tdSql.checkData(0, 3, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, maxRange - 1)  
        tdSql.checkData(0, 2, last_ts)    
        tdSql.checkData(0, 3, maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_has_row("Table Scan", explain_res, sql)

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, maxRange + 1) 
        tdSql.checkData(0, 3, None)    
        tdSql.checkData(0, 4, maxRange - 1)
        tdSql.checkData(0, 5, last_ts)  
        tdSql.checkData(0, 6, maxRange - 1)  

        tdSql.execute("drop table if exists test_t1 ;")
        tdSql.execute("drop stable if exists st;")
        tdSql.execute("drop database if exists last_test_last_row_model;")

    def both_model_test(self):
        tdSql.execute("create database last_test_both_model cachemodel 'both';")
        tdSql.execute("use last_test_both_model;")
        tdSql.execute("create stable st(ts timestamp, id int) tags(tid int);")
        tdSql.execute("create table test_t1 using st tags(1);")

        maxRange = 100
        # 2023-11-13 00:00:00.000
        startTs = 1699804800000
        for i in range(maxRange):
            insertSqlString = "insert into test_t1 values(%d, %d);" % (startTs + i, i)
            tdSql.execute(insertSqlString)
        
        last_ts = startTs + maxRange
        tdSql.execute("insert into test_t1 (ts) values(%d)" % (last_ts))
        sql = f'select last_row(ts), last(*)  from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)     
        tdSql.checkData(0, 2, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_no_row("Table Scan", explain_res, sql)
        
        sql = f'select last_row(ts), last(ts), last_row(id), last(id) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, None)    
        tdSql.checkData(0, 3, maxRange - 1)  

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)
        self.check_explain_res_no_row("Table Scan", explain_res, sql)

        sql = f'select last(*), last_row(ts), count(*) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, maxRange - 1)  
        tdSql.checkData(0, 2, last_ts)    
        tdSql.checkData(0, 3, maxRange + 1)

        explain_res = self.explain_sql(sql)
        self.check_explain_res_has_row("Last Row Scan", explain_res, sql)

        sql = f'select last_row(ts), last(ts), count(*) , last_row(id), last(id), last(*) from test_t1;'
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, last_ts)     
        tdSql.checkData(0, 1, last_ts)  
        tdSql.checkData(0, 2, maxRange + 1) 
        tdSql.checkData(0, 3, None)    
        tdSql.checkData(0, 4, maxRange - 1)
        tdSql.checkData(0, 5, last_ts)  
        tdSql.checkData(0, 6, maxRange - 1)  

        tdSql.error(f'select last(*), last_row(ts), ts from test_t4;')

        tdSql.execute("drop table if exists test_t1 ;")
        tdSql.execute("drop stable if exists st;")
        tdSql.execute("drop database if exists last_test_both_model;")

    def run(self):
        self.none_model_test()

        self.last_value_model_test()

        self.last_row_model_test()

        self.both_model_test()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
