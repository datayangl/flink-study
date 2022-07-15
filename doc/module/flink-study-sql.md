- [Join 场景](#join-场景)
  - [regular join](#regular-join)
  - [lookup join](#lookup-join)
  - [interval-join](#interval-join)

# Join 场景
主要的 join 场景示例：
* regular join
* lookup join
* interval join


## regular join
语法：
```sql
SELECT columns
FROM t1  [AS <alias1>]
[LEFT/INNER/FULL OUTER] JOIN t2
ON t1.column1 = t2.key-name1
```

相关 sql：
```sql
CREATE TABLE click (
 userid int,
 clickid int,
 item string,
 proctime AS PROCTIME()
) WITH (
 'connector' = 'datagen',
 'rows-per-second'='100',
 'fields.userid.kind'='random',
 'fields.userid.min'='1',
 'fields.userid.max'='100',
 'fields.clickid.kind'='random',
 'fields.clickid.min'='1',
 'fields.clickid.max'='100',
 'fields.item.length'='1')
CREATE TABLE orders (
 orderid int,
 userid int,
 item string,
 proctime AS PROCTIME()
) WITH (
 'connector' = 'datagen',
 'rows-per-second'='100',
 'fields.orderid.kind'='random',
 'fields.orderid.min'='1',
 'fields.orderid.max'='100',
 'fields.userid.kind'='random',
 'fields.userid.min'='1',
 'fields.userid.max'='100',
 'fields.item.length'='1')

SELECT * FROM click LEFT JOIN orders ON click.userid = orders.userid
```
执行计划：
```text
== Abstract Syntax Tree ==
LogicalProject(userid=[$0], clickid=[$1], item=[$2], proctime=[$3], orderid=[$4], userid0=[$5], item0=[$6], proctime0=[$7])
+- LogicalJoin(condition=[=($0, $5)], joinType=[left])
   :- LogicalProject(userid=[$0], clickid=[$1], item=[$2], proctime=[PROCTIME()])
   :  +- LogicalTableScan(table=[[default_catalog, default_database, click]])
   +- LogicalProject(orderid=[$0], userid=[$1], item=[$2], proctime=[PROCTIME()])
      +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Calc(select=[userid, clickid, item, PROCTIME_MATERIALIZE(proctime) AS proctime, orderid, userid0, item0, PROCTIME_MATERIALIZE(proctime0) AS proctime0])
+- Join(joinType=[LeftOuterJoin], where=[=(userid, userid0)], select=[userid, clickid, item, proctime, orderid, userid0, item0, proctime0], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])
   :- Exchange(distribution=[hash[userid]])
   :  +- Calc(select=[userid, clickid, item, PROCTIME() AS proctime])
   :     +- TableSourceScan(table=[[default_catalog, default_database, click]], fields=[userid, clickid, item])
   +- Exchange(distribution=[hash[userid]])
      +- Calc(select=[orderid, userid, item, PROCTIME() AS proctime])
         +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[orderid, userid, item])

== Optimized Execution Plan ==
Calc(select=[userid, clickid, item, PROCTIME_MATERIALIZE(proctime) AS proctime, orderid, userid0, item0, PROCTIME_MATERIALIZE(proctime0) AS proctime0])
+- Join(joinType=[LeftOuterJoin], where=[(userid = userid0)], select=[userid, clickid, item, proctime, orderid, userid0, item0, proctime0], leftInputSpec=[NoUniqueKey], rightInputSpec=[NoUniqueKey])
   :- Exchange(distribution=[hash[userid]])
   :  +- Calc(select=[userid, clickid, item, PROCTIME() AS proctime])
   :     +- TableSourceScan(table=[[default_catalog, default_database, click]], fields=[userid, clickid, item])
   +- Exchange(distribution=[hash[userid]])
      +- Calc(select=[orderid, userid, item, PROCTIME() AS proctime])
         +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[orderid, userid, item])
```

## lookup join
相关 sql：
```sql
CREATE TABLE click (
 userid int,
 clickid int,
 item string,
 proctime AS PROCTIME()
) WITH (
 'connector' = 'datagen',
 'rows-per-second'='100',
 'fields.userid.kind'='random',
 'fields.userid.min'='1',
 'fields.userid.max'='100',
 'fields.clickid.kind'='random',
 'fields.clickid.min'='1',
 'fields.clickid.max'='100',
 'fields.item.length'='1'
)

CREATE TABLE users (
  id int,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/test',
   'table-name' = 'user',
   'username' = 'root',
   'password' = 'root'
)

SELECT * FROM click LEFT JOIN users FOR SYSTEM_TIME AS OF click.proctime ON click.userid = users.id

```
执行计划如下：
```text
== Abstract Syntax Tree ==
LogicalProject(userid=[$0], clickid=[$1], item=[$2], proctime=[$3], id=[$4], name=[$5])
+- LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{0, 3}])
   :- LogicalProject(userid=[$0], clickid=[$1], item=[$2], proctime=[PROCTIME()])
   :  +- LogicalTableScan(table=[[default_catalog, default_database, click]])
   +- LogicalFilter(condition=[=($cor0.userid, $0)])
      +- LogicalSnapshot(period=[$cor0.proctime])
         +- LogicalTableScan(table=[[default_catalog, default_database, users]])

== Optimized Physical Plan ==
Calc(select=[userid, clickid, item, PROCTIME_MATERIALIZE(proctime) AS proctime, id, name])
+- LookupJoin(table=[default_catalog.default_database.users], joinType=[LeftOuterJoin], async=[false], lookup=[id=userid], select=[userid, clickid, item, proctime, id, name])
   +- Calc(select=[userid, clickid, item, PROCTIME() AS proctime])
      +- TableSourceScan(table=[[default_catalog, default_database, click]], fields=[userid, clickid, item])

== Optimized Execution Plan ==
Calc(select=[userid, clickid, item, PROCTIME_MATERIALIZE(proctime) AS proctime, id, name])
+- LookupJoin(table=[default_catalog.default_database.users], joinType=[LeftOuterJoin], async=[false], lookup=[id=userid], select=[userid, clickid, item, proctime, id, name])
   +- Calc(select=[userid, clickid, item, PROCTIME() AS proctime])
      +- TableSourceScan(table=[[default_catalog, default_database, click]], fields=[userid, clickid, item])
```

## interval-join
语法：
```sql
-- 写法1
SELECT columns
FROM t1  [AS <alias1>]
[LEFT/INNER/FULL OUTER] JOIN t2
ON t1.column1 = t2.key-name1 AND t1.timestamp BETWEEN t2.timestamp  AND  BETWEEN t2.timestamp + + INTERVAL '10' MINUTE;

-- 写法2
SELECT columns
FROM t1  [AS <alias1>]
[LEFT/INNER/FULL OUTER] JOIN t2
ON t1.column1 = t2.key-name1 AND t2.timestamp <= t1.timestamp and t1.timestamp <=  t2.timestamp + + INTERVAL ’10' MINUTE ;
```

相关sql：
```sql
CREATE TABLE click (
 userid int,
 clickid int,
 item string,
 proctime AS PROCTIME()
) WITH (
 'connector' = 'datagen',
 'rows-per-second'='100',
 'fields.userid.kind'='random',
 'fields.userid.min'='1',
 'fields.userid.max'='100',
 'fields.clickid.kind'='random',
 'fields.clickid.min'='1',
 'fields.clickid.max'='100',
 'fields.item.length'='1'
)

SELECT * FROM click LEFT JOIN orders ON click.userid = orders.userid and click.proctime >= orders.proctime and  click.proctime < orders.proctime + INTERVAL '10' MINUTE"
```

执行计划：
```text
== Abstract Syntax Tree ==
LogicalProject(userid=[$0], clickid=[$1], item=[$2], proctime=[$3], orderid=[$4], userid0=[$5], item0=[$6], proctime0=[$7])
+- LogicalJoin(condition=[AND(=($0, $5), >=($3, $7), <($3, +($7, 600000:INTERVAL MINUTE)))], joinType=[left])
   :- LogicalProject(userid=[$0], clickid=[$1], item=[$2], proctime=[PROCTIME()])
   :  +- LogicalTableScan(table=[[default_catalog, default_database, click]])
   +- LogicalProject(orderid=[$0], userid=[$1], item=[$2], proctime=[PROCTIME()])
      +- LogicalTableScan(table=[[default_catalog, default_database, orders]])

== Optimized Physical Plan ==
Calc(select=[userid, clickid, item, PROCTIME_MATERIALIZE(proctime) AS proctime, orderid, userid0, item0, PROCTIME_MATERIALIZE(proctime0) AS proctime0])
+- IntervalJoin(joinType=[LeftOuterJoin], windowBounds=[isRowTime=false, leftLowerBound=0, leftUpperBound=599999, leftTimeIndex=3, rightTimeIndex=3], where=[AND(=(userid, userid0), >=(proctime, proctime0), <(proctime, +(proctime0, 600000:INTERVAL MINUTE)))], select=[userid, clickid, item, proctime, orderid, userid0, item0, proctime0])
   :- Exchange(distribution=[hash[userid]])
   :  +- Calc(select=[userid, clickid, item, PROCTIME() AS proctime])
   :     +- TableSourceScan(table=[[default_catalog, default_database, click]], fields=[userid, clickid, item])
   +- Exchange(distribution=[hash[userid]])
      +- Calc(select=[orderid, userid, item, PROCTIME() AS proctime])
         +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[orderid, userid, item])

== Optimized Execution Plan ==
Calc(select=[userid, clickid, item, PROCTIME_MATERIALIZE(proctime) AS proctime, orderid, userid0, item0, PROCTIME_MATERIALIZE(proctime0) AS proctime0])
+- IntervalJoin(joinType=[LeftOuterJoin], windowBounds=[isRowTime=false, leftLowerBound=0, leftUpperBound=599999, leftTimeIndex=3, rightTimeIndex=3], where=[((userid = userid0) AND (proctime >= proctime0) AND (proctime < (proctime0 + 600000:INTERVAL MINUTE)))], select=[userid, clickid, item, proctime, orderid, userid0, item0, proctime0])
   :- Exchange(distribution=[hash[userid]])
   :  +- Calc(select=[userid, clickid, item, PROCTIME() AS proctime])
   :     +- TableSourceScan(table=[[default_catalog, default_database, click]], fields=[userid, clickid, item])
   +- Exchange(distribution=[hash[userid]])
      +- Calc(select=[orderid, userid, item, PROCTIME() AS proctime])
         +- TableSourceScan(table=[[default_catalog, default_database, orders]], fields=[orderid, userid, item])
```