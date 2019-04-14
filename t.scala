
**connect  terminal with  

- root   - pwd  cloudera

**copy  hive-site.xml to  spark/conf 

cp /etc/hive/conf.dist/hive-site.xml /user/lib/spark/conf


**spark-shell


scala> import org.apache.spark.sql.hive.HiveContext

scala> val hc = new HiveContext(sc)


scala> hc.sql( "select * from olympics_all").show()


scala> val result1 = hc.sql( "select name, count(medal) as count_medail FROM olympics_all group by name order by count_medail DESC")


sex  m and year 2016

val result1 = hc.sql( "select name,sex,year, count(medal) as count_medail FROM olympics_all where sex= 'M' and year =2016 group by  name, year, sex order by count_medail DESC limit 5")


sex f and year 2016




---------------------------------


val result1 = hc.sql( "select name, count(medal) as count_medail FROM olympics_all group by name order by count_medail DESC limit 5")
result1: org.apache.spark.sql.DataFrame = [name: string, count_medail: bigint]

scala> result1.show()
+--------------------+------------+                                             
|                name|count_medail|
+--------------------+------------+
|Heikki Ilmari Sav...|          39|
|Joseph "Josy" Sto...|          38|
| Ioannis Theofilakis|          36|
|Alexandros Theofi...|          32|
|      Andreas Wecker|          32|
+--------------------+------------+


scala> val result1 = hc.sql( "select name, year,count(medal) as count_medail FROM olympics_all where year =2016 group by name, year order by count_medail DESC limit 5")
result1: org.apache.spark.sql.DataFrame = [name: string, year: string, count_medail: bigint]

scala> result1.show
+--------------------+----+------------+                                        
|                name|year|count_medail|
+--------------------+----+------------+
|Oleh Yuriyovych V...|2016|           8|
|Srgio Yoshio Sasa...|2016|           8|
|Nikita Vladimirov...|2016|           8|
|Arthur Nory Oyaka...|2016|           7|
|       Julien Gobaux|2016|           7|
+--------------------+----+------------+





scala> val result1 = hc.sql( "select name,sex,year, count(medal) as count_medail FROM olympics_all where sex= 'M' and year =2016 group by  name, year, sex order by count_medail DESC limit 5")
result1: org.apache.spark.sql.DataFrame = [name: string, sex: string, year: string, count_medail: bigint]

scala> result1.show
+--------------------+---+----+------------+                                    
|                name|sex|year|count_medail|
+--------------------+---+----+------------+
|Nikita Vladimirov...|  M|2016|           8|
|Srgio Yoshio Sasa...|  M|2016|           8|
|Oleh Yuriyovych V...|  M|2016|           8|
|          Eddy Yusof|  M|2016|           7|
|        Bart Deurloo|  M|2016|           7|
+--------------------+---+----+------------+


scala> val result1 = hc.sql( "select name,sex,year, count(medal) as count_medail FROM olympics_all where sex= 'F' and year =2016 group by  name, year, sex order by count_medail DESC limit 5").show()
+--------------------+---+----+------------+                                    
|                name|sex|year|count_medail|
+--------------------+---+----+------------+
|Sarah Frederica S...|  F|2016|           7|
|         Rikako Ikee|  F|2016|           7|
|Simone Arianne Biles|  F|2016|           6|
|Seda Gurgenovna T...|  F|2016|           6|
|         Zsfia Kovcs|  F|2016|           6|
+--------------------+---+----+------------+

result1: Unit = ()


scala> val result1 = hc.sql( "select  avg(age) FROM olympics_all" )
result1: org.apache.spark.sql.DataFrame = [_c0: double]

scala> result1.show()
+------------------+
|               _c0|
+------------------+
|25.461703398336084|
+------------------+



val result1 = hc.sql( "select team, count( medal) as total_medal  FROM olympics_all group by team order by total_medal DESC")
result1: org.apache.spark.sql.DataFrame = [team: string, total_medal: bigint]

scala> result1.show()
+-------------+-----------+                                                     
|         team|total_medal|
+-------------+-----------+
|United States|      16527|
|       France|      11127|
|Great Britain|      10506|
|        Italy|       9539|
|      Germany|       8580|
|       Canada|       8528|
|        Japan|       7487|
|       Sweden|       7297|
|    Australia|       7030|
|      Hungary|       6207|
|       Poland|       5583|
|  Switzerland|       5510|
|  Netherlands|       5450|
| Soviet Union|       5138|
|        Spain|       4887|
|        China|       4820|
|      Finland|       4661|
|       Russia|       4539|
|      Austria|       4392|
|       Norway|       4282|
+-------------+-----------+
only showing top 20 rows
