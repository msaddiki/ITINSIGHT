spark-shell

hive context in spark :

import org.apache.spark.sql.hive.HiveContext

val hc = new HiveContext(sc)

val df = hc.read.option("header","true").table("olympics_all")

val df_select = df.select("name","age","sex","event","medal","year","noc")

+--------------------+---+---+--------------------+------+----+---+
|                name|age|sex|               event| medal|year|noc|
+--------------------+---+---+--------------------+------+----+---+
|           A Dijiang| 24|  M|Basketball Men's ...|    NA|1992|CHN|
|Einar Ferdinand "...| 26|  M|Swimming Men's 40...|    NA|1952|FIN|
|    Oszkr Abay-Nemes| 22|  M|Swimming Men's 10...|    NA|1936|HUN|
|    Oszkr Abay-Nemes| 22|  M|Swimming Men's 4 ...|Bronze|1936|HUN|
|   Dainius Adomaitis| 26|  M|Basketball Men's ...|Bronze|2000|LTU|
|         Gusztv Bene| 36|  M|Boxing Men's Welt...|    NA|1948|HUN|
|           Lszl Bene| NA|  M|Boxing Men's Heav...|    NA|1952|HUN|
|    Mrton Zoltn Bene| 23|  M|Alpine Skiing Men...|    NA|2010|HUN|
|    Mrton Zoltn Bene| 23|  M|Alpine Skiing Men...|    NA|2010|HUN|
|        Mihaela Bene| 23|  F|Canoeing Women's ...|    NA|1996|ROU|
|         Lino Benech| 24|  M|Cycling Men's 100...|    NA|1972|URU|
|Anna Beneck (-Mor...| 18|  F|Swimming Women's ...|    NA|1960|ITA|
|Anna Beneck (-Mor...| 18|  F|Swimming Women's ...|    NA|1960|ITA|
|Daniela Beneck (-...| 14|  F|Swimming Women's ...|    NA|1960|ITA|
|Daniela Beneck (-...| 18|  F|Swimming Women's ...|    NA|1964|ITA|
|Daniela Beneck (-...| 18|  F|Swimming Women's ...|    NA|1964|ITA|
|Daniela Beneck (-...| 18|  F|Swimming Women's ...|    NA|1964|ITA|
|   Christina Benecke| 25|  F|Volleyball Women'...|    NA|2000|GER|
|   Christina Benecke| 29|  F|Volleyball Women'...|    NA|2004|GER|
|        Emil Benecke| 29|  M|Water Polo Men's ...|  Gold|1928|GER|
+--------------------+---+---+--------------------+------+----+---+
only showing top 20 rows


scala> val df_filter = df_select.filter($"medal" like("Gold"))
df_filter: org.apache.spark.sql.DataFrame = [name: string, age: string, sex: string, event: string, medal: string, year: string, noc: string]

scala> val df_filter = df_select.filter($"medal" like("Gold")).show()
+--------------------+---+---+--------------------+-----+----+---+
|                name|age|sex|               event|medal|year|noc|
+--------------------+---+---+--------------------+-----+----+---+
|        Emil Benecke| 29|  M|Water Polo Men's ...| Gold|1928|GER|
|        Gbor Benedek| 25|  M|Modern Pentathlon...| Gold|1952|HUN|
|       Tibor Benedek| 28|  M|Water Polo Men's ...| Gold|2000|HUN|
|       Tibor Benedek| 32|  M|Water Polo Men's ...| Gold|2004|HUN|
|       Tibor Benedek| 36|  M|Water Polo Men's ...| Gold|2008|HUN|
|Charles Sumner Be...| 40|  M|Shooting Men's Mi...| Gold|1908|USA|
|      Andrea Benelli| 44|  M|Shooting Men's Skeet| Gold|2004|ITA|
|Stephanie Louise ...| 20|  F|Swimming Women's ...| Gold|2008|AUS|
|Stephanie Louise ...| 20|  F|Swimming Women's ...| Gold|2008|AUS|
|Stephanie Louise ...| 20|  F|Swimming Women's ...| Gold|2008|AUS|
|        Michael Rich| 22|  M|Cycling Men's 100...| Gold|1992|GER|
|      Pascal Richard| 32|  M|Cycling Men's Roa...| Gold|1996|SUI|
|Louis Marcel Rich...| 35|  M|Shooting Men's Fr...| Gold|1900|SUI|
|Louis Marcel Rich...| 35|  M|Shooting Men's Fr...| Gold|1900|SUI|
|Louis Marcel Rich...| 41|  M|Shooting Men's Fr...| Gold|1906|SUI|
|Louis Marcel Rich...| 41|  M|Shooting Men's Mi...| Gold|1906|SUI|
|Louis Marcel Rich...| 41|  M|Shooting Men's Mi...| Gold|1906|SUI|
|Alma Wilford Rich...| 22|  M|Athletics Men's H...| Gold|1912|USA|
|Robert Eugene "Bo...| 26|  M|Athletics Men's P...| Gold|1952|USA|
|Robert Eugene "Bo...| 30|  M|Athletics Men's P...| Gold|1956|USA|
+--------------------+---+---+--------------------+-----+----+---+
only showing top 20 rows



scala> val df_group = df_filter.groupBy($"medal",$"name").count()
df_group: org.apache.spark.sql.DataFrame = [medal: string, name: string, count: bigint]

scala> df_group.orderBy($"count".desc)
res36: org.apache.spark.sql.DataFrame = [medal: string, name: string, count: bigint]

scala> df_group.orderBy($"count".desc).show()
+-----+--------------------+-----+                                              
|medal|                name|count|
+-----+--------------------+-----+
| Gold|Michael Fred Phel...|   23|
| Gold|Raymond Clarence ...|   10|
| Gold|Frederick Carlton...|    9|
| Gold|   Mark Andrew Spitz|    9|
| Gold|Larysa Semenivna ...|    9|
| Gold|  Usain St. Leo Bolt|    8|
| Gold|Birgit Fischer-Sc...|    8|
| Gold|Matthew Nicholas ...|    8|
| Gold|Jennifer Elisabet...|    8|
| Gold| Ole Einar Bjrndalen|    8|
| Gold|          Sawao Kato|    8|
| Gold|Vra slavsk (-Odlo...|    7|
| Gold|Borys Anfiyanovyc...|    7|
| Gold|Nikolay Yefimovic...|    7|
| Gold|Aladr Gerevich (-...|    7|
| Gold|Viktor Ivanovych ...|    7|
| Gold|Donald Arthur "Do...|    7|
| Gold|Allyson Michelle ...|    6|
| Gold|           Viktor An|    6|
| Gold|Lidiya Pavlovna S...|    6|
+-----+--------------------+-----+
only showing top 20 rows


scala> val df_order = df_group.orderBy($"count".desc)
df_order: org.apache.spark.sql.DataFrame = [medal: string, name: string, count: bigint]

scala> df_order.take(5)
res38: Array[org.apache.spark.sql.Row] = Array([Gold,Michael Fred Phelps, II,23], [Gold,Raymond Clarence "Ray" Ewry,10], [Gold,Larysa Semenivna Latynina (Diriy-),9], [Gold,Mark Andrew Spitz,9], [Gold,Frederick Carlton "Carl" Lewis,9])

scala> df_order.take(5).show()
<console>:41: error: value show is not a member of Array[org.apache.spark.sql.Row]
              df_order.take(5).show()
                               ^

scala> df_order.limit(5).show()
+-----+--------------------+-----+                                              
|medal|                name|count|
+-----+--------------------+-----+
| Gold|Michael Fred Phel...|   23|
| Gold|Raymond Clarence ...|   10|
| Gold|Larysa Semenivna ...|    9|
| Gold|   Mark Andrew Spitz|    9|
| Gold|Frederick Carlton...|    9|
+-----+--------------------+-----+
