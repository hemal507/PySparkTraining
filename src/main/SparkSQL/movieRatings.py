from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import pyspark.sql.functions as fn

sc = SparkContext()
spark = SparkSession.builder.appName("Movie Ratings").getOrCreate()

rdd1 = sc.textFile("../resources/movies.dat")
rdd2 = sc.textFile("../resources/ratings.dat")

mv = rdd1.map(lambda x: x.split("::")).map(lambda x: Row(mid=int(x[0]), name=x[1], genre=x[2] )).toDF()
rt = rdd2.map(lambda x: x.split("::")).map(lambda x: Row(uid=x[0], rmid=int(x[1]), rating=int(x[2]), tmp=x[3])).toDF()


# using SQL API
#rt.filter(rt.rating == 5).join(mv, mv.mid == rt.rmid).createOrReplaceTempView("joined")
#spark.sql("select distinct(mid), genre from joined where mid in (select mid from joined group by mid having count(*) >= 1000 )") .show()


# DataFrame API
#grp=rt.filter(rt.rating == 5).join(mv, mv.mid == rt.rmid).groupBy(mv.mid)
#df2 = grp.agg(fn.count(mv.mid).alias('counts'))
#df2.filter(df2.counts > 1000).join(mv, mv.mid == df2.mid).show()

grp1 = rt.groupBy(rt.rmid)
df3 = grp1.agg(fn.count(rt.rmid).alias('counts'))
spark.conf.set("spark.sql.join.preferSortMergeJoin","false")
print('Sort Merge join preference ',spark.conf.get("spark.sql.join.preferSortMergeJoin"))
#df4 = df3.filter(df3.counts > 1000).join(fn.broadcast(mv), mv.mid == df3.rmid)
df4 = df3.filter(df3.counts > 1000).join(mv, mv.mid == df3.rmid)
df4.explain()
df4.select(df4.genre).show()
