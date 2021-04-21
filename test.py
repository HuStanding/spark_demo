from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, struct, to_json
from pyspark.sql.types import *

if __name__ == "__main__":
    app_name = 'test@liuziyu004'
    spark = SparkSession\
            .builder\
            .appName("PythonWordCount")\
            .master("yarn")\
            .getOrCreate()
    #spark = SparkSession.builder.enableHiveSupport().appName(app_name).getOrCreate()
    sc=spark.sparkContext
    """
    test1 = []
    test1.append((1,'a', '15'))
    test1.append((2,'a', '20'))
    test1.append((3,'b', '30'))
    test1.append((4,'b', '25'))
    df1 = spark.createDataFrame(test1,\
            ['resblock_id','bizcircle_id','trans_price1'])

    test2 = []
    test2.append((1,'a'))
    test2.append((1,'b'))
    test2.append((3,'b'))
    test2.append((4,'b'))
    df2= spark.createDataFrame(test2,\
            ['resblock_id','trans_price2'])

    join_df=df1.join(df2,['resblock_id'],how='left')
    join_df.show()
    """
    test = []
    test.append((1,'a',123))
    test.append((1,'b',234))
    test.append((2,'a',345))
    test.append((2,'b',456))
    df = spark.createDataFrame(test,\
            ['id','role','value'])

    df1 = df.filter(df['role'] == 'a')
    df2 = df.filter(df['role'] == 'b')
    #df1 = df1.drop('role')
    df1 = df1.withColumnRenamed("value","agent")
    #df2 = df2.drop('role')
    df2 = df2.withColumnRenamed("value","cust")
    cols = df1.columns
    columns = ["agent","role"]
    def add(*x):
        res = ""
        for item in x:
            res += str(item)
        return res
        

    add_col = udf(add,StringType())
    #df1 = df1.withColumn("new_col",add_col(*["id", "agent","role"]))
    df1 = df1.withColumn("agent",to_json(struct(["id","role"])))
    df1.show()
    df_new = df1.join(df2,['id'],how='left')
    df_new.show()
    """
    #求平均值
    df3=df1.groupBy('bizcircle_id')\
        .agg(mean('trans_price1').alias("mean1"),\
        mean('resblock_id').alias("mean2"))
    df3.show()

    # 根据数值新增列数据
    df4=df1\
        .withColumn('level',\
        when((df1['trans_price1']<'22') & (df1['trans_price1']>'0'),'1')\
        .when((df1['trans_price1']>'22') & (df1['trans_price1']<='30'), '2'))
    df4.show()
    # 按照某一列去重
    df5=df1.dropDuplicates(['bizcircle_id'])
    df5.show()

    #重命名列
    df6=df1.withColumnRenamed('trans_price1','trans_price')
    df6.show()
    """
