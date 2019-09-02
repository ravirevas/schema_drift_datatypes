from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import HiveContext
spark = SparkSession.builder \
    .master("local") \
    .appName("jdbc data sources") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.warehouse.dir",'/user/hive/warehouse/') \
    .config("hive.metastore.uris",'thrift://127.0.0.1:9083') \
    .config("spark.sql.hive.convertMetastoreParquet","false") \
    .enableHiveSupport() \
    .getOrCreate()


spark.catalog.refreshTable("sparktable3")
spark.sql("describe sparktable3").show()

#spark.sql("set -v").show(1000,False)
df = spark.read.format("csv").option("header", "true").option("mergeSchema", "true").option("inferSchema", "true").load("/home/cloudera/csv_test.csv")
#df.show()
#df.printSchema()
#df.write.format("parquet").save("/home/cloudera/parquet_files/csv_test_3.parquet")
#parquetFile = spark.read.option("inferSchema","false").option("mergeSchema","true").parquet("/home/cloudera/parquet_files/csv_test.parquet/","/home/cloudera/parquet_files/csv_test_2.parquet/")
#print(parquetFile.count())
#parquetFile.printSchema()
#parquetFile.show(30,False)
#parquetFile.createOrReplaceTempView("parquetFiles")
#teenagers = spark.sql("select * from parquetFiles")
#teenagers.show(30,False)
#parquetFile_3 = spark.read.option("inferSchema","True").parquet("/home/cloudera/parquet_files/csv_test_3.parquet/")
#parquetFile_3.printSchema()
#parquetFile_3.show()
#data_df = parquetFile_3.withColumn("new_column", parquetFile_3["new_column"].cast(IntegerType()))
#data_df.printSchema()
#data_df.show()
#a=data_df.schema.fields
#print(type(a))
#b=(str(a[0])).split(",")
#print(str(a).replace("StructField",""))
df.write.format("parquet").save("/home/cloudera/parquethiveserde")


#file_format_rdd=spark.sql("describe formatted par_test").rdd.map(lambda x:(x[0],x[1])).filter(lambda y:y[0]=="InputFormat")

  #file_format=file_format_rdd.map(lambda x:x[1]).first()
  #print(file_format)

  #file_location_rdd = spark.sql("describe formatted test_table_text").rdd.map(lambda x:(x[0],x[1])).filter(lambda y:y[0]=="Location")
  #file_location = file_location_rdd.map(lambda x:x[1]).first()
  #print(file_location)

  #if "Text" in file_format:
   # print("ITS A TEXT FILE")
    #file_format="textfile"
  #elif "parquet" in file_format:
    #file_format="parquetfile"
    #print("parquet file")

 #####################getting file format ends##############

  #print("#####################ALTERING TABLE###############")
  #data_type_changed_rdd_var=data_type_changed_rdd.collect()
  #print(data_type_changed_rdd_var[0][0])
  #print(data_type_changed_rdd_var[0][1][0])



#
 # table_name="test_table_text"
  #alter_stmt = "drop table {}".format(table_name)
  #print(alter_stmt)
  #add_col_new=new_column_rdd.collect()
  #for i in add_col_new :
   #    if(i[1][0] is None):
    #       col=i[0]+" "+i[1][1]
     #      altercommd="ALTER TABLE TEST_TABLE_TEXT ADD COLUMNS ({})".format(col)
      #     #spark.sql(altercommd)
       #    print(altercommd)
#
#
 # cast_stmt=data_type_changed_rdd.collect()
  #cast_list=[]
  #for i in cast_stmt:
   #   if(i[1][0] !=None and i[1][1]!=None):
    #      col="cast("+i[0]+" as "+i[1][1]+")"
     #    cast_list.append(col)
      ##
  ##select_stmt=rdd_old_new_schema.collect()
  #select_stmt_list=[]
  #for i in select_stmt:
   #   if(i[1][0]==i[1][1] or i[1][1] is None):
    #     select_stmt_list.append(i[0])

#print("Create external temp_table")
#spark.sql("create table dummy as select cast(id as int),name,address,boss,day,done from test_table_text limit 1")
#full_select_stmt="create table dummy as select "+str(cast_list+select_stmt_list).replace("[","").replace("]","").replace("'","")+" from {} limit 1".format(table_name)
#print("full select stmt")
#print(full_select_stmt)
#spark.sql(full_select_stmt)
#print("Dropping table")
#spark.sql("drop table test_table_text")
#last_table="CREATE EXTERNAL TABLE TEST_TABLE_TEXT LIKE dummy Location 'hdfs://quickstart.cloudera:8020/user/cloudera/test_table_text'"
#spark.sql(last_table)

#subprocess.call("hive -e 'ALTER TABLE sparktable3 change id id double' ", shell=True)



#s=varchar(25)
#int(re.search(r'\d+', string1).group())










