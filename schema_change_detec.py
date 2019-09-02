from pyspark.sql import SparkSession
from support import *
import subprocess
from pyspark.sql.types import *
from utils import *
from config.app_config import UatConfig
from config import  logging_conf
from config import *
import os


log_file_path = os.path.join(os.sep, UatConfig.log_dir, UatConfig.log_file)
logger = logging_conf.configure_logger(UatConfig.logger, log_file_path)
spark=get_spark_session("myjob")
#log_file_path = "/home/cloudera/log/wdl_ingest.log"
#logger = logging_conf.configure_logger("wdl", log_file_path)
logger.info("logger started here")
logger.debug("logger started here")
logger.error("logger started here")
#spark.sql("set -v").show(1000,False)
#spark.sql("ALTER TABLE sparktable3 REPLACE COLUMNS (id  double) CASCADE")

df_new_file = spark.read.format("csv").option("header", "true").option("mergeSchema", "true").option("inferSchema",
                                                                                                     "true").load(
    "{}".format(source_file))

print(df_new_file.printSchema())
df_new_file.createOrReplaceTempView("parquetFiles")
rdd_new_file = spark.sql("describe parquetFiles").rdd.map(lambda x: (x[0], x[1]))
print('###################iam here###############')
print(rdd_new_file.collect())
# df.write.mode('append').format('parquet').saveAsTable('sparKtable3')
rdd_old_file = spark.sql("describe PARQUET_DATA_TYPE_TESTING").rdd.map(lambda x: (x[0], x[1]))
logger.info("################NEW FILE SCHEMA################")
logger.info(rdd_new_file.collect())
logger.info("################OLD FILE SCHEMA################")
logger.info(rdd_old_file.collect())
rdd_old_new_schema = rdd_old_file.fullOuterJoin(rdd_new_file)

logger.info("##################FULL COMBINED METADATA################")
logger.info(rdd_old_new_schema.collect())


new_column_rdd = rdd_old_new_schema.filter(lambda x: x[1][0] is None)
missing_column_rdd = rdd_old_new_schema.filter(lambda y: y[1][1] is None)

logger.info("################NEW_COLUMNS(extra) FROM SOURCE##################")
logger.info(new_column_rdd.collect())

logger.info("###########NO CHANGES IN DATATYPE##########")
no_change_rdd = rdd_old_new_schema.filter(lambda v: v[1][1] is not None and v[1][1] == v[1][0] and v[1][0] is not None)
logger.info(no_change_rdd.collect())

logger.info("########MISSING COLUMNS (extra COLUMNS FROM hive##############")
logger.info(missing_column_rdd.collect())

logger.info("############DATA_TYPE_CHANGE DETECTED###########")
data_type_changed_rdd = rdd_old_new_schema.filter(
    lambda z: z[1][1] is not None and z[1][1] != z[1][0] and z[1][0] is not None)
logger.info(data_type_changed_rdd.collect())

#logger.info(rdd_old_new_schema.count())
#logger.info(no_change_rdd.count())

if (rdd_old_new_schema.count() == no_change_rdd.count()):
    logger.info("No Schema Change Detected....good record")


elif (rdd_old_new_schema.count() == (new_column_rdd.count() + missing_column_rdd.count())):
    logger.info("NO Common Column....bad record")
################################################metadata###comparison####ends###here##########################################################################################


if(data_type_changed_rdd.count() == 0) :
  alter_data_type=data_type_changed_rdd.collect()


  alter_data_type=[('id', ('decimal(6)', 'decimal(8)')), ('product_id', ('decimal', 'char(30)')),('id', ('decimal(6,3)', 'decimal(6,2)'))]
  for i in alter_data_type:
      logger.info("##################CONVERTING '{}'TO'{}'######################".format(i[1][0], i[1][1]))
      alter_status = check_alter_possibility(i[1])
      if (alter_status is '0'):
       logger.info("valid alter")
       col=i[0]+" "+i[0]+" "+i[1][1]
       hive_alter_cmd="hive -e 'ALTER TABLE {} change ".format(hive_table_name) +col+"'"
       logger.info(hive_alter_cmd)

      elif(alter_status is '1'):
          logger.info("...Invalid Alter....'{}' is there but the '{}' is not the in the parquet dict value".format(i[1][0],i[1][1]))

      elif(alter_status is '2'):
          logger.info("...Invalid...'{}' is not present in the parquet dictionary".format(i[1][0]))

      elif(alter_status is '3'):
          logger.info("....Invalid.....'{}' to '{}' is not permitted your decreasing the size".format(i[1][0],i[1][1]))

      else:
          logger.info("return code unknown...something went wrong")