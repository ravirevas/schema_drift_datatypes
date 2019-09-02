from pyspark.sql import SparkSession
from support import *
import subprocess
from pyspark.sql.types import *
from utils import *
from config.app_config import UatConfig
from config import  logging_conf
from config import *
from config.dict_utils import DictUtil
import os
from pyspark.sql.functions import lit
import utils

log_file_path = os.path.join(os.sep, UatConfig.log_dir, UatConfig.log_file)
logger = logging_conf.configure_logger(UatConfig.logger, log_file_path)


spark=get_spark_session("schema_drift_datatypes")
current_df=spark.sql("describe sparktable1   ").rdd.map(lambda x:(x[0],x[1]))

new_df=spark.sql("describe sparktest_862019").rdd.map(lambda x: (x[0], x[1]))

curr_schema_mapping={}

for field in current_df.collect():
    curr_schema_mapping[field[0]]=field[1]

new_schema_mapping = {}
for field in new_df.collect():
    new_schema_mapping[field[0]]=field[1]

new_columns = []
dropped_columns = []
drift_columns = []
differ =  DictUtil(new_schema_mapping,curr_schema_mapping)
## capture newly added columns
new_columns = new_columns + list(differ.added())

##capture dropped columns
dropped_columns = new_columns + list(differ.removed())
drift_columns = differ.changed()
print('######new_columns#########')
print(new_columns)
print('######dropped_column########')
print(dropped_columns)
print('#######drift column############')
print(drift_columns)
column_string= ''
query = None
database = "default"
hive_table="sparktable3"

if drift_columns:
    drift_columns=[('id', ('decimal(6,3)', 'decimal')), ('product_id', ('decimal', 'char(30)')),('id', ('decimal(6,3)', 'decimal(6,2)'))]
    for i in drift_columns:
        logger.info("##################CONVERTING '{}'TO'{}'######################".format(i[1][0], i[1][1]))
        alter_status = check_alter_possibility(i[1])
        if (alter_status is '0'):
            logger.info("valid alter")
            col = i[0] + " " + i[0] + " " + i[1][1]
            cmd = "ALTER TABLE {} change ".format(hive_table_name) + col
            cmd_stmt = ['hive', '-e', '"{}"'.format(cmd)]
            #ret, out, err = utils.run_cmd(cmd_stmt, logger)
        elif (alter_status is '1'):
            logger.info(
                "...Invalid Alter....'{}' is there but the '{}' is not the in the parquet dict value".format(i[1][0],
                                                                                                             i[1][1]))

        elif (alter_status is '2'):
            logger.info("...Invalid...'{}' is not present in the parquet dictionary".format(i[1][0]))

        elif (alter_status is '3'):
            logger.info(
                "....Invalid.....'{}' to '{}' is not permitted your are  decreasing the size".format(i[1][0], i[1][1]))

        else:
            logger.info("return code unknown...something went wrong")

