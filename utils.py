from pyspark.sql import SparkSession
import subprocess
import re
from utils import *

parquet_types = \
    {
        'tinyint': ['smallint', 'int', 'bigint'],
        'smallint': ['int', 'bigint'],
        'int': ['bigint', 'float'],
        'float': ['double'],
        'varchar': ['varchar', 'string'],
        'char': ['char', 'varchar', 'string'],
        'decimal': ['decimal'],
        'integer': ['integer','float']
    }


def get_spark_session(appName):
    spark = SparkSession.builder \
        .master("local") \
        .appName(appName) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.warehouse.dir", '/user/hive/warehouse/') \
        .config("hive.metastore.uris", 'thrift://127.0.0.1:9083') \
        .config("spark.sql.hive.convertMetastoreParquet", "false") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


## function to run shell commands
def run_cmd(args_list, logger):
    logger.info('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def hasNumbers(inputString):
    # fn to check if string contains a number (i.e varchar(30))
    return bool(re.search(r'\d+', str(inputString)))


def getNumbers(inputString):
    # fn to return number from varchar(30) i.e (30)
    return re.search(r'\d+', inputString).group(0)


# Function to extract all the numbers from the given string
def getNumber(str):
    array = re.findall(r'[0-9]+', str)
    return array


def getlength(list):
    length = len(list)
    return length


# Driver code
# str = "varchar(20,23)"
# array = getNumbers(str)
# print(type(array))
# print(len(array))


def alphabets_only(input):
    # fn to return varchar from varchar(30)
    regex = re.compile('[^a-zA-Z]')
    alphabet = regex.sub('', str(input))
    return alphabet

# converting dict to tuple
def dict_to_tuple(dict):
    list = [(k, v) for k, v in dict.items()]
    list_tuple = tuple(list)
    return list_tuple

# double to decimal X
# decimal(5,2) to decimal(6,3)
# float to decimal X

def check_alter_possibility(data_type_changes):
    # check if datatype is present in dict key or not

    if data_type_changes[0] in parquet_types:
        print("\n..........key is present in dict.......case1....\n")

        # check if datatype (ex:int) value is present or not
        if (data_type_changes[1] in parquet_types[data_type_changes[0]]):
            return "0"

        # if value is not present in dict
        else:
            return "1"

    # 0:valid (datatype can be converted to new datatype)
    # 1:invalid(datatype cannot be converted to new datatype as no entry in dicts value)
    # 2: invalid(datatype key not found )
    # 3:invalid (datatype is converted from varchar(20)to varchar(10) size not compatible

    # looking for datatypes like (varchar(20))
    elif (hasNumbers(data_type_changes[0]) is True and hasNumbers(data_type_changes[1]) is True and getlength(
            getNumber(data_type_changes[0])) == 1 and getlength(getNumber(data_type_changes[0])) == 1):

        # looking for varchar in dict key
        if (alphabets_only(data_type_changes[0]) in parquet_types):

            print("\n..........key is present in dict........... case2\n")

            # looking for varchar in keys value
            if (alphabets_only(data_type_changes[1]) in parquet_types[alphabets_only(data_type_changes[0])]):

                # if varchar(20) is > than varchar(10)
                if (int(getNumber(data_type_changes[1])[0]) > int(getNumber(data_type_changes[0])[0])):
                    print("made change here")
                    return "0"

                else:
                    return "3"
            else:
                return "1"
        else:
            return "2"

        # looking for datatypes like (decimal(20,3))
    elif (hasNumbers(data_type_changes[0]) is True and hasNumbers(data_type_changes[1]) is True and getlength(
            getNumber(data_type_changes[0])) == 2 and getlength(getNumber(data_type_changes[0])) == 2):

        # looking for varchar in dict key
        if (alphabets_only(data_type_changes[0]) in parquet_types):

            print("\n..........key is present in dict........... casedoubledecimal\n")

            # looking for varchar in keys value
            if (alphabets_only(data_type_changes[1]) in parquet_types[alphabets_only(data_type_changes[0])]):

                # if decimal(5,2) is > than varchar(6,3)

                digits_old = int(getNumber(data_type_changes[0])[0]) - int(getNumber(data_type_changes[0])[1])
                decimal_old = int(getNumber(data_type_changes[0])[1])
                digits_new = int(getNumber(data_type_changes[1])[0]) - int(getNumber(data_type_changes[1])[1])
                decimal_new = int(getNumber(data_type_changes[1])[1])
                if digits_new > digits_old and decimal_new >= decimal_old:
                    return "0"
                else:
                    return "3"
            else:
                return "1"
        else:
            return "2"


    # for case like varchar(20) to string and datatype size should contains one digit
    elif (hasNumbers(data_type_changes[0]) is True and hasNumbers(data_type_changes[1]) is False and getlength(
            getNumber(data_type_changes[0])) == 1):

        if (alphabets_only(data_type_changes[0] in parquet_types)):

            print("key is present in dict case3\n")

            if data_type_changes[1] in parquet_types[alphabets_only(data_type_changes[0])]:
                return "0"

            else:
                return "1"
        else:
            return "2"

    else:
        print(".............key not found..........or size range error\n")
        return "2"
