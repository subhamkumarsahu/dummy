from datetime import datetime

from botocore.exceptions import ClientError
import pymysql
from configparser import ConfigParser
import json
import xlsxwriter
import smtplib
from email.message import EmailMessage
from email.utils import formatdate
import os
import boto3
import pytz

tz_NY = pytz.timezone('Asia/Kolkata')
datetime_NY = datetime.now(tz_NY)
datetime_ist = datetime_NY.strftime("%d-%m-%Y_%H:%M:%S")

#reading config file
configure = ConfigParser()
configure.read('config.ini')

#converting str values to tuple, and reading dict values from config.ini file
c_null_check = (tuple(map(str, configure.get('columns_validation','NULL_DATA_CHECK').split(', '))))
c_date_format_check = json.loads(configure.get('columns_validation','DATE_FORMAT_CHECK'))
c_date_format_columns = []
c_type_not_string_check = (tuple(map(str, configure.get('columns_validation','NON_STRING_CHECK').split(', '))))
c_type_non_numeric_check = (tuple(map(str, configure.get('columns_validation','NON_NUMERIC_CHECK').split(', '))))
c_data_length_check = json.loads(configure.get('columns_validation','DATA_LENGTH_CHECK'))
c_data_length_columns = []
c_spcl_char_check = (tuple(map(str, configure.get('columns_validation','SPCL_CHAR_CHECK').split(', '))))

NULL_DATA_CHECK=[]
DATE_FORMAT_CHECK=[]
NON_STRING_CHECK=[]
NON_NUMERIC_CHECK=[]
DATA_LENGTH_CHECK=[]
SPCL_CHAR_CHECK=[]

source_rds_data = ''
target_rds_data = ''



mismatch_column = configure.get('db_mismatch_column','mismatch_column')

athena_database = configure.get('athena_instance_details','athena_database')
athena_table = configure.get('athena_instance_details','athena_table')
athena_results_output_location = configure.get('athena_instance_details','athena_results_output_location')


c_null_mismatched_db_results = []
c_null_records_list= []
c_date_format_mismatch_db_results = []
c_date_format_mismatch_list= []
c_string_mismatch_db_results = []
c_string_mismatch_list= []
c_numeric_mismatch_db_results = []
c_numeric_mismatch_list= []
c_data_length_mismatch_db_results = []
c_data_length_mismatch_list= []
c_spcl_char_mismatch_db_results = []
c_spcl_char_numeric_mismatch_list= []
#getting DB_Name from config.ini
database_name = configure.get('database_credentials','DB_NAME')

# Creating Session With Boto3.
s3_session = boto3.Session(
    aws_access_key_id=configure.get('aws_credentials','AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=configure.get('aws_credentials','AWS_SECRET_ACCESS_KEY')
)

# Establishing athena Client
athena_client=boto3.client('athena')

# Creating S3 Resource From the Session.
s3 = s3_session.resource('s3')
s3_client = boto3.client('s3')

txt_data = b'This is the content of the file uploaded from python boto3 asdfasdf'

def execute_validation(event = None, context= None):
    null_data_check_exec_status = 'Failed'
    date_format_check_exec_status = 'Failed'
    non_string_check_exec_status = 'Failed'
    non_numeric_check_exec_status = 'Failed'
    data_length_check_exec_status = 'Failed'
    spcl_char_exec_status = 'Failed'
    mysql_validation_exec_status = ''

    #connection establishing object for MYSQL
    database = pymysql.connect(host=configure.get('database_credentials','DB_HOST'), user=configure.get('database_credentials','DB_User'), password=configure.get('database_credentials','DB_PWD'))
    cursor = database.cursor()

    cursor.execute('use '+ configure.get('database_credentials','DB_SCHEMA'))
    file = '/tmp/'+'logs_'+datetime_ist+'.txt'
    #file = 'logs_' + datetime_ist + '.txt'
    f = open(file, 'w')
    # with open('logs_'+datetime_ist+'.txt', 'w') as f:
    f.write("********************************\n\n")
    f.write("DATE: {}\n".format(datetime_ist))
    f.write("Data Validation Starts\n")
    f.write("********************************\n\n")
    print("***** Data Validation Starts *****")

    #MySQL RDS Data Validation
    print("\nNULL Validations")
    f.write("NULL Validations\n")
    # NULL_CHECK Validation
    for items in c_null_check:
        null_data_results_query = '''
        SELECT count({}) as null_check
        from {}
        WHERE {} IS NULL OR
        {} ="" OR
        {} ="null";
        '''.format(items,database_name, items, items, items)
        cursor.execute(null_data_results_query)
        fetched_data = cursor.fetchall()

        for data in fetched_data:
            for item in data:
                fetched_data_list = []
                fetched_data_list.append(item)
                print("Null Data Found for column {}: {} records".format(items,fetched_data_list[0]) )
                f.write("Null Data Found for column {}: {} records \n".format(items,fetched_data_list[0]))
                NULL_DATA_CHECK.append(fetched_data_list[0])

    for items in c_null_check:
        null_data_mismatch_query = '''
            SELECT {}
            from {}
            WHERE
            {} IS NULL OR
            {} ="" OR
            {}="null";
            '''.format(mismatch_column, database_name, items, items, items)
        cursor.execute(null_data_mismatch_query)
        null_fetched_data = cursor.fetchall()
        c_null_mismatched_db_results.append(null_fetched_data)

    for item in list(c_null_mismatched_db_results):
        if len(item) == 0:
            c_null_records_list.append("")
        elif len(item) ==1:
            for i in item:
                c_null_records_list.append(list(i))
            break
        else:
            c_null_records_list.append([items for sublist in item for items in sublist])

    regex = ''
    # Date Format Validation
    print("\nDate Format Validations")
    f.write("\n")
    f.write("Date Format Validations\n")
    for columns, date_format in c_date_format_check.items():
        c_date_format_columns.append(columns)
        if date_format in ("MM/DD/YYYY", "MM/DD/YYY", "mm/dd/yyyy", "mm/dd/yyy"):
            regex = '^(1[0-2]|0[1-9])/(3[01]|[12][0-9]|0[1-9])/[0-9]{4}$'
        elif date_format in ("DD/MM/YYYY", "DD/MM/YYY", "dd/mm/yyyy", "dd/mm/yyy"):
            regex = '^([0]?[1-9]|[1|2][0-9]|[3][0|1])[./-]([0]?[1-9]|[1][0-2])[./-]([0-9]{4}|[0-9]{2})$'

        date_format_results_query = '''
        SELECT count({}) as date_format_check
        from {}
        WHERE NOT {} REGEXP '{}';
        '''.format(columns, database_name, columns, regex)

        cursor.execute(date_format_results_query)
        fetched_data = cursor.fetchall()

        for data in fetched_data:
            for item in data:
                fetched_data_list = []
                fetched_data_list.append(item)
                print("Unmatched Date format Found for column {} with Format {}: {} records".format(columns, date_format,fetched_data_list[0]))
                f.write("Unmatched Date format Found for column {} with Format {}: {} records\n".format(columns,
                                                                                                     date_format,
                                                                                                     fetched_data_list[
                                                                                                         0]))

                DATE_FORMAT_CHECK.append(fetched_data_list[0])

    for columns, date_format in c_date_format_check.items():
        if date_format in ("MM/DD/YYYY", "MM/DD/YYY", "mm/dd/yyyy", "mm/dd/yyy"):
            regex = '^(1[0-2]|0[1-9])/(3[01]|[12][0-9]|0[1-9])/[0-9]{4}$'
        elif date_format in ("DD/MM/YYYY", "DD/MM/YYY", "dd/mm/yyyy", "dd/mm/yyy"):
            regex = '^([0]?[1-9]|[1|2][0-9]|[3][0|1])[./-]([0]?[1-9]|[1][0-2])[./-]([0-9]{4}|[0-9]{2})$'

        date_format_results_query = '''
        SELECT {}
        from {}
        WHERE NOT {} REGEXP '{}';
        '''.format(mismatch_column, database_name, columns, regex)

        cursor.execute(date_format_results_query)
        date_format_fetched_data = cursor.fetchall()
        c_date_format_mismatch_db_results.append(date_format_fetched_data)

    for item in list(c_date_format_mismatch_db_results):
        if len(item) == 0:
            c_date_format_mismatch_list.append("")
        elif len(item) ==1:
            for i in item:
                c_date_format_mismatch_list.append(list(i))
            break
        else:
            c_date_format_mismatch_list.append([items for sublist in item for items in sublist])

    # Data_Type:String Validation
    print("\nData_Type:Non-String Validations")
    f.write("\n")
    f.write("Data_Type:Non-String Validations\n")
    for items in c_type_not_string_check:
        non_string_results_query = '''
        select count({})
        from {}
        where {} regexp '^[0-9]+$';
        '''.format(items, database_name, items)

        cursor.execute(non_string_results_query)
        fetched_data = cursor.fetchall()

        for data in fetched_data:
            for item in data:
                fetched_data_list = []
                fetched_data_list.append(item)
                print("Unmatched String Data Found for column {}: {} records".format(items,fetched_data_list[0]) )
                f.write("Unmatched String Data Found for column {}: {} records\n".format(items, fetched_data_list[0]))
                NON_STRING_CHECK.append(fetched_data_list[0])

    for items in c_type_not_string_check:
        string_mismatch_query = '''
            SELECT {}
            from {}
            where {} regexp '^[0-9]+$';
            '''.format(mismatch_column, database_name, items)
        cursor.execute(string_mismatch_query)
        string_mismatch_fetched_data = cursor.fetchall()
        c_string_mismatch_db_results.append(string_mismatch_fetched_data)

    for item in list(c_string_mismatch_db_results):
        if len(item) == 0:
            c_string_mismatch_list.append("")
        elif len(item) ==1:
            for i in item:
                c_string_mismatch_list.append(list(i))
            break
        else:
            c_string_mismatch_list.append([items for sublist in item for items in sublist])

    # Data_Type:Non-Numeric Validation
    print("\nData_Type:Non-Numeric Validations")
    f.write("\n")
    f.write("Data_Type:Non-Numeric Validations\n")
    for items in c_type_non_numeric_check:
        non_numeric_results_query = '''
        SELECT count({}) as numeric_check
        from {}
        where not {} REGEXP '^[0-9]+$';
        '''.format(items, database_name, items)

        cursor.execute(non_numeric_results_query)
        fetched_data = cursor.fetchall()

        for data in fetched_data:
            for item in data:
                fetched_data_list = []
                fetched_data_list.append(item)
                print("Unmatched Numeric Data Found for column {}: {} records".format(items,fetched_data_list[0]) )
                f.write("Unmatched Numeric Data Found for column {}: {} records\n".format(items,fetched_data_list[0]) )
                NON_NUMERIC_CHECK.append(fetched_data_list[0])

    for items in c_type_non_numeric_check:
        non_numeric_data_mismatch_query = '''
            SELECT {}
            from {}
            WHERE not {} REGEXP '^[0-9]+$';
            '''.format(mismatch_column, database_name, items)
        cursor.execute(non_numeric_data_mismatch_query)
        numeric_mismatch_fetched_data = cursor.fetchall()
        c_numeric_mismatch_db_results.append(numeric_mismatch_fetched_data)

    for item in list(c_numeric_mismatch_db_results):
        if len(item) == 0:
            c_numeric_mismatch_list.append("")
        elif len(item) ==1:
            for i in item:
                c_numeric_mismatch_list.append(list(i))
            break
        else:
            c_numeric_mismatch_list.append([items for sublist in item for items in sublist])

    # Data Length Validation
    print("\nData Length Validations")
    f.write("\n")
    f.write("Data Length Validations\n")
    for columns, length in c_data_length_check.items():
        c_data_length_columns.append(columns)
        data_length_results_query = '''
        select count({})
        from {}
        where {} NOT regexp '^[0-9]+$' and LENGTH({})!={};
        '''.format(columns, database_name, columns, columns, length)

        cursor.execute(data_length_results_query)
        fetched_data = cursor.fetchall()

        for data in fetched_data:
            for item in data:
                fetched_data_list = []
                fetched_data_list.append(item)
                print("Unmatched Length Data Found for column {} with Length {}: {} records".format(columns, length,fetched_data_list[0]))
                f.write("Unmatched Length Data Found for column {} with Length {}: {} records\n".format(columns, length,
                                                                                                         fetched_data_list[
                                                                                                             0]))
                DATA_LENGTH_CHECK.append(fetched_data_list[0])

    for columns, length in c_data_length_check.items():
        data_length_results_query = '''
        select {}
        from {}
        where {} NOT regexp '^[0-9]+$' and LENGTH({})!={};
        '''.format(mismatch_column, database_name, columns, columns, length)

        cursor.execute(data_length_results_query)
        mismatch_length_fetched_data = cursor.fetchall()
        c_data_length_mismatch_db_results.append(mismatch_length_fetched_data)

    for item in list(c_data_length_mismatch_db_results):
        if len(item) == 0:
            c_data_length_mismatch_list.append("")
        elif len(item) == 1:
            for i in item:
                c_data_length_mismatch_list.append(list(i))
            break
        else:
            c_data_length_mismatch_list.append([items for sublist in item for items in sublist])


    # Special Character Validation
    print("\nSpecial Character Validations")
    f.write("\n")
    f.write("Special Character Validations\n")
    for items in c_spcl_char_check:
        spcl_char_results_query = '''
        SELECT count({}) as spcl_char_check
        from {}
        WHERE {} REGEXP '[^a-zA-Z0-9]';
        '''.format(items, database_name, items)

        cursor.execute(spcl_char_results_query)
        fetched_data = cursor.fetchall()

        for data in fetched_data:
            for item in data:
                fetched_data_list = []
                fetched_data_list.append(item)
                print("Special Character Data Found for column {}: {} records".format(items,fetched_data_list[0]) )
                f.write("Special Character Data Found for column {}: {} records\n".format(items, fetched_data_list[0]))
                SPCL_CHAR_CHECK.append(fetched_data_list[0])

    for items in c_spcl_char_check:
        spcl_char_data_mismatch_query = '''
            SELECT {}
            from {}
            WHERE {} REGEXP '[^a-zA-Z0-9]';
            '''.format(mismatch_column, database_name, items)
        cursor.execute(spcl_char_data_mismatch_query)
        spcl_char_fetched_data = cursor.fetchall()
        c_spcl_char_mismatch_db_results.append(spcl_char_fetched_data)

    for item in list(c_spcl_char_mismatch_db_results):
        if len(item) == 0:
            c_spcl_char_numeric_mismatch_list.append("")
        elif len(item) ==1:
            for i in item:
                c_spcl_char_numeric_mismatch_list.append(list(i))
            break
        else:
            c_spcl_char_numeric_mismatch_list.append([items for sublist in item for items in sublist])

    validation_types = []
    for validation_type in configure['columns_validation']:
        validation_types.append(validation_type.upper())

    exact_columns = []

    # for columns in all_columns:
    #    if columns not in exact_columns:
    #       exact_columns.append(columns)

    #writing data to excel Workbook
    localfileName = '/tmp/'+(configure.get('results_excel_file_details','FILE_NAME') +"_"+datetime_ist+".xlsx")

    workbook_name = configure.get('results_excel_file_details', 'FILE_NAME') + "_" + datetime_ist + ".xlsx"

    workbook = xlsxwriter.Workbook(localfileName)

    cell_format = workbook.add_format()
    cell_format.set_bold()

    null_kv_pair ={}
    for key in list(c_null_check):
        for value in NULL_DATA_CHECK:
            null_kv_pair[key] = value
            NULL_DATA_CHECK.remove(value)
            break

    null_kv_pair = {x:y for x,y in null_kv_pair.items() if y!=0}
    print("null_kv_pair : ", null_kv_pair)

    if len(null_kv_pair) >= 1:
        ws_null_data_check = workbook.add_worksheet('NULL_DATA_CHECK')
        col = 1

        for key, value in null_kv_pair.items():
            ws_null_data_check.set_row(0, col, cell_format)
            ws_null_data_check.write(0, 0, "COLUMNS")
            ws_null_data_check.write(1, 0, "Mismatched Records Count")
            ws_null_data_check.write(3, 0, "Data Mismatched", cell_format)
            ws_null_data_check.write(4, 0, "Column Name", cell_format)
            ws_null_data_check.write(0, col, key)
            ws_null_data_check.set_row(0, col, cell_format)
            ws_null_data_check.write(1, col, "{} Records".format(value))
            col += 1

        col = 0
        row = 5
        for k in c_null_records_list:
            col += 1
            ws_null_data_check.set_row(0, col, cell_format)

            if type(k) is list:
                if len(k) >= 2:
                    row = 5
                    for i in k:
                        ws_null_data_check.write(4, col, mismatch_column)
                        ws_null_data_check.write(row, col, i)
                        row += 1
                elif len(k) == 1:
                    for i in k:
                        row = 5
                        ws_null_data_check.write(4, col, mismatch_column)
                        ws_null_data_check.write(row, col, i)
            else:
                ws_null_data_check.write(5, col, k)
                col -= 1
    else:
        null_data_check_exec_status = "Passed"
        print("NULL_DATA_CHECK returned 0 records...")

    date_format_kv_pair = {}
    for key in c_date_format_columns:
        for value in DATE_FORMAT_CHECK:
            date_format_kv_pair[key] = value
            DATE_FORMAT_CHECK.remove(value)
            break

    date_format_kv_pair = {x: y for x, y in date_format_kv_pair.items() if y != 0}
    print("date_format_kv_pair : ", date_format_kv_pair)

    if len(date_format_kv_pair) >=1:
        ws_date_format_check = workbook.add_worksheet('DATE_FORMAT_CHECK')
        col = 1

        for key, value in date_format_kv_pair.items():
            ws_date_format_check.set_row(0, col, cell_format)
            ws_date_format_check.write(0, 0, "COLUMNS")
            ws_date_format_check.write(1, 0, "Mismatched Records Count")
            ws_date_format_check.write(3, 0, "Data Mismatched", cell_format)
            ws_date_format_check.write(4, 0, "Column Name", cell_format)
            ws_date_format_check.write(0, col, key)
            ws_date_format_check.set_row(0, col, cell_format)
            ws_date_format_check.write(1, col, "{} Records".format(value))
            col += 1

        col = 0
        row = 5
        for k in c_date_format_mismatch_list:
            col += 1
            ws_date_format_check.set_row(0, col, cell_format)
            if type(k) is list:
                if len(k) >= 2:
                    row = 5
                    for i in k:
                        ws_date_format_check.write(4, col, mismatch_column)
                        ws_date_format_check.write(row, col, i)
                        row += 1
                elif len(k) == 1:
                    for i in k:
                        row = 5
                        ws_date_format_check.write(4, col, mismatch_column)
                        ws_date_format_check.write(row, col, i)
            else:
                ws_date_format_check.write(5, col, k)
                col -= 1
    else:
        date_format_check_exec_status = "Passed"
        print("DATE_FORMAT_CHECK returned 0 records...")

    non_string_kv_pair = {}
    for key in list(c_type_not_string_check):
        for value in NON_STRING_CHECK:
            non_string_kv_pair[key] = value
            NON_STRING_CHECK.remove(value)
            break

    non_string_kv_pair = {x: y for x, y in non_string_kv_pair.items() if y != 0}
    print("non_string_kv_pair : ", non_string_kv_pair)

    if len(non_string_kv_pair) >=1:
        ws_non_string_check = workbook.add_worksheet('NON_STRING_CHECK')
        col = 1

        for key, value in non_string_kv_pair.items():
            ws_non_string_check.set_row(0, col, cell_format)
            ws_non_string_check.write(0, 0, "COLUMNS")
            ws_non_string_check.write(1, 0, "Mismatched Records Count")
            ws_non_string_check.write(3, 0, "Data Mismatched", cell_format)
            ws_non_string_check.write(4, 0, "Column Name", cell_format)
            ws_non_string_check.write(0, col, key)
            ws_non_string_check.set_row(0, col, cell_format)
            ws_non_string_check.write(1, col, "{} Records".format(value))
            col += 1

        col = 0
        row = 5
        for k in c_string_mismatch_list:
            col += 1
            ws_non_string_check.set_row(0, col, cell_format)
            # ws_non_string_check.write(5, 0, mismatch_column)
            if type(k) is list:
                if len(k) >= 2:
                    row = 5
                    for i in k:
                        ws_non_string_check.write(4, col, mismatch_column)
                        ws_non_string_check.write(row, col, i)
                        row += 1
                elif len(k) == 1:
                    for i in k:
                        row = 5
                        ws_non_string_check.write(4, col, mismatch_column)
                        ws_non_string_check.write(row, col, i)
            else:
                ws_non_string_check.write(5, col, k)
                col -= 1
    else:
        non_string_check_exec_status = "Passed"
        print("NON_STRING_CHECK returned 0 records...")

    non_numeric_kv_pair = {}
    for key in list(c_type_non_numeric_check):
        for value in NON_NUMERIC_CHECK:
            non_numeric_kv_pair[key] = value
            NON_NUMERIC_CHECK.remove(value)
            break

    non_numeric_kv_pair = {x: y for x, y in non_numeric_kv_pair.items() if y != 0}
    print("non_numeric_kv_pair : ", non_numeric_kv_pair)

    if len(non_numeric_kv_pair) >=1:
        ws_non_numeric_check = workbook.add_worksheet('NON_NUMERIC_CHECK')
        col = 1

        for key, value in non_numeric_kv_pair.items():
            ws_non_numeric_check.set_row(0, col, cell_format)
            ws_non_numeric_check.write(0, 0, "COLUMNS")
            ws_non_numeric_check.write(1, 0, "Mismatched Records Count")
            ws_non_numeric_check.write(3, 0, "Data Mismatched", cell_format)
            ws_non_numeric_check.write(4, 0, "Column Name", cell_format)
            ws_non_numeric_check.write(0, col, key)
            ws_non_numeric_check.set_row(0, col, cell_format)
            ws_non_numeric_check.write(1, col, "{} Records".format(value))
            col += 1

        col = 0
        row = 5
        for k in c_numeric_mismatch_list:
            col += 1
            ws_non_numeric_check.set_row(0, col, cell_format)
            # ws_non_numeric_check.write(5, 0, mismatch_column)
            if type(k) is list:
                if len(k) >= 2:
                    row = 5
                    for i in k:
                        ws_non_numeric_check.write(4, col, mismatch_column)
                        ws_non_numeric_check.write(row, col, i)
                        row += 1
                elif len(k) == 1:
                    for i in k:
                        row = 5
                        ws_non_numeric_check.write(4, col, mismatch_column)
                        ws_non_numeric_check.write(row, col, i)
            else:
                ws_non_numeric_check.write(5, col, k)
                col -= 1
    else:
        non_numeric_check_exec_status = "Passed"
        print("NON_NUMERIC_CHECK returned 0 records...")

    data_length_kv_pair = {}
    for key in c_data_length_columns:
        for value in DATA_LENGTH_CHECK:
            data_length_kv_pair[key] = value
            DATA_LENGTH_CHECK.remove(value)
            break

    data_length_kv_pair = {x: y for x, y in data_length_kv_pair.items() if y != 0}
    print("data_length_kv_pair : ", data_length_kv_pair)

    if len(data_length_kv_pair) >=1:
        ws_data_length_check = workbook.add_worksheet('DATA_LENGTH_CHECK')
        col = 1

        for key, value in data_length_kv_pair.items():
            ws_data_length_check.set_row(0, col, cell_format)
            ws_data_length_check.write(0, 0, "COLUMNS")
            ws_data_length_check.write(1, 0, "Mismatched Records Count")
            ws_data_length_check.write(3, 0, "Data Mismatched", cell_format)
            ws_data_length_check.write(4, 0, "Column Name", cell_format)
            ws_data_length_check.write(0, col, key)
            ws_data_length_check.set_row(0, col, cell_format)
            ws_data_length_check.write(1, col, "{} Records".format(value))
            col += 1

        col = 0
        row = 5
        for k in c_data_length_mismatch_list:
            col += 1
            ws_data_length_check.set_row(0, col, cell_format)

            if type(k) is list:
                if len(k) >= 2:
                    row = 5
                    for i in k:
                        ws_data_length_check.write(4, col, mismatch_column)
                        ws_data_length_check.write(row, col, i)
                        row += 1
                elif len(k) == 1:
                    for i in k:
                        row = 5
                        ws_data_length_check.write(4, col, mismatch_column)
                        ws_data_length_check.write(row, col, i)
            else:
                ws_data_length_check.write(5, col, k)
                col -= 1
    else:
        data_length_check_exec_status = "Passed"
        print("DATA_LENGTH_CHECK returned 0 records...")

    spcl_char_kv_pair = {}
    for key in list(c_spcl_char_check):
        for value in SPCL_CHAR_CHECK:
            spcl_char_kv_pair[key] = value
            SPCL_CHAR_CHECK.remove(value)
            break

    spcl_char_kv_pair = {x: y for x, y in spcl_char_kv_pair.items() if y != 0}
    print("spcl_char_kv_pair : ", spcl_char_kv_pair)

    if len(spcl_char_kv_pair) >= 1:
        ws_spcl_char_check = workbook.add_worksheet('SPCL_CHAR_CHECK')
        col = 1

        for key, value in spcl_char_kv_pair.items():
            ws_spcl_char_check.set_row(0, col, cell_format)
            ws_spcl_char_check.write(0, 0, "COLUMNS")
            ws_spcl_char_check.write(1, 0, "Mismatched Records Count")
            ws_spcl_char_check.write(3, 0, "Data Mismatched", cell_format)
            ws_spcl_char_check.write(4, 0, "Column Name", cell_format)
            ws_spcl_char_check.write(0, col, key)
            ws_spcl_char_check.set_row(0, col, cell_format)
            ws_spcl_char_check.write(1, col, "{} Records".format(value))
            col += 1

        col = 0
        row = 5
        for k in c_spcl_char_numeric_mismatch_list:
            col += 1
            ws_spcl_char_check.set_row(0, col, cell_format)
            # ws_spcl_char_check.write(5, 0, mismatch_column)
            if type(k) is list:
                if len(k) >= 2:
                    row = 5
                    for i in k:
                        ws_spcl_char_check.write(4, col, mismatch_column)
                        ws_spcl_char_check.write(row, col, i)
                        row += 1
                elif len(k) == 1:
                    for i in k:
                        row = 5
                        ws_spcl_char_check.write(4, col, mismatch_column)
                        ws_spcl_char_check.write(row, col, i)
            else:
                ws_spcl_char_check.write(5, col, k)
                col -= 1
    else:
        spcl_char_exec_status = "Passed"
        print("SPCL_CHAR_CHECK returned 0 records...")

    f.write("\n")
    print("\nExecuting TESTCASE for Source:MySQL DB and Target:MySQL DB")
    f.write("Executing TESTCASE for Source:MySQL DB and Target:MySQL DB\n\n")

    for key, value in configure['queries_to_validate_source_MySQL'].items():
        # RDS Instance Query
        cursor.execute(value)
        source_rds_data = cursor.fetchall()

    for key, value in configure['queries_to_validate_target_MySQL'].items():
        # RDS Instance Query
        cursor.execute(value)
        target_rds_data = cursor.fetchall()

    print("\nSource MySQL DB Results: ", source_rds_data)
    f.write("Source MySQL DB Results: {}\n".format(source_rds_data))
    print("Target MySQL DB Results ", target_rds_data)
    f.write("Target MySQL DB Results: {}\n\n".format(target_rds_data))
    #print("\nComparing Results of MySQL DB Target and Source Tables")
    #f.write("Comparing Results of MySQL DB Target and Source Tables\n\n")

    mysql_results = []

    if (source_rds_data == target_rds_data):
        mysql_validation_exec_status = 'Passed'
        #print("Result: The Data is Matching")
        #f.write("Result: The Data is Matching\n\n")
        mysql_results.append("The Data is Matching.")
    else:
        mysql_validation_exec_status = 'Failed'
        #print("Result: The Data is not Matching")
        #f.write("Result: The Data is not Matching\n\n")
        mysql_results.append("The Data is not Matching.")

    f.write("Testcase Execution Summary - \n\n")
    f.write("NULL_DATA_CHECK - {}\n".format(null_data_check_exec_status))
    f.write("DATE_FORMAT_CHECK - {}\n".format(date_format_check_exec_status))
    f.write("NON_STRING_CHECK - {}\n".format(non_string_check_exec_status))
    f.write("NON_NUMERIC_CHECK - {}\n".format(non_numeric_check_exec_status))
    f.write("DATA_LENGTH_CHECK - {}\n".format(data_length_check_exec_status))
    f.write("SPCL_CHAR_CHECK - {}\n".format(spcl_char_exec_status))
    f.write("MYSQL_VALIDATION - {}\n\n".format(mysql_validation_exec_status))

    execution_status = []
    execution_status.append(null_data_check_exec_status)
    execution_status.append(date_format_check_exec_status)
    execution_status.append(non_string_check_exec_status)
    execution_status.append(non_numeric_check_exec_status)
    execution_status.append(data_length_check_exec_status)
    execution_status.append(spcl_char_exec_status)
    execution_status.append(mysql_validation_exec_status)

    f.write("Testcases Passed: {}\n".format(sum('Passed' in s for s in execution_status)))
    f.write("Testcases Failed: {}\n\n".format(sum('Failed' in s for s in execution_status)))

    mysql_validation_worksheet = workbook.add_worksheet('MYSQL_VALIDATION')
    col = 0
    row = 1
    for source_key, source_value in configure['queries_to_validate_source_MySQL'].items():
        for target_key, target_value in configure['queries_to_validate_target_MySQL'].items():
            for i in mysql_results:
                if len(target_key) == len(source_key):
                    mysql_validation_worksheet.set_row(0, col, cell_format)
                    mysql_validation_worksheet.write(0, 0, "MySQL Source Table Queries")
                    mysql_validation_worksheet.write(row, 0, source_value)
                    mysql_validation_worksheet.write(0, 1, "MySQL Target Table Queries")
                    mysql_validation_worksheet.write(row, 1, target_value)
                    mysql_validation_worksheet.write(0, 2, "Results")
                    mysql_validation_worksheet.write(row, 2, i)
                    row +=1
                else:
                    print("The key length for MySQL Source and Target Tables are not Same. Please verify.")

    print("\nTesting Completed Successfully...")
    f.write("Testing Completed Successfully...\n\n")
    f.write("Writing Results to Excel Workbook: {}\n\n".format(localfileName))
    print("\nSaving Execution results to WorkBook: {}".format(localfileName))
    workbook.close()


    mail_recipients = configure.get('gmail_details','mail_recipients')
    sender_email_address = configure.get('gmail_details','sender_email_address')

    # Sending Email to Users via SMTP for Gmail
    msg = EmailMessage()
    msg['Subject'] = "Hey, Validation Suite's execution is Successful !!!"
    msg['From'] : sender_email_address
    msg['To'] = mail_recipients
    msg['Date'] = formatdate(localtime = True)
    msg.set_content('''Hi User,

    The execution of Data Validation Suite is triggered at {}.

    Testcase Execution Summary -

    NULL_DATA_CHECK - {}
    DATE_FORMAT_CHECK - {}
    NON_STRING_CHECK - {}
    NON_NUMERIC_CHECK - {}
    DATA_LENGTH_CHECK - {}
    SPCL_CHAR_CHECK - {}
    MYSQL_VALIDATION - {}

    Testcases Passed: {}
    Testcases Failed: {}

    Attached is the test evidence for reference.

    Thanks,
    Automation Team
    '''.format(datetime_ist, null_data_check_exec_status, date_format_check_exec_status, non_string_check_exec_status, non_numeric_check_exec_status, data_length_check_exec_status, spcl_char_exec_status, mysql_validation_exec_status, sum('Passed' in s for s in execution_status), sum('Failed' in s for s in execution_status)))
    port = configure.get('gmail_details','smtp_port')
    smtp_server = configure.get('gmail_details','smtp_server')
    password = configure.get('gmail_details','sender_password')

    with open(localfileName, 'rb') as f:
        file_data = f.read()
    msg.add_attachment(file_data, maintype="application", subtype="xlsx", filename=localfileName)

    with open(file, 'rb') as f:
        file_data = f.read()
    msg.add_attachment(file_data, maintype="application", subtype="txt", filename=file)

    with smtplib.SMTP_SSL(smtp_server, port) as server:
        server.login(sender_email_address, password)
        server.send_message(msg)

    f.close()
    # delete esisting files from s3 bucket
    my_bucket = s3.Bucket('validationtestresultss3')
    for my_bucket_object in my_bucket.objects.all():
        obj = s3.Object("validationtestresultss3", my_bucket_object.key)
        obj.delete()
    print(file)
    # upload the results file to s3 bucket
    response = s3_client.upload_file(localfileName, 'validationtestresultss3', localfileName)
    upload_logs_file_s3 = s3_client.upload_file(file, 'validationtestresultss3', file)
    # # get current file from s3 bucket
    # current_results_file = ''
    # for my_bucket_object in my_bucket.objects.all():
    #     current_results_file = my_bucket_object.key

    print("\nNew File uploaded to S3 Bucket: ", localfileName)
    #f.write("New File uploaded to S3 Bucket: {}\n\n".format(localfileName))

execute_validation()
