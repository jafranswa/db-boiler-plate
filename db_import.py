import sys
import subprocess
import mysql.connector
from mysql.connector import Error

def get_config_value(config_file, key):
    command = f"php {config_file} {key}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    return result.stdout.strip()

def connect_to_mysql():
    try:
        DB_USER = get_config_value("portal_config.php", "get_db_user")
        DB_PASS = get_config_value("portal_config.php", "get_db_pass")
        DB_HOST = get_config_value("portal_config.php", "get_db_host")
        connection = mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        sys.exit(1)

def import_table_from_s3(connection, table_name):
    try:
        cursor = connection.cursor()
        S3_ACCESS_KEY = ""
        S3_SECRET_KEY = ""
        S3_BUCKET = ""
        
        print(f"Importing table {table_name} from {S3_BUCKET}")
        
        s3cmd_command = f"s3cmd get {S3_BUCKET}/{table_name}.GZ --access_key={S3_ACCESS_KEY} --secret_key={S3_SECRET_KEY} -"
        gunzip_command = "gunzip"
        mysql_import_command = f"mysql -u {DB_USER} -h {DB_HOST} -p{DB_PASS} {TEMP_DB_NAME}"

        subprocess.run(f"{s3cmd_command} | {gunzip_command} | {mysql_import_command}", shell=True)

    except Error as e:
        print(f"Error importing table from S3: {e}")
        sys.exit(1)
    finally:
        cursor.close()

def main():
    if len(sys.argv) != 3:
        print("DB_NAME AND TABLE_NAME arguments are required")
        sys.exit(1)

    DB_NAME = sys.argv[1]
    TABLE_NAME = sys.argv[2]
    TEMP_DB_NAME = "temp"

    connection = connect_to_mysql()
    
    import_table_from_s3(connection, TABLE_NAME)

    diff_query = f"select count(*) into @rows_new from {TEMP_DB_NAME}; select count(*) into @row_old from {DB_NAME}.{TABLE_NAME}; select (if(@row_new/@rows_old > 0.9 AND @rows_new/@rows_old < 1.1, 1, 0))"
    
    try:
        cursor = connection.cursor()
        cursor.execute(diff_query)
        diff_result = cursor.fetchone()[0]

        if diff_result == 1:
            print("Data delta is < 10%, replacing the old table")
            cursor.execute(f"drop table if exists {DB_NAME}.{TABLE_NAME}")
            cursor.execute(f"rename table {TEMP_DB_NAME}.{TABLE_NAME} to {DB_NAME}.{TABLE_NAME}")
        else:
            print("Data delta is greater than 10%, discarding new data")
            cursor.execute(f"drop table if exists {TEMP_DB_NAME}.{TABLE_NAME}")

        connection.commit()

    except Error as e:
        print(f"Error executing MySQL queries: {e}")
        sys.exit(1)
    finally:
        cursor.close()
        connection.close()

    sys.exit(0)

if __name__ == "__main__":
    main()
