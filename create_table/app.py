import pymysql
import logging
import json

rds_host = "db-roads2.ch0my8icql8f.us-east-2.rds.amazonaws.com"
rds_user = "admin"
rds_password = "poiuytrewq"
rds_db = "dbroads"

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, __):
    try:
        # Probar la conexión a la base de datos
        test_connection()

        # Crear la tabla
        create_table()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Table created successfully"
            })
        }
    except Exception as e:
        logger.error("Unexpected error: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "INTERNAL_SERVER_ERROR",
                "error": str(e)
            })
        }


def test_connection():
    logger.info("Testing database connection")
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    connection.close()
    logger.info("Database connection successful")


def create_table():
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS roads (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nombre VARCHAR(255) NOT NULL,
                tipo VARCHAR(200) NOT NULL,
                longitud FLOAT NOT NULL,
                velocidad_maxima FLOAT NOT NULL
            );
            """
        )
        connection.commit()
    except Exception as e:
        logger.error("Database transaction error: %s", str(e), exc_info=True)
        raise
    finally:
        connection.close()
