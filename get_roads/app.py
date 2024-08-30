import json
import pymysql
from decimal import Decimal

rds_host = "db-roads2.ch0my8icql8f.us-east-2.rds.amazonaws.com"
rds_user = "admin"
rds_password = "poiuytrewq"
rds_db = "dbroads"


def decimal_to_float(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def lambda_handler(event, __):
    try:
        result = get_all_roads()

        body = {
            "message": "ROADS_FETCHED",
            "roads": result
        }
        return {
            "statusCode": 200,
            "body": json.dumps(body, default=decimal_to_float),
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            }
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "INTERNAL_SERVER_ERROR",
                "error": str(e)
            }),
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "https://www.example.com",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            }
        }


def get_all_roads():
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM roads;")

        result = cursor.fetchall()
        result = [dict(zip([column[0] for column in cursor.description], row)) for row in result]

        return result
    except Exception as e:
        raise e
    finally:
        connection.close()
