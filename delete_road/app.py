import json
import pymysql
import logging

rds_host = "db-roads2.ch0my8icql8f.us-east-2.rds.amazonaws.com"
rds_user = "admin"
rds_password = "poiuytrewq"
rds_db = "dbroads"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, __):
    try:
        if 'body' not in event:
            logger.error("Request body not found in the event")
            raise KeyError('body')

        body = json.loads(event['body'])

        road = body

        if 'id' not in road:
            logger.error("El campo 'id' es requerido para eliminar una carretera")
            raise KeyError('id')

        delete_road(road['id'])

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Road deleted successfully"
            }),
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",  # Cambia esto a tu dominio si es necesario
                "Access-Control-Allow-Methods": "OPTIONS,DELETE"
            }
        }
    except KeyError as e:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message": "BAD_REQUEST",
                "error": str(e)
            }),
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",  # Cambia esto a tu dominio si es necesario
                "Access-Control-Allow-Methods": "OPTIONS,DELETE"
            }
        }
    except Exception as e:
        logger.error("Unexpected error: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "INTERNAL_SERVER_ERROR",
                "error": str(e)
            }),
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",  # Cambia esto a tu dominio si es necesario
                "Access-Control-Allow-Methods": "OPTIONS,DELETE"
            }
        }

def delete_road(road_id):
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM roads WHERE id = %s",
            (road_id,)
        )
        connection.commit()
        if cursor.rowcount == 0:
            logger.error("No rows affected. Road with ID %s not found.", road_id)
            raise ValueError("Road not found")

    except Exception as e:
        logger.error("Database transaction error: %s", str(e), exc_info=True)
        raise e

    finally:
        connection.close()
