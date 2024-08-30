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

        required_keys = ['id', 'nombre', 'tipo', 'longitud', 'velocidad_maxima']
        for key in required_keys:
            if key not in road:
                logger.error(f"El campo {key} es requerido para actualizar una carretera")
                raise KeyError(key)

        update_road(road)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Road updated successfully",
                "road": road
            }),
            "headers": {
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Origin": "*",  # Cambia esto a tu dominio si es necesario
                "Access-Control-Allow-Methods": "OPTIONS,POST,PUT,GET"
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
                "Access-Control-Allow-Methods": "OPTIONS,POST,PUT,GET"
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
                "Access-Control-Allow-Methods": "OPTIONS,POST,PUT,GET"
            }
        }

def update_road(road):
    connection = pymysql.connect(host=rds_host, user=rds_user, password=rds_password, db=rds_db)
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE roads
            SET nombre = %s, tipo = %s, longitud = %s, velocidad_maxima = %s
            WHERE id = %s
            """,
            (road['nombre'], road['tipo'], road['longitud'], road['velocidad_maxima'], road['id'])
        )
        connection.commit()
        if cursor.rowcount == 0:
            logger.error("No rows affected. Road with ID %s not found.", road['id'])
            raise ValueError("Road not found")

    except Exception as e:
        logger.error("Database transaction error: %s", str(e), exc_info=True)
        raise e

    finally:
        connection.close()
