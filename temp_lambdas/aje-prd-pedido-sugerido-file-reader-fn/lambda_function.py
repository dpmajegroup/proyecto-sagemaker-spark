import json
import urllib.parse
import boto3
import os
import json
import requests
from orders_reader import OrdersFileReader, Fields
from datetime import datetime
import logging
import zipfile
import sys


ERROR_BATCH_SIZE = 1000
DELIMITER = ';'
MESSAGE_GROUP_ID = "1"
HEADER_ERROR_TYPE = 'header'
EXPIRATION_SECONDS=604799

sqs = boto3.client('sqs')
blocks_url = os.environ['blocks_url']
notifications_url = os.environ['notifications_url']
secret_id = os.environ['secret_id']

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secrets(secret_id):
    secretsmanager = boto3.client('secretsmanager')
    response = secretsmanager.get_secret_value(SecretId=secret_id)
    secret_string = response['SecretString']
    secrets = json.loads(secret_string)
    return secrets
    
    
def get_auth_info(secrets):
    salesforce_url = secrets['salesforce_url']
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    
    payload = {
        'grant_type': 'password',
        'client_id': secrets['client_id'],
        'client_secret': secrets['client_secret'],
        'username': secrets['username'],
        'password': secrets['password'],
        'grant_type':secrets['grant_type']
    }
    
    payload = "&".join([f"{key}={value}" for key, value in payload.items()])
    logger.info(f"Monitorero Request Salesforce: {payload}")
    try:
        response = requests.request("POST", salesforce_url, headers=headers, data=payload)
        auth_info = response.json()
    except requests.exceptions.RequestException as e:
        logger.error(e)
        message = f"Error de conexión: {e}"
        send_notification('Error en credenciales - Pedido Sugerido', message)
        return {}
    except ValueError:
        logger.error(response.text)
        message = "Error de conexión: url inexistente"
        send_notification('Error en credenciales - Pedido Sugerido', message)

    except Exception as e:
        message = "Error de conexión: solicitar revisión de logs"
        send_notification('Error en credenciales - Pedido Sugerido', message)
        return {}
    try:
        auth_info['orders_url'] = auth_info['instance_url'] + '/services/apexrest/PSPedidos'
    except Exception as e:
        message = "Error de autenticación:\n"
        for field, value in auth_info.items():
            message += f"{field}: {value}\n"
        logger.error(message)
        logger.error(e)
        send_notification('Error en consulta - Pedido Sugerido', message)
        return {}
    return auth_info


def get_s3_obj(event, bucket_name):
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    try:
        response = boto3.client('s3').get_object(Bucket=bucket_name, Key=key)
        return {'object': response, 'filename': key}
    except Exception as e:
        logger.error(e)
        logger.error(f"Error getting object {key} from bucket {bucket_name}.")
        raise e

def register_order_header(auth_info, header_dict, n_blocks):
    payload = {
        "encabezado": {
            "compania": header_dict['Compania'],
            "sucursal": header_dict['Sucursal'],
            "fecha": header_dict['Fecha'],
            "bloques": n_blocks
        }
    }
    headers = {
        'Authorization': f"Bearer {auth_info['access_token']}",
    }
    logger.info(f"payload encabezado: {payload}")
    try:
        response = requests.put(auth_info['orders_url'], headers=headers, json=payload)
    except requests.exceptions.RequestException as e:
        message = "Error en registro de cabecera\n"
        message += f"Error: {e}"
        send_notification('Error en consulta - Pedido Sugerido', message)
        logger.error(message)
        return [{"message": message}]
    logger.info(f"header register response: {response.json()}")
    return response.json()
    
def get_order_visits(auth_info, header_dict):
    url = auth_info['instance_url'] + '/services/apexrest/PSVisitas'
    payload = json.dumps({
        "pais": header_dict['Pais'],
        "compania": header_dict['Compania'],
        "sucursal": header_dict['Sucursal'],
        "fecha": header_dict['Fecha']
    })
    headers = {
        'Authorization': f"Bearer {auth_info['access_token']}",
        'Content-Type': 'application/json',
    }
    logger.info(f"payload visitas: {payload}")
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
    except requests.exceptions.RequestException as e:
        message = "Error en consulta de visitas\n"
        message += f"Error: {e}"
        send_notification('Error en consulta - Pedido Sugerido', message)
        logger.error(message)
        return [{"message": message}]
    logger.info(f"order visits response: {response.json()}")
    return response.json()


def start_sfn(message, name):
    client = boto3.client('stepfunctions')
    state_arn = os.environ['state_arn']
    response = client.start_execution(
        stateMachineArn=state_arn,
        name=name,
        input=json.dumps(message)
    )
    logger.info(f"step function response: {response}")
    
def send_sqs(queue_url, message, block_id):
    response = sqs.send_message(
        MessageGroupId=MESSAGE_GROUP_ID,
        MessageDeduplicationId=str(block_id),
        QueueUrl=queue_url,
        MessageBody=json.dumps(message)
    )
    logger.info(f"sqs response: {response}")

def clean_sqs(queue_url):
    response = sqs.purge_queue(QueueUrl=queue_url)
    logger.info(f"purged queue: {response}")
    
def send_notification(subject, message):
    sns = boto3.client('sns')
    try:
        response = sns.publish(
            TopicArn=os.environ['topic_arn'],
            Message=message,
            Subject=subject
        )
        logger.info(f"sns: {response}")
    except Exception as e:
        logger.error(e)

def send_parser_error(response_obj, header_info):
    header_dict, header_id = header_info
    error_message = header_dict.copy()
    error_message['Mensaje'] = ' - '.join([error['message'] for error in response_obj])
    logger.error(error_message)

    sqs_message = {
        "error_message": [error_message],
        "error_type": HEADER_ERROR_TYPE
    }
    send_sqs(notifications_url, sqs_message, f'{HEADER_ERROR_TYPE}_{header_id}')

def filter_orders_with_visit(visits_data, header_info, orders):
    actual_orders = []
    header_dict, header_id = header_info
    if len(visits_data) == 0:
        error_message = header_dict.copy()
        error_message['Mensaje'] = 'no tiene visitas'
        
        logger.warning(error_message)
        sqs_message = {
            "error_message": [error_message],
            "error_type": 'no visit'
        }
        send_sqs(notifications_url, sqs_message, f'header_{header_id}')
    else:
        visit_tuples = [(visit['cliente'], visit['modulo']) for visit in visits_data]
        visit_tuples = set(visit_tuples)

        no_client_visits = set()
        for order in orders:
            if (order['Cliente'], order['Modulo']) in visit_tuples:
                actual_orders.append(order)
            else:
                no_client_visits.add((order['Cliente'], order['Modulo']))
        
        if len(no_client_visits) > 0:
            error_messages = []
            for client_info in no_client_visits:
                error_message = header_dict.copy()
                error_message.update({
                    'Cliente': client_info[0],
                    'Modulo': client_info[1],
                    'Mensaje': 'no tiene visita'
                })

                logger.warning(error_message)
                error_messages.append(error_message)

            error_batches = [error_messages[i:(i + ERROR_BATCH_SIZE)] for i in range(0, len(error_messages), ERROR_BATCH_SIZE)]
            for error_batch in error_batches:
                sqs_message = {
                    "error_message": error_batch,
                    "error_type": 'no visit'
                }
                send_sqs(notifications_url, sqs_message, f'header_{header_id}')
    return actual_orders

def save_errors(bucket_name, filepath, log_message, start_time):
    s3 = boto3.client('s3')
    
    filedir = os.path.dirname(filepath).replace(f"/pedidos", f"/errores/validacion")
    filename = 'error_' + os.path.basename(filepath).replace('.csv', f"_{start_time}.csv")

    temp_csv = '/tmp/' + filename
    with open(temp_csv, 'w') as f:
        f.write(log_message)
    
    file_zip = filename.replace('.csv', '.zip')
    temp_zip = '/tmp/' + file_zip

    with zipfile.ZipFile(temp_zip, 'w', compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zip:
        zip.write(temp_csv, arcname=filename)

    with open(temp_zip, 'rb') as f:
        key = os.path.join(filedir, file_zip)
        response = s3.put_object(
            Body=f,
            Bucket=bucket_name,
            Key=key
        )
    logger.info(f"put object: {response}")
    
    return s3.generate_presigned_url(
        'get_object',
        Params = {'Bucket': bucket_name, 'Key': key},
        ExpiresIn=EXPIRATION_SECONDS
    )


def lambda_handler(event, context):
    print(event)
    print(context)
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    secrets = get_secrets(secret_id)
    
    # Purge queues before sending messages
    try:
        clean_sqs(blocks_url)
        clean_sqs(notifications_url)
    except Exception as e:
        logger.error(e)

    auth_info = get_auth_info(secrets)
    if not auth_info:
        logger.error("No se obtuvo auth_info")
        return {
            "error": True
        }

    s3_response = get_s3_obj(event, bucket_name)
    s3_obj = s3_response['object']
    filepath = s3_response['filename']
    
    start_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    orders_file_reader = OrdersFileReader(logger=logger)
    try:
        validation = orders_file_reader.validate_file(s3_obj)
    except Exception as e:
        logger.error(f"FATAL ERROR en el lector: {str(e)}")
        return {"error": True}

    if validation['error']:
        message = f"{filepath} no es un csv válido.\n"
        message += f"\n{validation['summary']}\n"
        
        error_messages = validation['messages']

        log_message = f"Fila{DELIMITER}Error\n"
        for error_message in error_messages:
            log_message += f'{error_message}\n'

        link = save_errors(bucket_name, filepath, log_message, start_time)
        message += f'\nDetalle de errores: {link}\n'
        
        send_notification(
            subject=f'Reporte de subida de archivo {filepath}',
            message=message
        )
        return {
            "error": True
        }

    block_size = int(os.environ['block_size'])
    company_orders = orders_file_reader.get_orders()
    logger.info(f"Compañías encontradas: {len(company_orders)}")
    blocks = []
    for header_id, (header_data, orders) in enumerate(company_orders.items()):
        header_dict = Fields.format_header_data(header_data)
        response_obj = get_order_visits(auth_info, header_dict)
        print('response_obj')
        print(response_obj)
        print('orders')
        print(orders)
        header_info = header_dict, header_id
        if isinstance(response_obj, list):
            send_parser_error(response_obj, header_info)
        elif response_obj['error']:
            error_message = header_dict.copy()
            error_message['Mensaje'] = response_obj['errors']
            logger.error(error_message)
            sqs_message = {
                "error_message": [error_message],
                "error_type": HEADER_ERROR_TYPE
            }
            send_sqs(notifications_url, sqs_message, f'{HEADER_ERROR_TYPE}_{header_id}')

        else:
            visits_data = response_obj['visita']
            actual_orders = filter_orders_with_visit(visits_data, header_info, orders)
            if len(actual_orders) > 0:    
                print('Existen Ordenes con Visita')
                block_orders = [actual_orders[i:i + block_size] for i in range(0, len(actual_orders), block_size)]
                response_obj = register_order_header(auth_info, header_dict, len(block_orders))
                
                if isinstance(response_obj, list):
                    send_parser_error(response_obj, header_info)
                elif response_obj['errors']:
                    error_message = header_dict.copy()
                    error_message['Mensaje'] = ' - '.join(response_obj["errors"])
                    logger.error(error_message)
                    sqs_message = {
                        "error_message": [error_message],
                        "error_type": HEADER_ERROR_TYPE
                    }
                    send_sqs(notifications_url, sqs_message, f'header_{header_id}')
                else:
                    blocks.extend(block_orders)
    
    # for block_id, block_orders in enumerate(blocks):
    #     dict_obj = {
    #         'orders': block_orders,
    #         'block_id': block_id,
    #         'auth_info': auth_info
    #     }
    #     send_sqs(blocks_url, dict_obj, block_id)
    
    for block_id, block_orders in enumerate(blocks):
        # Reducimos auth_info a lo mínimo para ahorrar espacio
        compact_auth = {
            'access_token': auth_info.get('access_token'),
            'instance_url': auth_info.get('instance_url'),
            'orders_url': auth_info.get('orders_url')
        }
        
        dict_obj = {
            'orders': block_orders,
            'block_id': block_id,
            'auth_info': compact_auth  # Enviamos la versión ligera
        }
        
        # Validación de seguridad antes de enviar
        if len(json.dumps(dict_obj)) > 260000:
            logger.error(f"BLOQUE {block_id} DEMASIADO GRANDE ({len(json.dumps(dict_obj))} bytes).")
            continue 
            
        send_sqs(blocks_url, dict_obj, block_id)

    total_blocks = len(blocks)
    logger.info(f"Total blocks: {total_blocks}")
    start_sfn({
        'is_empty': False,
        'filename': filepath,
        'total_blocks': total_blocks,
        'start_time': start_time
    }, name=start_time)