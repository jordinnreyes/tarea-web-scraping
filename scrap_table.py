import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web que contiene los sismos reportados
    url = "https://www.igp.gob.pe/servicios/centro-sismologico-nacional/ultimo-sismo/sismos-reportados"
    

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla de sismos usando el id
    table = soup.find('table', {'id': 'sismosreportados'})
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla de sismos en la página web'
        }

    # Extraer los encabezados de la tabla
    headers = [header.text.strip() for header in table.find_all('th')]

    # Extraer las filas de datos
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        if len(cells) == len(headers):  # Verifica que cada fila tenga todas las celdas necesarias
            rows.append({headers[i]: cells[i].text.strip() for i in range(len(headers))})

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    dynamo_table = dynamodb.Table('TablaWebScrapping2')

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = dynamo_table.scan()
    with dynamo_table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    # Insertar los nuevos datos en DynamoDB
    for row in rows:
        row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
        dynamo_table.put_item(Item=row)

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': rows
    }
