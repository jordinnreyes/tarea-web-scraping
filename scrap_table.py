import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web con la tabla de COVID-19
    url = "https://www.worldometers.info/coronavirus/"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla de estadísticas
    table = soup.find('table', {'id': 'main_table_countries_today'})  # Tabla específica con id 'main_table_countries_today'
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla de estadísticas en la página web'
        }

    # Extraer las filas de la tabla (cada país)
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        if len(cells) > 1:  # Asegurarse de que la fila contiene datos
            country = cells[1].text.strip()  # Nombre del país
            total_cases = cells[2].text.strip()  # Casos totales
            total_deaths = cells[4].text.strip()  # Muertes totales
            total_recovered = cells[6].text.strip()  # Recuperados totales
            active_cases = cells[8].text.strip()  # Casos activos

            rows.append({
                'country': country,
                'total_cases': total_cases,
                'total_deaths': total_deaths,
                'total_recovered': total_recovered,
                'active_cases': active_cases,
            })

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('CovidStats')

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(
                Key={
                    'id': each['id']
                }
            )

    # Insertar los nuevos datos
    for row in rows:
        row['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
        table.put_item(Item=row)

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': rows
    }
