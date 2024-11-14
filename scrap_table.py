import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web de Time and Date con la información del clima
    url = "https://www.timeanddate.com/weather/"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la sección de clima actual
    weather_section = soup.find('div', {'class': 'h2'})
    if not weather_section:
        return {
            'statusCode': 404,
            'body': 'No se encontró la información del clima en la página web'
        }

    # Extraer el nombre de la ciudad, la temperatura y el estado del clima
    city_name = weather_section.find_previous('h1').text.strip()
    temperature = weather_section.text.strip()
    weather_description = soup.find('p', {'class': 'h2'}).text.strip()  # Descripción del clima

    # Organizar los datos en un diccionario
    weather_data = {
        'city': city_name,
        'temperature': temperature,
        'weather_description': weather_description,
    }

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('WeatherData')

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
    weather_data['id'] = str(uuid.uuid4())  # Generar un ID único para cada entrada
    table.put_item(Item=weather_data)

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': weather_data
    }
