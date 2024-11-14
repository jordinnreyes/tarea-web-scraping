import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web de FIFA World Ranking
    url = "https://inside.fifa.com/fifa-world-ranking"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla de clasificaciones
    table = soup.find('table', {'class': 'ranking_table'})  # Aquí debes asegurarte de que la clase sea correcta
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla de clasificación en la página web'
        }

    # Extraer las filas de la tabla (cada selección)
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        if len(cells) >= 4:  # Asegurarse de que hay suficiente información (por ejemplo, posición, equipo, puntos, etc.)
            rank = cells[0].text.strip()  # La posición del equipo
            country = cells[1].text.strip()  # El nombre del país
            points = cells[2].text.strip()  # Los puntos
            movement = cells[3].text.strip()  # El movimiento (ascenso o descenso)

            rows.append({
                'rank': rank,
                'country': country,
                'points': points,
                'movement': movement
            })

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaIMDBTopMovies')

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
