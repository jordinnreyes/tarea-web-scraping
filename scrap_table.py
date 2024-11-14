import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web de la Premier League
    url = "https://www.espn.com/soccer/table/_/league/ENG.1"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla de posiciones de la Premier League
    table = soup.find('table', {'class': 'Table'})
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla de posiciones en la página web'
        }

    # Extraer las filas de la tabla (cada equipo)
    rows = []
    for row in table.find_all('tr')[1:]:  # Omitir el encabezado
        cells = row.find_all('td')
        if len(cells) >= 7:  # Asegurarse de que hay suficiente información
            position = cells[0].text.strip()
            team = cells[1].text.strip()
            played = cells[2].text.strip()
            won = cells[3].text.strip()
            drawn = cells[4].text.strip()
            lost = cells[5].text.strip()
            points = cells[6].text.strip()

            rows.append({
                'position': position,
                'team': team,
                'played': played,
                'won': won,
                'drawn': drawn,
                'lost': lost,
                'points': points,
            })

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('PremierLeagueRankings')

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
