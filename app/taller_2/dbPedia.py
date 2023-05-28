import requests


CONFIDENCE = '0'
SUPPORT = '10'
BASE_URL = 'http://api.dbpedia-spotlight.org/es/annotate?text={text}&confidence={confidence}&support={support}'


def consultar_dbpedia_spotlight(texto):

    # Realizar la solicitud GET al API
    # print(texto)
    REQUEST = BASE_URL.format(
        text=texto,
        confidence=CONFIDENCE,
        support=SUPPORT
    )
    HEADERS = {'Accept': 'application/json'}
    response = requests.get(url=REQUEST, headers=HEADERS)

    # Verificar el estado de la respuesta
    if response.status_code == 200:
        # Obtener los resultados de la respuesta
        # print(response)
        try:
            data = response.json()
            return data
        except ValueError:
            print('La respuesta no es un JSON válido')
            return None
    else:
        print('Error en la solicitud:', response.status_code)
        return None
