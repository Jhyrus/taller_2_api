import requests

CONFIDENCE = '0.5'  # Ajusta la confianza deseada (valor entre 0 y 1)
SUPPORT = '20'  # Ajusta el soporte deseado (valor numérico)
BASE_URL = 'http://api.dbpedia-spotlight.org/es/annotate?text={text}&confidence={confidence}&support={support}'


def consultar_dbpedia_spotlight(texto):
    # Realizar la solicitud GET al API
    REQUEST = BASE_URL.format(
        text=texto,
        confidence=CONFIDENCE,
        support=SUPPORT
    )
    HEADERS = {'Accept': 'application/json'}
    response = requests.get(url=REQUEST, headers=HEADERS)

    # Verificar el estado de la respuesta y obtener los resultados
    if response.status_code == 200:
        try:
            data = response.json()
            return data
        except ValueError:
            print('La respuesta no es un JSON válido')
            return None
    else:
        print('Error en la solicitud:', response.status_code)
        return None
