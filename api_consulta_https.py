import requests

def api_people(pagina):
    url = f'https://swapi.dev/api/people/{pagina}/'
    
    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()  
        return data
    except requests.exceptions.RequestException as e:
        print(f"Ocorreu um erro na requisição: {e}")
        return None

def api_planets(pagina):
    url = f'https://swapi.dev/api/planets/{pagina}/'
    
    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()  
        return data
    except requests.exceptions.RequestException as e:
        print(f"Ocorreu um erro na requisição: {e}")
        return None

def api_films(pagina):
    url = f'https://swapi.dev/api/films/{pagina}/'
    
    try:
        response = requests.get(url)
        response.raise_for_status()  
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Ocorreu um erro na requisição: {e}")
        return None

