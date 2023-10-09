import pandas as pd


def consulta_api():
    from api_consulta_https import api_people, api_planets, api_films

    a = api_people(1)
    df = pd.DataFrame(a)



    b = api_planets(1)
    c = api_films(1)

consulta_api()