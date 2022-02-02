from airflow.models import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pandas import json_normalize
from datetime import datetime
import json


default_args = {
    'start_date': datetime(2022, 1, 31)
}


#Função para processar os filmes e armazenar num arquivo .csv
def _process_films(ti):
    films = ti.xcom_pull(task_ids=['extract_films'])
    if not len(films) or 'results' not in films[0]:
        raise ValueError('Film is empty')
        film = films[0]['results'][0]
        processed_film = json_normalize({
            'episode_id': film['episode_id'],
            'opening_crawl': film['opening_crawl']['opening_crawl'],
            'director': film['director']['director'],
            'producer': film['producer']['producer'],
            'release_date': film['release_date']['release_date'],
            'characters': film['characters']['characters'],
			'planets': film['planets']['planets'],
			'starships': film['starships']['starships'],
			'vehicles': film['vehicles']['vehicles'],
			'created': film['created']['created'],
			'edited': film['edited']['edited'],
			'url': film['url']['url']
        })
        processed_film.to_csv('dags\processed_film.csv', index=None, header=False)

with DAG('process_films', schedule_interval='@daily',  default_args=default_args, catchup=False) as dag:

        #Task para criar tabela
        creating_table = SqliteOperator(
        task_id='creating_table',
        sqlite_conn_id='db_sqlite',
        sql='''
                CREATE TABLE film(
                episode_id TEXT NOT NULL PRIMARY KEY,
                opening_crawl TEXT NOT NULL,
                director TEXT NOT NULL,
                producer TEXT NOT NULL,
                release_date TEXT NOT NULL,
                characters TEXT NOT NULL,
				planets TEXT NOT NULL,
				starships TEXT NOT NULL,
				vehicles TEXT NOT NULL,
				species TEXT NOT NULL,
				created TEXT NOT NULL,
				edited TEXT NOT NULL,
				url TEXT NOT NULL 
                );
        '''
        )
        #Task para verificar se API está disponível
        is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='starwars_api',
        endpoint='api/films'
    )
        #Task para puxar arquivo Json da API
        extract_films = SimpleHttpOperator(
        task_id='extract_films',
        http_conn_id='starwars_api',
        endpoint='api/films',
        method='GET',
        response_filter= lambda response: json.loads(response.text),
        log_response=True
    )
        #Task que chama função para processar os filmes e armazenar num arquivo .csv
        processing_films = PythonOperator(
        task_id='processing_films',
        python_callable=_process_films
    )

        store_films = BashOperator(
        task_id='store_films',
        bash_command='echo -e ".separator ","\n.import dags\process_films.py films" | sqlite3 dags\airflow.db'
        )
    
        #Dependências
        creating_table >> is_api_available >> extract_films >> processing_films >> store_films
    