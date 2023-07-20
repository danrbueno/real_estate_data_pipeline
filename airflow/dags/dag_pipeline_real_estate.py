import pandas as pd

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.settings import DAGS_FOLDER
from airflow.utils.dates import days_ago


def transform_rentals():
    pd.set_option('display.max_colwidth', 0)
    input_file = "/home/daniel/repositorios/real_estate/data/web/rentals.json"
    output_file = "/home/daniel/repositorios/real_estate/data/staging/rentals.csv"

    df = pd.read_json(input_file, lines=True, dtype=False)
    df = pd.json_normalize(df.data)

    # Deleting unnecessary columns
    df.drop(columns=["codigo", 
                    "ultima_atualizacao", 
                    "nome_do_edificio", 
                    "total_de_andar_do_empreendimento", 
                    "posicao_do_sol",
                    "posicao_do_imovel"],
            inplace=True)

    # Renaming column names
    df.rename(columns={"key_1"             : "price",
                    "area_util"            : "useful_area",
                    "valor_rs_m2"          : "m2_value",
                    "bairro"               : "neighborhood",
                    "cidade"               : "city",
                    "quartos"              : "rooms",
                    "garagens"             : "parking",
                    "condominio_rs"        : "condo_value",
                    "suites"               : "suites",
                    "area_total"           : "total_area",
                    "iptu_rs"              : "iptu",
                    "andar_do_apartamento" : "floor",
                    "unidades_no_andar"    : "floor_properties",
                    "aceita_financiamento" : "financing",
                    "aceita_permuta"       : "exchange",
                    "agio_rs"              : "agio",
                    "area_terreno"         : "land_area"}, 
                inplace=True)

    # Change column types to number types.
    # Due to pandas convertion float NaN to integer:
    # 1) For int types, use fillna method before transforming
    # 2) For float types, use fillna method after transforming

    df.price = df.price.str.replace("[.]","",regex=True)
    df.price = df.price.astype(float)
    df.price.fillna(value=0, inplace=True)

    df.useful_area = df.useful_area.str.replace("[.]","",regex=True)
    df.useful_area = df.useful_area.str.replace("[,]",".",regex=True)
    df.useful_area = df.useful_area.str.replace("[ m²]","",regex=True)
    df.useful_area = df.useful_area.astype(float)
    df.useful_area.fillna(value=0, inplace=True)

    df.m2_value = df.m2_value.astype(float)
    df.m2_value.fillna(value=0, inplace=True)

    df.rooms.fillna(value=0, inplace=True)
    df.rooms = df.rooms.astype(int)

    df.parking.fillna(value=0, inplace=True)
    df.parking = df.parking.astype(int)

    df.condo_value = df.condo_value.str.replace("[.]","",regex=True)
    df.condo_value = df.condo_value.str.replace("[,]",".",regex=True)
    df.condo_value = df.condo_value.astype(float)
    df.condo_value.fillna(value=0, inplace=True)

    df.total_area = df.total_area.str.replace("[.]","",regex=True)
    df.total_area = df.total_area.str.replace("[,]",".",regex=True)
    df.total_area = df.total_area.str.replace("[ m²]","",regex=True)
    df.total_area = df.total_area.astype(float)
    df.total_area.fillna(value=0, inplace=True)

    df.iptu = df.iptu.str.replace("[.]","",regex=True)
    df.iptu = df.iptu.str.replace("[,]",".",regex=True)
    df.iptu = df.iptu.astype(float)
    df.iptu.fillna(value=0, inplace=True)

    df.suites.fillna(value=0, inplace=True)
    df.suites = df.suites.astype(int)

    df.floor.fillna(value=0, inplace=True)
    df.floor = df.floor.str.replace("[\D]", "", regex=True)
    df.floor = df.floor.fillna(value=0)
    df.floor = df.floor.astype(int)

    df.land_area = df.land_area.str.replace("[.]","",regex=True)
    df.land_area = df.land_area.str.replace("[,]",".",regex=True)
    df.land_area = df.land_area.str.replace("[ m²]","",regex=True)
    df.land_area = df.land_area.astype(float)
    df.land_area.fillna(value=0, inplace=True)    

    df.fillna(value="", inplace=True)

    # Dropping properties registered with rental value upper than R$ 10.000,00 (maybe due to digitation error)
    drop_rental_value = df.loc[df.price >= 10000]
    df.drop(drop_rental_value.index, inplace=True)

    # Deleting data with condo values upper then 3000
    drop_condo_value = df.loc[df.condo_value >= 3000]
    df.drop(drop_condo_value.index, inplace=True)

    # Deleting data with total area upper than 5000 m²
    drop_total_area = df.loc[df.total_area >= 5000]
    df.drop(drop_total_area.index, inplace=True)

    # End cleaning, reset index and save to a CSV file
    df.reset_index(inplace=True, drop=True)
    df.to_csv(output_file, index=False)

args = {"owner": "daniel", "start_date": days_ago(1)}

dag = DAG(dag_id="dag_pipeline_real_estate", default_args=args, schedule_interval=None)

with dag:
    # Scrap data from website https://www.dfimoveis.com.br/aluguel/df/todos/apartamento
    # The output file is defined in the scope of scraping project (/airflow/scraping/scraping/pipelines.py)
    task_scrap_rentals = BashOperator(
        task_id="scrap_rentals",
        bash_command="cd /home/daniel/repositorios/real_estate/scrapy && scrapy crawl DFImoveis -a category=rentals",
        task_concurrency=2
    )

    # Scrap data from website https://www.dfimoveis.com.br/venda/df/todos/apartamento
    # The output file is defined in the scope of scraping project (/airflow/scraping/scraping/pipelines.py)
    task_scrap_sales = BashOperator(
        task_id="scrap_sales",
        bash_command="cd /home/daniel/repositorios/real_estate/scrapy && scrapy crawl DFImoveis -a category=sales",
        task_concurrency=2
    )    

    # Adjustments and load dataset into a CSV file
    task_transform_rentals = PythonOperator(
        task_id = "transform_rentals",
        python_callable=transform_rentals,
        task_concurrency=2
    )

    # Task execution order
    task_scrap_rentals >> task_transform_rentals
    task_scrap_sales