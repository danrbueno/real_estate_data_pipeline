import os
from pathlib import Path

import pandas as pd
import pendulum

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

main_dir = str(Path(os.path.dirname(__file__)).parent.parent)
data_web_dir = main_dir+"/data/web"
data_staging_dir = main_dir+"/data/staging"
scrapy_dir = main_dir+"/scrapy"

def transform_rentals():
    pd.set_option("display.max_colwidth", 0)
    input_file = data_web_dir+"/rentals.json"
    output_file = data_staging_dir+"/rentals.csv"

    df = pd.read_json(input_file, lines=True, dtype=False)
    df = pd.json_normalize(df.data)

    # Deleting unnecessary columns
    df.drop(
        columns=[
            "codigo",
            "ultima_atualizacao",
            "nome_do_edificio",
            "total_de_andar_do_empreendimento",
            "posicao_do_sol",
            "posicao_do_imovel",
        ],
        inplace=True,
    )

    # Renaming column names
    df.rename(
        columns={
            "key_1": "price",
            "area_util": "useful_area",
            "valor_rs_m2": "m2_value",
            "bairro": "neighborhood",
            "cidade": "city",
            "quartos": "rooms",
            "garagens": "parking",
            "condominio_rs": "condo_value",
            "suites": "suites",
            "area_total": "total_area",
            "iptu_rs": "iptu",
            "andar_do_apartamento": "floor",
            "unidades_no_andar": "floor_properties",
            "aceita_financiamento": "financing",
            "aceita_permuta": "exchange",
            "agio_rs": "agio",
            "area_terreno": "land_area",
        },
        inplace=True,
    )

    # Change column types to number types.
    # Due to pandas convertion float NaN to integer:
    # 1) For int types, use fillna method before transforming
    # 2) For float types, use fillna method after transforming

    df.price = df.price.str.replace("[.]", "", regex=True)
    df.price = df.price.astype(float)
    df.price.fillna(value=0, inplace=True)

    df.useful_area = df.useful_area.str.replace("[.]", "", regex=True)
    df.useful_area = df.useful_area.str.replace("[,]", ".", regex=True)
    df.useful_area = df.useful_area.str.replace("[ m²]", "", regex=True)
    df.useful_area = df.useful_area.astype(float)
    df.useful_area.fillna(value=0, inplace=True)

    df.m2_value = df.m2_value.astype(float)
    df.m2_value.fillna(value=0, inplace=True)

    df.rooms.fillna(value=0, inplace=True)
    df.rooms = df.rooms.astype(int)

    df.parking.fillna(value=0, inplace=True)
    df.parking = df.parking.astype(int)

    df.condo_value = df.condo_value.str.replace("[.]", "", regex=True)
    df.condo_value = df.condo_value.str.replace("[,]", ".", regex=True)
    df.condo_value = df.condo_value.astype(float)
    df.condo_value.fillna(value=0, inplace=True)

    df.total_area = df.total_area.str.replace("[.]", "", regex=True)
    df.total_area = df.total_area.str.replace("[,]", ".", regex=True)
    df.total_area = df.total_area.str.replace("[ m²]", "", regex=True)
    df.total_area = df.total_area.astype(float)
    df.total_area.fillna(value=0, inplace=True)

    df.iptu = df.iptu.str.replace("[.]", "", regex=True)
    df.iptu = df.iptu.str.replace("[,]", ".", regex=True)
    df.iptu = df.iptu.astype(float)
    df.iptu.fillna(value=0, inplace=True)

    df.suites.fillna(value=0, inplace=True)
    df.suites = df.suites.astype(int)

    df.floor.fillna(value=0, inplace=True)
    df.floor = df.floor.str.replace("[\D]", "", regex=True)
    df.floor = df.floor.fillna(value=0)
    df.floor = df.floor.astype(int)

    df.land_area = df.land_area.str.replace("[.]", "", regex=True)
    df.land_area = df.land_area.str.replace("[,]", ".", regex=True)
    df.land_area = df.land_area.str.replace("[ m²]", "", regex=True)
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

def transform_sales():    

    data_web_dir = main_dir+"/data/web"
    data_staging_dir = main_dir+"/data/staging"

    pd.set_option("display.max_colwidth", 0)

    input_file = data_web_dir+"/sales.json"
    output_file = data_staging_dir+"/sales.csv"

    df = pd.read_json(input_file, lines=True, dtype=False)
    df = pd.json_normalize(df.data)

    # Renaming column names
    df.rename(
        columns={
            "rs": "price",
            "area_util": "useful_area",
            "valor_rs_m2": "m2_value",
            "bairro": "neighborhood",
            "cidade": "city",
            "quartos": "rooms",
            "garagens": "parking",
            "condominio_rs": "condo_value",
            "suites": "suites",
            "area_total": "total_area",
            "iptu_rs": "iptu",
            "andar_do_apartamento": "floor",
            "unidades_no_andar": "floor_properties",
            "aceita_financiamento": "financing",
            "aceita_permuta": "exchange",
            "area_terreno": "land_area",
        },
        inplace=True,
    )

    # Delete rows with prices on request 
    on_request = df.loc[df.price=="Sob Consulta"]
    df.drop(on_request.index, inplace=True)

    # Drop rows with nan values for rooms
    nan_rooms = df.loc[df.rooms.isnull()]
    df.drop(nan_rooms.index, inplace=True)

    # Drop rows with no m2_value
    nan_m2_value = df.loc[df.m2_value == ""]
    df.drop(nan_m2_value.index, inplace=True)

    # During the scraping, some prices goes to 'rs' and other goes to 'key_1'.
    # Let's put this information in just one column
    df["price"] = df.apply(lambda x: x["key_1"] if pd.isna(x["price"]) else x["price"], axis=1)
    df["key_1"] = df.apply(lambda x: 0 if x["key_1"]==x["price"] else x["key_1"], axis=1)

    # Deleting unnecessary columns
    df.drop(
        columns=[
            "key_1",
            "key_2",
            "key_3",        
            "codigo",
            "ultima_atualizacao",
            "nome_do_edificio",
            "total_de_andar_do_empreendimento",
            "posicao_do_sol",
            "posicao_do_imovel",
            "memorial_de_incorporacao",
            "agio_rs"        
        ],
        inplace=True,
    )

    # Change column types to number types.
    # Due to pandas convertion float NaN to integer:
    # 1) For int types, use fillna method before transforming
    # 2) For float types, use fillna method after transforming

    df.price = df.price.str.replace("[.]", "", regex=True)
    df.price = df.price.astype(float)
    df.price.fillna(value=0, inplace=True)

    df.useful_area = df.useful_area.str.replace("[.]", "", regex=True)
    df.useful_area = df.useful_area.str.replace("[,]", ".", regex=True)
    df.useful_area = df.useful_area.str.replace("[ m²]", "", regex=True)
    df.useful_area = df.useful_area.astype(float)
    df.useful_area.fillna(value=0, inplace=True)

    df.m2_value = df.m2_value.str.replace("[.]", "", regex=True)
    df.m2_value = df.m2_value.str.replace("[,]", ".", regex=True)
    df.m2_value = df.m2_value.astype(float)
    df.m2_value.fillna(value=0, inplace=True)

    df.rooms.fillna(value=0, inplace=True)
    df.rooms = df.rooms.astype(int)

    df.parking.fillna(value=0, inplace=True)
    df.parking = df.parking.astype(int)

    df.condo_value = df.condo_value.str.replace("[.]", "", regex=True)
    df.condo_value = df.condo_value.str.replace("[,]", ".", regex=True)
    df.condo_value = df.condo_value.astype(float)
    df.condo_value.fillna(value=0, inplace=True)

    df.total_area = df.total_area.str.replace("[.]", "", regex=True)
    df.total_area = df.total_area.str.replace("[,]", ".", regex=True)
    df.total_area = df.total_area.str.replace("[ m²]", "", regex=True)
    df.total_area = df.total_area.astype(float)
    df.total_area.fillna(value=0, inplace=True)

    df.iptu = df.iptu.str.replace("[.]", "", regex=True)
    df.iptu = df.iptu.str.replace("[,]", ".", regex=True)
    df.iptu = df.iptu.astype(float)
    df.iptu.fillna(value=0, inplace=True)

    df.suites.fillna(value=0, inplace=True)
    df.suites = df.suites.astype(int)

    df.floor.fillna(value=0, inplace=True)
    df.floor = df.floor.str.replace("[\D]", "", regex=True)
    df.floor = df.floor.fillna(value=0)
    df.floor = df.floor.astype(int)

    df.land_area = df.land_area.str.replace("[.]", "", regex=True)
    df.land_area = df.land_area.str.replace("[,]", ".", regex=True)
    df.land_area = df.land_area.str.replace("[ m²]", "", regex=True)
    df.land_area = df.land_area.astype(float)
    df.land_area.fillna(value=0, inplace=True)

    df.fillna(value="", inplace=True)

    # Delete apartments with price higher than R$ 10.000.000
    drop_higher_prices = df.loc[df.price >= 10000000]
    df.drop(drop_higher_prices.index, inplace=True)

    # Delete apartments with price lower than R$ 10.000
    drop_lower_prices = df.loc[df.price <= 10000]
    df.drop(drop_lower_prices.index, inplace=True)

    # End cleaning, reset index and save to a CSV file
    df.reset_index(inplace=True, drop=True)
    df.to_csv(output_file, index=False)


dag = DAG(
    dag_id="dag_real_estate_data_pipeline",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 7, 20)
)

with dag:
    task_start_dag = EmptyOperator(task_id="start_dag")

    # Scrap data from website https://www.dfimoveis.com.br/aluguel/df/todos/apartamento
    # The output file is defined in the scope of scraping project (/airflow/scraping/scraping/pipelines.py)
    task_scrap_rentals = BashOperator(
        task_id="scrap_rentals",
        bash_command="cd "+scrapy_dir+" && scrapy crawl DFImoveis -a category=rentals",
    )

    # Scrap data from website https://www.dfimoveis.com.br/venda/df/todos/apartamento
    # The output file is defined in the scope of scraping project (/airflow/scraping/scraping/pipelines.py)
    task_scrap_sales = BashOperator(
        task_id="scrap_sales",
        bash_command="cd "+scrapy_dir+" && scrapy crawl DFImoveis -a category=sales",
    )

    # Adjustments and load dataset into a CSV file
    task_transform_rentals = PythonOperator(
        task_id="transform_rentals", python_callable=transform_rentals
    )

    # Adjustments and load dataset into a CSV file
    task_transform_sales = PythonOperator(
        task_id="transform_sales", python_callable=transform_rentals
    )    

    # Task execution order
    task_start_dag >> task_scrap_rentals >> task_transform_rentals
    task_start_dag >> task_scrap_sales >> task_transform_sales
