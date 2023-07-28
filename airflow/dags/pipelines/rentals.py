import os
from pathlib import Path

import pandas as pd

def transform():
    main_dir = str(Path(os.path.dirname(__file__)).parent.parent.parent)
    data_web_dir = main_dir+"/data/web"
    data_staging_dir = main_dir+"/data/staging"    
    input_file = data_web_dir+"/rentals.json"
    output_file = data_staging_dir+"/rentals.csv"

    pd.set_option("display.max_colwidth", 0)
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
            "unidades_no_andar",
            "aceita_financiamento",
            "aceita_permuta",
            "area_terreno"
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
            "andar_do_apartamento": "floor"
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
    df["transaction_type"] = "rental"
    df.to_csv(output_file, index=False)