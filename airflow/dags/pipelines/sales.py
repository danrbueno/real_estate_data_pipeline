import os
from pathlib import Path
import pandas as pd

def transform():    
    main_dir = str(Path(os.path.dirname(__file__)).parent.parent.parent)
    data_web_dir = main_dir+"/data/web"
    data_staging_dir = main_dir+"/data/staging"
    input_file = data_web_dir+"/sales.json"
    output_file = data_staging_dir+"/sales.csv"

    pd.set_option("display.max_colwidth", 0)
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
            "andar_do_apartamento": "floor"
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
            "agio_rs",
            "unidades_no_andar",
            "aceita_financiamento",
            "aceita_permuta",
            "area_terreno"            
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

    df.fillna(value="", inplace=True)

    # Delete apartments with price higher than R$ 10.000.000
    drop_higher_prices = df.loc[df.price >= 10000000]
    df.drop(drop_higher_prices.index, inplace=True)

    # Delete apartments with price lower than R$ 10.000
    drop_lower_prices = df.loc[df.price <= 10000]
    df.drop(drop_lower_prices.index, inplace=True)

    # End cleaning, reset index and save to a CSV file
    df.reset_index(inplace=True, drop=True)
    df["transaction_type"] = "sale"
    df.to_csv(output_file, index=False)

if __name__=="__main__":
    transform()    