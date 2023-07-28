import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from pipelines.models.base import Base, Session, engine
from pipelines.models.city import City
from pipelines.models.neighborhood import Neighborhood
from pipelines.models.property import Property
from pipelines.models.transaction_type import TransactionType


def reset():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

def load():
    main_dir = str(Path(os.path.dirname(__file__)).parent.parent.parent)
    csv_file = main_dir+"/data/staging/all_data.csv"
    df = pd.read_csv(csv_file)
    
    # Add all transactions types into MySQL
    session = Session()
    transaction_types = [TransactionType(transaction_type) 
                         for transaction_type in df.transaction_type.unique()]
    session.add_all(transaction_types)

    # Add all cities into MySQL
    cities = [City(city) for city in df.city.unique()]
    session.add_all(cities)

    # Add all neighborhoods into MySQL, using the cities added above
    columns = ['neighborhood','city']
    df_neighborhoods = df.drop_duplicates(subset = columns).reset_index()
    neighborhoods = [Neighborhood(name=item.neighborhood, 
                                  city=session.query(City).filter_by(name=item.city).first())
                     for item in df_neighborhoods.itertuples()]
    session.add_all(neighborhoods)

    # Add all rows to properties table in MySQL, using all the relationships added above

    # print([datetime.strptime(item.scraped_at, "%Y-%m-%d %H:%M:%S") for item in df.itertuples()])

    properties = [Property(scraped_at=datetime.strptime(item.scraped_at, "%Y-%m-%d %H:%M:%S"),
                           link=item.link,
                           title=item.title,
                           price=item.price,
                           useful_area=item.useful_area,
                           m2_value=item.m2_value,
                           rooms=item.rooms,
                           parking=item.parking,
                           condo_value=item.condo_value,
                           total_area=item.total_area,
                           suites=item.suites,
                           iptu=item.iptu,
                           floor=item.floor,
                           transaction_type=session.query(TransactionType).filter_by(name=item.transaction_type).first(),
                           neighborhood=session.query(Neighborhood).filter_by(name=item.neighborhood).first()
                           ) 
                  for item in df.itertuples()]
    session.add_all(properties)
    
    session.commit()