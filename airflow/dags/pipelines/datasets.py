import os
from pathlib import Path
import pandas as pd

def join():
    main_dir = str(Path(os.path.dirname(__file__)).parent.parent.parent)
    df_rentals = pd.read_csv(main_dir+"/data/staging/rentals.csv")
    df_sales = pd.read_csv(main_dir+"/data/staging/sales.csv")
    output_file = main_dir+"/data/staging/all_data.csv"
    df_all_data = pd.concat([df_rentals, df_sales], ignore_index=True)
    df_all_data.to_csv(output_file, index=False)