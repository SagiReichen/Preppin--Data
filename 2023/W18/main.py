#%%
from pathlib import Path

import polars as pl

data_file_path = Path('Messy Nut House Data.csv').absolute()


# using a dict for mapping the cities (a list can not be used in dict.keys)
location_mapping = {
            ("L0ndon", "London", "londen", "L0ndOn") : 'London',
            ("Liverpool", "Livrepool", "LIverpool", "Liverp00l") : 'Liverpool',
            ("MANchester", "Manchester", "Manch3ster") : 'Manchester'
          }



# -------------------------------------------------------------------
# load and transform the data in a lazy dataframe
# -------------------------------------------------------------------

df = ( pl.scan_csv(data_file_path, has_header=True, separator=',', 
                 with_column_names=lambda cols: [col.lower().replace(' ', '_')
                                                 for col in cols])
         .select(
                  pl.all().exclude('location'),
                  pl.col('location').apply(lambda x: next(v for k, v in location_mapping.items() if x in k)) 
                )
     ).collect() \
      .pivot(values='value', columns='category', index=['location', 'nut_type']) \
      .lazy() \
      .with_columns((pl.col('Price (£) per pack') * pl.col('Quant per Q')).alias('revenue')) \
      

df = df.groupby('location').agg(
                                pl.mean('Price (£) per pack').round(2).cast(pl.Float32),
                                pl.sum('revenue')
                               )



# -------------------------------------------------------------------
# output the data
# -------------------------------------------------------------------

Path.mkdir(Path('./output'), exist_ok=True)
output_dir = Path('output').absolute()

df.collect().write_csv(f'{output_dir}/output.csv', has_header=True)



# %%



