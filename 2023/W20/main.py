from pathlib import Path

import polars as pl
from numpy import floor

# -------------------------------------------------------------------------------------
# helper funcs
# -------------------------------------------------------------------------------------

def round_half_up(n: float, decimals: int=0) -> int:
    '''Using round half up method vs the python default rounding mechanism'''

    mp = 10 ** decimals
    return floor(n * mp + 0.5) / mp

# -------------------------------------------------------------------------------------
# loading the data sets into data frames
# -------------------------------------------------------------------------------------

file_path = Path('Dining_Hall_Debacle_Input_Data.xlsx').absolute()

sheets = pl.read_excel(file_path, sheet_id=0)
df_meal_prices, df_nutrition = sheets['Meal Prices'], sheets['Meal Nutritional Info']


# -------------------------------------------------------------------------------------
# transforming the data sets 
# ------------------------------------------------------------------------------------- 

df = ( df_nutrition.with_columns(
                                 pl.when(pl.col('Type').str.contains('(?i)meat')).then('meat-base')
                                 .when(pl.col('Type').str.contains('(?i)vegan')).then('vegan')
                                 .otherwise('vegetarian')
                                 .alias('Type')
                              )
                   .join(df_meal_prices, on='Meal Option', how='inner')
                   .with_columns(pl.col('Type').count().over('Type').alias('num_meals_per_type'),
                                 pl.col('Type').count().alias('total_meals')
                                )
                   .with_columns((pl.col('num_meals_per_type') / pl.col('total_meals') * 100).round(2).alias('pct_of_total'))
                   .groupby('Type').agg(
                                        pl.mean('Price').round(2).cast(pl.Float32).alias('avg_price'),
                                        pl.max('pct_of_total').apply(round_half_up).cast(pl.Int8)
                                       )
     )


# -------------------------------------------------------------------------------------
# output the data
# ------------------------------------------------------------------------------------- 

df.write_csv(f"{Path('.').absolute()}/output-py-sol.csv")






