from pathlib import Path

import polars as pl

# NOTE:
# -----------------------------------------------------------------------------
# the python solution output gives a different result set than the provided 
# results set from the preppin' team. The reason is that i've decided to account
# for all bike types and stores to show data up from the the minimum date and the
# max date range. in the offical result data set, they only accounted for the max
# date (june 2023), without taking into account a common min date. whereas I took 
# the intiative for account (Jan 2020) as a minimum bench mark for all of the bike type
# and stores. 
#
# therefore all of them have monthly date range from jan 2020 - june 2023 without
# data gaps





# load the dataset
# -----------------------------------------------------------------------------

df = pl.scan_csv(
                 Path("input/AllChains Data.csv").absolute(),
                 with_column_names=lambda cols: [col.lower().replace(' ', '_') 
                                                 for col in cols],
                 try_parse_dates=True
                )



# filter data after july-23
df = ( df.filter(pl.col('date') < pl.date(2023, 7, 1))
         .with_columns(pl.col('date').dt.truncate('1mo').alias('month_trunc'))
     )


df_grouped = ( df.groupby(['month_trunc', 
                         'store', 
                         'bike_type']).agg([
                                            pl.sum('sales'), 
                                            pl.sum('profit')
                                           ]
                                          )
                 .sort('month_trunc')
            )


df_out = ( df_grouped.select(
                        pl.col('store'), 
                        pl.col('bike_type'),
                        pl.date_range(
                            pl.col('month_trunc').min(),
                            pl.col('month_trunc').max(),
                            interval='1mo',
                            eager=True
                            ).alias('date')
                        ).explode('date') # exploding the list into rows, so we get each store and bike_type with a full date range
                         .join(df_grouped, 
                              how='left', 
                              left_on=['store', 'bike_type', 'date'],
                              right_on=['store', 'bike_type', 'month_trunc']) 
                         .fill_null(strategy='zero') 
                         .groupby(['store', 'bike_type', 'date']).agg(pl.min('sales'), pl.min('profit')) 
                         # sorting the dataframe so it picks the running mean correctly
                         .sort(['date', 'store', 'bike_type']) \
                         .with_columns(pl.col('profit')
                                       .rolling_mean(window_size=3)
                                       .over(['store', 'bike_type'])
                                       .alias('profit_running_mean_3p'))
      )



# output the data
# ----------------------------------------------------------------------

# create output directory
Path('./output').mkdir(exist_ok=True)

# output the data
df_out.collect().write_csv(f"{Path('./output')}/py-sol-output.csv")











