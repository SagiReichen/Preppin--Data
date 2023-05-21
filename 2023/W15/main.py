#!/usr/bin/env python3
from pathlib import Path

import polars as pl

# shows all the columns setting
pl.Config.set_tbl_cols(-1)


# ------------------------------------------------------------------------------
# load the data sheet
# ------------------------------------------------------------------------------

file_csv = Path('Easter Dates.csv').absolute()

# necessary to use infer_schema_length=2 so it loads everything as a string,
# otherwise it throws an error
df_base = pl.read_csv(file_csv, skip_rows=3, has_header=False, infer_schema_length=2) \
            .drop('column_1', 'column_2')

# ------------------------------------------------------------------------------

# extracting the years
df_years = df_base.drop_nulls('column_3').drop('column_3')

df_years = ( df_years.with_columns(pl.col([col for col in df_years.columns]).cast(pl.Int16))
                     .melt(variable_name='col', value_name='year')
                     .drop_nulls()
           )

# ------------------------------------------------------------------------------

# extracting the dates
# dividing the days and months into seperate data frames, then concatenating them together

df_dates = pl.concat([  # extracting the months for each col
                        (df_base.filter(pl.col('column_3').is_null())
                                .drop('column_3')[1:]                    # [1:] gives only the last row
                                .melt()
                                .fill_null(strategy='forward')
                                .select(
                                            pl.col('variable').alias('col'),
                                            pl.col('value').str.replace_all('\s', '').alias('month'),
                                            pl.col('variable').str.extract(r'(\d+)', 1).cast(pl.Int16).alias('sorter')
                                       )
                                .sort(by='sorter', descending=False)
                        ), 
                         # extracting the dats of each col
                        ( df_base.filter(pl.col('column_3').is_null())
                                 .drop('column_3')[:1]           
                                 .melt()
                                 .select(
                                            pl.col('variable').alias('col_'),
                                            pl.col('value').alias('day'),
                                            pl.col('variable').str.extract(r'(\d+)', 1).cast(pl.Int16).alias('sorter_')
                                        )
                                 .sort(by='sorter_', descending=False)
                        ) 
                   ], how='horizontal') \
             .select(
                        pl.col('col'),
                        pl.col('month'),
                        pl.col('day')
                    )

# joining the dates col to the years data frame
df_comb = ( df_years.join(df_dates, on='col', how='inner')
                    .filter(pl.col('year') <= 2023)
                    .select((pl.col('year').cast(pl.Utf8) + '-' + pl.col('month') + '-' + pl.col('day'))
                                .str.strptime(pl.Date, '%Y-%B-%d')
                                .alias('date')
                           )             
          )


# ------------------------------------------------------------------------------
# load the data sheet
# ------------------------------------------------------------------------------

df_comb.write_csv(f'{Path(__file__).parent}/output-py.csv')

