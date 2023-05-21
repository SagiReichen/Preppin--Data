#!/usr/bin/env python3

from glob import iglob
from pathlib import Path
from typing import Iterator

import polars as pl

# confings for polars views 
pl.Config.set_fmt_str_lengths(150)
pl.Config.activate_decimals(active=True)
pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_cols(50)


# ----------------------------------------------------------------------------------------------
# load all data files and trasform the data as required
# ----------------------------------------------------------------------------------------------

countires_files_dir = Path(__file__).parent.joinpath('input-countries-files')
countries_files: Iterator[Path] = iglob(f'{countires_files_dir}/*')

countries_code = Path(__file__).parent.joinpath('input-countries-code/countries-codes.csv')

# ----------------------------------------------------------------------------------------------

df_countries = pl.concat([pl.read_csv(file, has_header=True, 
                                      encoding='utf8-lossy',
                                      infer_schema_length=0,
                                     ) 
                            # get the country code from within the file name
                            .with_columns(pl.lit(file).str.split('_').arr.get(1).alias('country_code'))
                            # filtering out redundent data rows
                            .filter((pl.col('Indicator Type').is_in(['Export', 'Import'])) & 
                                    (~pl.col('Reporter').is_in(['European Union', 'Occ.Pal.Terr', 'Other Asia, nes', 'World'])) &
                                    (~pl.col('Partner').is_in(['...', 'Special Categories', 'World']))
                                   )
                          for file in countries_files], 
                          how='vertical')
                          

# unpivot the data, making it taller for cleaning purposes
# clean the year columns and the value column and casting those
df_countries = ( df_countries.melt(id_vars=['Reporter', 'Partner', 'Product categories', 
                                          'Indicator Type', 'Indicator', 'country_code'],
                                variable_name='year',
                                value_name='value')
                             .with_columns([
                                             pl.col('year').str.strptime(pl.Date, fmt='%Y').alias('year'),
                                             pl.col('value').str.extract(r'([0-9.])').cast(pl.Float32)
                                          ])
                             .filter(pl.col('value').is_not_null())
               )

# pivot the table again on the indicator field
df_countries = df_countries.pivot(values='value', columns='Indicator', 
                                  index=[col for col in df_countries.columns if col not in ['value', 'Indicator']],
                                  aggregate_function='sum',
                                  maintain_order=True)

# df_countries

# ------------------------------------------------
# load and clean the codes data set for joining to the main df
df_codes = ( pl.read_csv(countries_code, separator=';', 
                       has_header=True, columns=['ISO3 CODE', 'geo_point_2d']) 
               .drop_nulls()
               .with_columns(
                                pl.col('geo_point_2d').str.split(',').arr.first().cast(pl.Float64).alias('latitude'),
                                pl.col('geo_point_2d').str.split(',').arr.last().cast(pl.Float64).alias('longitude')
                            )
               .drop('geo_point_2d')
               .rename({'ISO3 CODE': 'iso3_code'})
           )


# joining the two tables together
# 1st join, joining the countries with its corresponding codes to get the (lat,long)
df_comb = ( df_countries.join(df_codes,
                            how='left',
                            left_on='country_code',
                            right_on='iso3_code')
                        # 2nd join accounting for the partner countries codes
                        .join((df_countries.select(['country_code', 'Reporter'])
                                          .unique(['country_code', 'Reporter'])
                                          .rename({'country_code' : 'code_partner',
                                                   'Reporter' : 'country'})),
                               how='left',
                               left_on='Partner',
                               right_on='country')
                        # 3rd join to get the lat and long values for the partner
                        .join(df_codes,
                              how='left',
                              left_on='code_partner',
                              right_on='iso3_code')
          )


df_comb = df_comb.rename({ col : col.lower().replace(' ', '_') 
                          for col in df_comb.columns})

# ----------------------------------------------------------------------------------------------
# output the data into a csv file
# ----------------------------------------------------------------------------------------------

output_path = Path.cwd() / 'output-py.csv'
df_comb.write_csv(output_path, has_header=True, separator=',', date_format='%Y-%m-%d')


