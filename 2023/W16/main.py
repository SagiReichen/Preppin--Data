
from pathlib import Path

import polars as pl

# --------------------------------------------------------------------------------------------------------
# input and load the files
# --------------------------------------------------------------------------------------------------------

input_dir = (Path('.') / 'input').absolute()
input_files = list(Path.glob(input_dir, '*'))


df_easter = ( pl.scan_csv(input_files[1], has_header=True, try_parse_dates=True)
                .sort('date')
                .rename({'date' : 'easter_date'})
            )


df_full_moon = ( pl.scan_csv(input_files[0],
                           has_header=True,
                           with_column_names=lambda cols: [col.lower().strip() for col in cols]
                              )
                    .select(pl.all().str.strip())
                    .with_columns(
                                    pl.col('date').str.strptime(pl.Date, '%d %B %Y').alias('fm_date'),
                                    pl.col('day').alias('fm_day'),
                                    pl.col('time').str.replace_all(pattern='[^*+]', value='', literal=False).alias('time')
                                  )
                    .with_columns(pl.when(pl.col('time')=='**')
                                     .then('total lunar eclipse takes place')
                                     .when(pl.col('time')=='*')
                                     .then('partial lunar eclipse takes place')
                                     .when(pl.col('time')=='+')
                                     .then('blue moon')
                                     .when(pl.col('time')=='**+')
                                     .then('total lunar eclipse & blue moon')
                                     .alias('time'))
                    .drop(['date', 'day'])
                    .sort(by='fm_date', descending=False)
               )


# --------------------------------------------------------------------------------------------------------
# transform the data and join them
# --------------------------------------------------------------------------------------------------------


df = ( df_full_moon.join_asof(df_easter,
                            left_on='fm_date',
                            right_on='easter_date',
                            strategy='forward',
                           ) 
                   .with_columns((pl.col('easter_date') - pl.col('fm_date')).dt.days().alias('days_between'))
                   .drop_nulls('easter_date')
                   .with_columns(pl.col('days_between').min().over('easter_date').alias('min_date_per_easter'))
                   .filter(pl.col('days_between')==pl.col('min_date_per_easter'))
                   .select(
                            pl.col('time'),
                            pl.col('fm_date').dt.year().alias('fm_date_year'),
                            pl.col('easter_date').dt.year().alias('easter_date_year'),
                            pl.col('days_between')
                   )
                   .groupby('days_between').agg(
                                                 num_occurrences=pl.col('days_between').count(),
                                                 full_moon_notes=pl.col('time').max(),
                                                 fm_date=pl.col('fm_date_year').min(),
                                                 easter_sunday=pl.col('easter_date_year').max()
                          )
                   .sort('days_between')      
     )


# --------------------------------------------------------------------------------------------------------
# output the data to a csv file
# --------------------------------------------------------------------------------------------------------

output_dir = (Path() / 'output').absolute()
df_collected = df.collect()

def save_df_as_csv(df: pl.DataFrame, path: Path) -> None:
    if not path.exists():
        output_dir.mkdir(parents=False, exist_ok=True)
    
    df.write_csv(f'{path}/output-py-sol.csv', has_header=True, separator=',')
    
    return None
        
# ---------------------------------------------------------------------------------------------------------

save_df_as_csv(df_collected, output_dir)

