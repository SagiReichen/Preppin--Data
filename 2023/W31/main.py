#%%
from pathlib import Path

import polars as pl

# -----------------------------------------------------------------------------
# load data sources 
# -----------------------------------------------------------------------------

df_dim = pl.scan_csv(Path('./input/ee_dim_input.csv'))
df_monthly = pl.scan_csv(Path('./input/ee_monthly_input.csv'))



# -----------------------------------------------------------------------------
# helper function
# -----------------------------------------------------------------------------

def clean_ids(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    '''
    making sure the ids columns are non-nullable by coalescing 
    the columns using a lookup table
    '''

    df = ( 
            df.join(df_lookup, on='guid', how='left', suffix='_right')
                  .with_columns(pl.coalesce(['employee_id_right', 'employee_id']).alias('emp_id'))
                  .drop(['employee_id_right', 'employee_id'])
                  .join(df_lookup, left_on='emp_id', right_on='employee_id', how='inner')
                  .with_columns(pl.coalesce(['guid', 'guid_right']).alias('guid'))
                  .drop('guid_right')
         )


    return df


# -----------------------------------------------------------------------------
# creating the transformations
# -----------------------------------------------------------------------------


# creating a lookup table for unique rows of emp_id and guid
df_lookup = ( pl.concat([
                            df_dim.groupby(['employee_id', 'guid']).count(),
                            df_monthly.groupby(['employee_id', 'guid']).count()
                        ])
                .filter(pl.col('employee_id').is_not_null() & pl.col('guid').is_not_null())
                .select(['employee_id', 'guid'])
                # deduplicate rows since some employees appear in both data sets
                .groupby(['employee_id', 'guid']).count()
                .drop('count')
            )



# adjusting the employee dimension table
df_dim_clean = df_dim.pipe(clean_ids)


# adjusting the monthly table
df_monthly_clean = df_monthly.pipe(clean_ids)



# -----------------------------------------------------------------------------
# output the clean outputs
# -----------------------------------------------------------------------------

df_monthly_clean.collect().write_csv(f"{Path('./output')}/monthly_clean_output_py.csv")
df_dim_clean.collect().write_csv(f"{Path('./output')}/dim_clean_output_py.csv")





