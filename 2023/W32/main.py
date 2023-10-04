#%%
from pathlib import Path

import polars as pl

# IF [age] < 20 THEN 'Under 20 years'
# ELSEIF [age] >= 20 AND [age] <= 24 THEN '20-24 years'
# ELSEIF [age] >= 25 AND [age] <= 29 THEN '25-29 years'
# ELSEIF [age] >= 30 AND [age] <= 34 THEN '30-34 years'
# ELSEIF [age] >= 35 AND [age] <= 39 THEN '35-39 years'
# ELSEIF [age] >= 40 AND [age] <= 44 THEN '40-44 years'
# ELSEIF [age] >= 45 AND [age] <= 49 THEN '45-49 years'
# ELSEIF [age] >= 50 AND [age] <= 54 THEN '50-54 years'
# ELSEIF [age] >= 55 AND [age] <= 59 THEN '55-59 years'
# ELSEIF [age] >= 60 AND [age] <= 64 THEN '60-64 years'
# ELSEIF [age] >= 65 AND [age] <= 69 THEN '65-69 years'
# ELSEIF [age] >= 70 THEN '70+ years'
# ELSE 'Not Provided'
# END


# calculate the age-range buckets func
def age_buckets(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    df = ( df
            .with_columns(
                pl.when(pl.col('age').lt(20)).then('Under 20 years')
                  .when((pl.col('age').ge(20) & pl.col('age').le(24))).then('20-24 years')
                  .when((pl.col('age').ge(25) & pl.col('age').le(29))).then('25-29 years')
                  .when((pl.col('age').ge(30) & pl.col('age').le(34))).then('30-34 years')
                  .when((pl.col('age').ge(35) & pl.col('age').le(39))).then('35-39 years')
                  .when((pl.col('age').ge(40) & pl.col('age').le(44))).then('40-44 years')
                  .when((pl.col('age').ge(45) & pl.col('age').le(49))).then('45-49 years')
                  .when((pl.col('age').ge(50) & pl.col('age').le(54))).then('50-54 years')
                  .when((pl.col('age').ge(55) & pl.col('age').le(59))).then('55-59 years')
                  .when((pl.col('age').ge(60) & pl.col('age').le(64))).then('60-64 years')
                  .when((pl.col('age').ge(65) & pl.col('age').le(69))).then('65-99 years')
                  .when(pl.col('age').ge(70)).then('70+ years')
                  .otherwise('Not Provided')
                  .alias('age_bucket')
         )
    )
    
    return df



# load all datasets
ee_dim = Path('./input/ee_dim_v2.csv')
ee_monthly = Path('./input/ee_monthly_v2.csv')
generations = Path('./input/generations.csv')


# transformations steps
df_dim = (pl.scan_csv(ee_dim)
            .with_columns(pl.col('date_of_birth').str.to_date('%d/%m/%Y'),
                          pl.col('hire_date').str.to_date('%d/%m/%Y'),
                          pl.col('leave_date').str.to_date('%d/%m/%Y'),
                          )
            .with_columns(pl.col('date_of_birth').dt.year().alias('birth_year'))
            )

# df_dim.head(10)

df_gen = ( pl.scan_csv(generations)
             .with_columns(pl.when(pl.col('start_year').is_null())
                             .then(pl.col('generation') + ' (born in or before ' 
                                   + pl.col('end_year').cast(pl.Utf8) + ')')
                             .when(pl.col('end_year').is_null())
                             .then(pl.col('generation') + ' (born in or after ' 
                                   + pl.col('start_year').cast(pl.Utf8) + ')')
                             .otherwise(pl.col('generation') + ' (' 
                                        + pl.col('start_year').cast(pl.Utf8) + '-' 
                                        + pl.col('end_year').cast(pl.Utf8) + ')')
                            .alias('gen_name')
                          )
         )


df_ = ( df_dim.join(df_gen, how='cross')
              .filter(
                     ((pl.col('birth_year').ge(pl.col('start_year'))) & 
                     (pl.col('birth_year').le(pl.col('end_year'))))
                    )
      )

# accounting for the dropped 3 values
df_output_one = df_dim.join(df_, on='employee_id', how='left') \
                      .with_columns(
                                  pl.when(pl.col('gen_name')
                                    .is_null())
                                    .then('Not Provided')
                                    .otherwise(pl.col('gen_name'))
                                    .alias('gen_name')
                                   ) \
                      .select('employee_id', 'guid', 'gen_name', 'first_name',
                              'last_name', 'nationality', 
                              'gender', 'email', 'hire_date', 'leave_date', 
                              'date_of_birth')

df_output_two = ( pl.scan_csv(ee_monthly, try_parse_dates=True)
                    .join(df_output_one, 
                          left_on='employee_id', 
                          right_on='employee_id',
                          how='inner',
                         )
                    .with_columns((pl.col('month_end_date').dt.year() 
                                        - pl.col('date_of_birth').dt.year())
                                     .cast(pl.Int16)
                                     .alias('age')
                                 )
                    .pipe(age_buckets)
                    .select('dc_nbr', 'month_end_date', 'employee_id', 'guid',
                            'hire_date', 'leave_date', 'age_bucket')
                    )
                

# output to csvs

df_output_one.collect().write_csv(f"{Path('./output')}/output_1.csv")
df_output_two.collect().write_csv(f"{Path('./output')}/output_2.csv")

# %%
