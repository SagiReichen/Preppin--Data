#%%
from pathlib import Path

import polars as pl

# create a file path
file_path = Path('PD 2023 Wk 21 input.csv').absolute()

# load and transform the data set
df = ( pl.scan_csv(file_path, separator=',', try_parse_dates=True, 
                 with_column_names= lambda cols: [col.lower().replace(' ', '_')
                                                 for col in cols]
                )
         .melt(id_vars=['student_id', 
                        'first_name', 
                        'last_name', 
                        'gender', 
                        'd.o.b'],
               value_name='score',
               variable_name='year_category') 
         .with_columns(pl.col('year_category').str.split('-')
                                              .arr.to_struct()
                                              .struct.rename_fields(['year','category'])
                      )
         .unnest('year_category')
         .collect()
         .pivot(values='score', columns='year', aggregate_function='min',
                index=['student_id', 'first_name', 'last_name', 
                       'gender', 'd.o.b', 'category']
                )
         .lazy()        
         .with_columns(pl.col('2021').mean().over('student_id').alias('2021_avg'),
                       pl.col('2022').mean().over('student_id').alias('2022_avg')
                      )
         .with_columns( (pl.col('2022_avg') - pl.col('2021_avg')).alias('diff') )
         .with_columns(pl.when(pl.col('diff').gt(0))
                         .then('Improvement')
                         .when(pl.col('diff').eq(0))
                         .then('No change')
                         .otherwise('Cause for concern')
                         .alias('status')
                      )
         .filter(pl.col('status')=='Cause for concern')
         .select(pl.all().exclude('category', '2021', '2022'))
         .unique()
         .sort(by='student_id')
         .collect()
    
     )



# output the data
df.write_csv(f'{Path.cwd()}/output-py-sol.csv', date_format='%d/%m/%Y')
