#%%
from pathlib import Path

import polars as pl

# -----------------------------------------------------------------------------
# creating functions to use
# -----------------------------------------------------------------------------


def load_dataset(path: Path | str) -> pl.LazyFrame:
    
    path = Path(path) if isinstance(path, str) else path

    df = pl.scan_csv( path.absolute(),
                    with_column_names=lambda cols: 
                        [col.lower().replace(' ', '_') for col in cols]
                    )
    
    return df

def grade_to_score(df) -> pl.LazyFrame:
    '''
    setting up the grade column to its matching score:
        1 = A, 2 = B, 3 = C, 4 = D, 5 = E, 6 = F
    
    args: df = is the piped dataframe -> None
    '''
    return ( df.with_columns(
               pl.when(pl.col('grade')==1)
               .then('A')                              
               .when(pl.col('grade')==2).then('B')
               .when(pl.col('grade')==3).then('C')
               .when(pl.col('grade')==4).then('D')
               .when(pl.col('grade')==5).then('E')
               .otherwise('F')
               .alias('grade'))
           )
  

def subject_parser(df: pl.DataFrame) -> pl.DataFrame:
    
    '''
    iterates over the sub_grade list[struct] column
    in order to create grade column for each subject per student

    args: df = applied dataframe
    '''
    
    for i in range(df.select(pl.col('sub_grade')
                        .list.lengths())
                        .to_series()
                        .to_list()[0]):
        
        subject : str = df.select(pl.col('sub_grade')
                            .list.get(i).struct.field('subject')) \
                            .to_series().to_list()[0].lower()
        
        df = df.with_columns(pl.col('sub_grade')
                               .list.get(i)
                               .struct.field('grade')
                               .alias(subject)
                            )

    return df                 



def fn_upper(df: pl.DataFrame) -> pl.DataFrame:
    '''
    generating a full_name field with CAPITAL LETTERS
    args: df = applied df
    '''
    df = ( df.with_columns(pl.concat_str(
                                [
                                 pl.col('first_name').str.to_uppercase(),
                                 pl.col('last_name').str.to_uppercase()
                                ], separator=' ').alias('full_name'))    
         )

    return df

# -----------------------------------------------------------------------------
# loading and transforming the datasets
# -----------------------------------------------------------------------------

# east students
df_east = ( load_dataset('./East Students.csv')
                .with_columns(pl.col('student_id')
                                .str.extract(r'^([A-Za-z]+)\d+$', 1)
                                .alias('region'),

                              pl.col('student_id')
                                .str.extract(r'[A-Za-z]+(\d+)$', 1)
                                .cast(pl.Int16)
                                .alias('student_id'),

                              pl.col('date_of_birth')
                                .str.strptime(pl.Date,'%A, %d %B, %Y'),
                        )
                .pipe(fn_upper)
                .select(
                        [
                         'student_id', 'full_name', 'date_of_birth', 
                         'subject', 'grade', 'region'
                        ]
                       )
        )


# west students
df_west = ( load_dataset('./West Students.csv').pipe(fn_upper)
                .with_columns(
                              pl.col('date_of_birth')
                                .str.strptime(pl.Date, '%d/%m/%Y'),

                              pl.col('student_id')
                                .str.split('-')
                                .list.to_struct(fields=['student_id', 'region'])
                                .alias('splitted_fields'),
                              
                              pl.col('grade').cast(pl.Int8)
                            )
                .pipe(grade_to_score)
                .select(['full_name', 'date_of_birth', 
                         'subject', 'grade', 'splitted_fields'])
                .unnest('splitted_fields')
                .collect()
                .select(pl.col('student_id').cast(pl.Int16),
                        pl.all().exclude('student_id'))
                .lazy()
          )




# union df_east and df_west
df_combined = ( pl.concat([df_east, df_west]) 
                  # A = 50, B = 40, C = 30, D = 20, E = 10, F = 0
                  .with_columns( pl.when(pl.col('grade')=='A').then(50)
                                   .when(pl.col('grade')=='B').then(40)
                                   .when(pl.col('grade')=='C').then(30)
                                   .when(pl.col('grade')=='D').then(20)
                                   .when(pl.col('grade')=='E').then(10)
                                   .otherwise(0)
                                   .alias('score'),                                 
                               )
                 .with_columns(pl.col('score').sum().over('student_id'))      
                 .collect()
                 .groupby(['student_id', 'full_name', 
                           'date_of_birth', 'score', 
                           'region'])
                            .agg(pl.struct('subject', 'grade').alias('sub_grade')
                         )
                .pipe(subject_parser)
                .lazy()
                .drop('sub_grade')
              )


# join the combined data with the lookup table
df = df_combined.join(load_dataset('./School Lookup.csv'),
                      left_on='student_id',
                      right_on=pl.col('student_id').cast(pl.Int16)).collect()




# -----------------------------------------------------------------------------
# output the table
# -----------------------------------------------------------------------------
df.write_csv(f"{Path('./')}/py-sol-output.csv")

# %%
