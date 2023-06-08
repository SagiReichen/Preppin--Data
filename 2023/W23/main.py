#%%
from pathlib import Path

import polars as pl

# load the datasets
file_path = Path('Student Input.xlsx').absolute()

dfs: dict = pl.read_excel(file_path, 
                          sheet_id=0, 
                          read_csv_options={"has_header": True})

df_students_info: pl.DataFrame  = dfs.get('Student Info', 'key doesn\'t exist')
df_results: pl.DataFrame = dfs.get('Results', 'key doesn\'t exist')


# cleaning the df_students_info table, (striping whitespaces in header names and rows)
df_students_info = ( df_students_info
                            .rename(
                                    {col: col.strip().lower().replace(' ', '_')
                                    for col in df_students_info.columns}
                                   )
                            .select(pl.col('student_id'),
                                    pl.all().exclude('student_id').str.strip())
                    ).lazy()


# renaming df_results cols to be lower case                                           
df_results = df_results.rename(
                               {col: col.strip().lower().replace(' ', '_')
                               for col in df_results.columns}
                              ).lazy()


# tranforming the data-set to its output
df = ( df_students_info.join(df_results, 
                             on='student_id',
                             how='inner')
                       .groupby(by='class').agg(pl.mean('english').round(2),
                                                pl.mean('economics').round(2),
                                                pl.mean('psychology').round(2)
                                              )
                       .melt(id_vars='class', variable_name='subject', value_name='grade')
                       .with_columns(pl.col('grade').rank(method='dense', descending=False)
                                                    .over('subject')
                                                    .alias('grade_rank_desc'))
                       .filter(pl.col('grade_rank_desc')==1)
                       .select(pl.col(['subject', 'grade', 'class']))
     ).collect()


# output the data
df.write_csv(f'{Path(".").absolute()}/output-py-sol.csv')