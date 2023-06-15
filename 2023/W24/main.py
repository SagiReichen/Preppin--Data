#%%
from pathlib import Path

import polars as pl

# -----------------------------------------------------------------------------
# input all data sets
# -----------------------------------------------------------------------------

# extracting all data sets from the main xsl file
file_path_students: dict = pl.read_excel(f"{Path('.').absolute()}/Student Input.xlsx",
                                   sheet_id=0)

# tiles categories table
df_tiles = pl.read_excel(f"{Path('.').absolute()}/Tiles.xlsx",
                                sheet_id=1)

# cleaning the student info table 
df_info = file_path_students['Student Info']

df_info = ( df_info.rename(
                            { col: col.lower().strip()
                            for col in df_info.columns}
                        )
                .select(pl.col('student id'),
               pl.all().exclude('student id').str.strip())
          )



# -----------------------------------------------------------------------------
# transform the data
# -----------------------------------------------------------------------------


df_out = (file_path_students['Results']
                .melt(id_vars='Student ID',
                      # all subjects are to become pivoted
                      variable_name='subject',
                      value_name='score')
                # creating the tiles
                .with_columns(pl.col('score')
                                .apply(lambda x: x.qcut([0.25, 0.5, 0.75], labels=['4', '3', '2', '1'], maintain_order=True)['category'])
                                .over('subject').cast(pl.Utf8).cast(pl.Int64).alias('score_tile'))
              # joining the tiles data set 
                .join(df_tiles, how='inner', left_on='score_tile', right_on='Number') 
              # joining the df_info data set  
                .join(df_info, how='inner', left_on='Student ID', right_on='student id')   
                .filter(pl.col('class').is_in(['9A', '9B']))     
                .pivot(values='Range', index=['full name', 'class'], columns='subject')
              # creating an array for all subjects' tiles 
                .with_columns(pl.concat_list(['English', 'Economics', 'Psychology']).alias('helper_array')) 
              # iterating over each element to check if how many times 25th appears
                .with_columns(pl.col('helper_array').arr.eval(
                                        pl.element().str.contains('25th', literal=True))
                                          .arr.count_match('true')
                                          .alias('flag_how_many_25th')
                             ) 
             # filtering for only students that appear more than 2 in the last percentile
               .filter(pl.col('flag_how_many_25th') >= 2) 
            # selecting only necessary cols for the output
               .select(pl.all().exclude('helper_array'))
             )


# -----------------------------------------------------------------------------
# output the data
# -----------------------------------------------------------------------------
df_out.write_csv(f"{Path('.').absolute()}/output-py-sol.csv")









