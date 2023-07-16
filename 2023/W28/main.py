#%%
from pathlib import Path

import polars as pl

# -----------------------------------------------------------------------------
# loading all data sheets 
# -----------------------------------------------------------------------------

xl_file = Path('./Prep School Track Team.xlsx').absolute()
sheets = pl.read_excel(xl_file, sheet_id=0)



# -----------------------------------------------------------------------------
# transforming the data sets 
# -----------------------------------------------------------------------------


df = ( sheets['Students']
         .join(sheets['Track Times'], on='id', how='inner')
         # the benchmarks sheet has space in its columns names,
         # has to be cleaned before joined
         .join(sheets['Benchmarks'].select(pl.all().map_alias(
                    lambda col: col.strip().lower())), 
               how='inner', 
               left_on=['age', 'gender', 'track_event'], 
               right_on=['age', 'gender', 'event'])
        # filtering students, who don't fall under the benchmark
        .filter(pl.col('time').lt(pl.col('benchmark')))
        # remove any 200m times that fall below 25 seconds
        .filter(~( (pl.col('track_event')=='200m') & (pl.col('time').lt(25)) ))
        .with_columns(pl.col('time')
                        .rank('dense', descending=False)
                        .over('track_event')
                        .alias('rank'))
        .sort(by='rank')
     )



# -----------------------------------------------------------------------------
# output the cleaned data set
# -----------------------------------------------------------------------------

df.write_csv(f"{Path('.').absolute()}/output-py-sol.csv")






