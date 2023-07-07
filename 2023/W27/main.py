#%%
from pathlib import Path

import polars as pl

# -----------------------------------------------------------------------------
# load xl file and all sheets
# -----------------------------------------------------------------------------

xl_file = Path('./OverHeadCosts.xlsx').absolute()

df_dict = pl.read_excel(xl_file, sheet_id=0)

# fetching all data frames using lazy evaluations by generator expression
df_iterator = (df for df in df_dict.values())

# -----------------------------------------------------------------------------
# transform the data 
# -----------------------------------------------------------------------------

df = ( pl.concat(df_iterator)
         .select(pl.all()
                   .map_alias(lambda col: col.lower().replace(' ', '_'))
                )
         .pivot(
                values='value', 
                columns='name', 
                aggregate_function='min', 
                index=['school_name', 'year', 'month']
               )
        .with_columns(pl.sum(['Electricity Cost', 'Water Cost', 
                              'Gas Cost', 'Maintenance Cost'])
                        .alias('total_cost'),

                     pl.concat_str(['year', 'month', pl.lit('01')])
                       .str.strptime(pl.Date, format='%Y%B%d')
                       .alias('date')
                    )
        .select(pl.all().exclude(['year', 'month']))
        .sort(by=['school_name', 'date'], descending=[False, False])
     )


# -----------------------------------------------------------------------------
# output the data
# -----------------------------------------------------------------------------

df.write_csv(f"{Path('.')}/output-py-sol.csv")

