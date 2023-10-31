#%%
import polars as pl
from pathlib import Path

# loading data into dfs
ee_dim_file = Path('./input/ee_dim_v3.csv')
ee_monthly = Path('./input/ee_monthly_v3.csv')

# cleaning data

# empolyee data frame, removing unnecessary columns
emp_df = pl.read_csv(ee_dim_file, 
                     columns=['employee_id', 'nationality', 
                              'gender', 'generation_name'])

(pl.read_csv(ee_monthly, try_parse_dates=True)
#    .with_columns(pl.when(pl.col('leave_date').is_null())
                #    .then())
)
