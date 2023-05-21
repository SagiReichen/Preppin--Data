import polars as pl
from pathlib import Path

# ----------------------------------------------------------------------------------
# load all sheets into one dataframe 
# ----------------------------------------------------------------------------------

input_file = Path.cwd() / 'input' / 'New Customers.xlsx'

# load all sheets into a dict of dataframes -> dict of dfs
dict_dfs = pl.read_excel(input_file, sheet_id=0)    # sheet_id= (0) means all sheets within the wb

# extract all dataframes from dict and make it a list
dfs_list = list(dict_dfs.values())

# change all cols to be the same, since one sheet has demagraphic and not demographic
# cuz couldn't concat all dataframes together
new_dfs_list = []
for i, df in enumerate(dfs_list):
    df.columns = ["id", "joining_day", "demographic", 'value']
    df = df.with_column(pl.lit(list(dict_dfs.keys())[i]).alias("month"))
    new_dfs_list.append(df)

# union all dataframes into one table
data_all = pl.concat(new_dfs_list, how="vertical")
#print(data_all)


# ----------------------------------------------------------------------------------
# Transform the dataset
# ----------------------------------------------------------------------------------

data = ( data_all.with_columns([
                (pl.col('joining_day').cast(pl.Utf8) 
                    +'/' + pl.col("month")
                    + '/' + '2023').alias("date")
                ])
                .with_columns(pl.col("date").str.strptime(pl.Date, "%d/%B/%Y", strict=True))
                .pivot(values="value", index=['id', 'date'], columns="demographic")
                .with_columns([pl.col("Date of Birth").str.strptime(pl.Date, "%m/%e/%Y", strict=True),
                               pl.col("Ethnicity").cast(pl.Categorical),
                               pl.col("Account Type").cast(pl.Categorical)])
                .groupby(['id', 'Ethnicity', 'Date of Birth', 'Account Type']).agg(pl.col("date").min())
        )

# sense check: checking for duplicate ids
print(data.select( pl.col("id")
                  .filter(pl.col("id")
                  .is_duplicated()) 
                  ))

#print(data)


# ----------------------------------------------------------------------------------
# Output the dataset
# ----------------------------------------------------------------------------------

data.write_csv(f"{Path.cwd()}/output-py-solution.csv", has_header=True, sep=",")


