from pathlib import Path
import polars as pl


# polars settings:
pl.Config.set_tbl_cols(20)
pl.Config.set_tbl_rows(20)

# ----------------------------------------------------------------------
# input the data
# ----------------------------------------------------------------------

# get the file using pathlib lib, so it works on both unix and windows os
input_file = Path(__file__).parent.joinpath('PD 2023 Wk 1 Input.csv')

# data types definitions:
# Uint is being used since no values under [1,2,3] columns can be negative
dtypes = {
    'Transaction Code': pl.Utf8,
    'Value': pl.UInt32,
    'Customer_code': pl.UInt32,
    'Online or In-Person': pl.UInt8,
    'Transaction Date': pl.Utf8
}

# ----------------------------------------------------------------------
# transform the data to be grouped by bank and month, summing up value
# ----------------------------------------------------------------------

# dataframe creation
df = ( pl.read_csv(input_file, has_header=True, sep=',', dtypes=dtypes)
         .with_columns([pl.col('Transaction Date').str.strptime(pl.Date, fmt="%d/%m/%Y %T", strict=True)
                        , #pl.col('Transaction Code').str.extract(r"^(\w+)-")   // can be done with regex or ↓
                          # split transaction code into a list of values, lamda to retrieve the first item
                          pl.col('Transaction Code').str.split(by='-').apply(lambda tc: tc[0])
                        ])
        .with_columns([pl.col('Transaction Date').dt.strftime('%B').alias('month')
                        , pl.col('Transaction Code').alias('Bank')])
        .drop(['Transaction Code', 'Transaction Date'])
        .groupby(by=['Bank', 'month'])
        .agg(pl.col('Value').sum())
     )

# creating the ranks 
df = ( df.select(
            [
                pl.col('*')
                , pl.col('Value').rank(method='dense', reverse=True).over('month').alias('Bank Rank Per Month')
            ]
        )
        .with_columns([
                pl.col('Bank Rank Per Month').mean().over('Bank').round(2).alias('Avg Rank per Bank')
                , pl.col('Value').mean().over('Bank Rank Per Month').round(2).alias('Avg Transaction Value per Rank')
        ])
        
)

# check: 
# print(df.sort(pl.col(['month', 'Bank Rank Per Month'])))

# replace column names with snake_case and replacing space with '_'
new_col_names = pl.Series(df.columns).str.to_lowercase().str.replace_all(' ','_', literal=True).to_list()
df.columns = new_col_names


# ----------------------------------------------------------------------
# outout the data into a csv file
# ----------------------------------------------------------------------

output_dir = Path(__file__).parent.joinpath('output')
print(output_dir)

df.write_csv(f'{output_dir}/output-py-solution.csv', has_header=True, sep=',')