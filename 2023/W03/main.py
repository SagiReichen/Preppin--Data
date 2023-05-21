import polars as pl
from pathlib import Path

# --------------------------------------------------------------------
# Polars settings
# --------------------------------------------------------------------

pl.Config.set_tbl_cols(20) 
pl.Config.set_tbl_rows(20)

# --------------------------------------------------------------------
# read transactions data set and transform it
# --------------------------------------------------------------------


file_transactions = Path('./drive-download-20230118T170546Z-001/PD 2023 Wk 1 Input.csv')
file_targets = Path('./drive-download-20230118T170546Z-001/Targets.csv')

df_trans = ( pl.read_csv(file_transactions, has_header=True, parse_dates=True, encoding='utf8')
            .filter(pl.col("Transaction Code").str.contains("DSB"))
            .with_columns([
                            pl.col('Transaction Date').str.strptime(pl.Datetime, fmt='%d/%m/%Y %T', strict=False).cast(pl.Date)
                            .dt.quarter().cast(pl.Int8).alias('quarter'),
                            pl.when(pl.col("Online or In-Person") == 1).then("Online").otherwise("In-Person").alias("Online or In-Person")
                         ])
            .groupby(["quarter", "Online or In-Person"], maintain_order=True).agg(pl.sum("Value").cast(pl.Int32))                                                                           
            )
            
            
df_targets = ( pl.read_csv(file_targets, has_header=True, encoding='utf8')
               .melt(id_vars="Online or In-Person", value_vars=["Q1", "Q2", "Q3", "Q4"])
               .rename({"variable": "quarter", "value": "target"})  
               .with_columns([
                              pl.col("target").cast(pl.Int32), 
                              pl.col("quarter").str.extract(r"(\d{1})", 1).cast(pl.Int8)
                            ])
             )

# print(df_trans, df_targets) -- sense check

# inner joining the two tables on online/in-person field and quarter
df_joined = df_trans.join(
              df_targets,
              left_on=["quarter", "Online or In-Person"],
              right_on=["quarter", "Online or In-Person"],
              how="inner"
            )
            

# Calculate the Variance to Target for each row

df = df_joined.with_column((pl.col("Value") - pl.col("target"))
              .alias("variance_to_target"))


# --------------------------------------------------------------------
# output the file
# --------------------------------------------------------------------

pwd = Path(__file__).parent.absolute()
output = df.write_csv(f'{pwd}/output-py.csv', has_header=True, sep=",")


