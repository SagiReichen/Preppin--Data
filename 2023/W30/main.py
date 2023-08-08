#%%

from pathlib import Path

import polars as pl


def clean_sheet(month: str, sheet: pl.DataFrame) -> pl.DataFrame:
    # for month, sheet in sheets.items():
    sheet = sheet.with_columns(pl.lit(month).alias('month'))

    if sheet.shape[1]==7:
        sheet = sheet.drop('Comments')
    
    return sheet


def fiscal_year(df: pl.DataFrame) -> pl.DataFrame:
    
    df = ( df
            .with_columns(pl.col('order_date').dt.month().alias('month'))
            .with_columns(pl.when( (pl.col('month')>=7) & (pl.col('month')<=9) )
                            .then('1')
                            .when( (pl.col('month')>=10) & (pl.col('month')<=12) )
                            .then('2')
                            .when( (pl.col('month')>=1) & (pl.col('month')<=3) )
                            .then('3')
                            .when( (pl.col('month')>=4) & (pl.col('month')<=6) )
                            .then('4')
                            .alias('fiscal_quarter')
                        )
         )
    
    
    return df



def categorization(df: pl.DataFrame) -> pl.DataFrame: 

    df = df.with_columns((pl.when( pl.col('sign').abs()==3 )
                           .then(pl.when(pl.col('sign')==3)
                                   .then('Going from strength to strength')
                                   .otherwise('Going from bad to worse')
                                )
                           .when(pl.col('sign') == 1)
                           .then(pl.when(pl.col('pct_diff').sign() == -1)
                                   .then('Good growth, until Q4')
                                   .when(pl.col('pct_diff_prev1').sign() == -1)
                                   .then('Some good growth, but concerns in Q3')
                                   .when(pl.col('pct_diff_prev2').sign() == -1)
                                   .then('Good growth in last half')
                            )
                           .when(pl.col('sign') == -1)
                           .then(pl.when(pl.col('pct_diff').sign() == 1)
                                   .then('Concerning performance, but improving in Q4')
                                   .when(pl.col('pct_diff_prev1').sign() == 1)
                                   .then('Concerning performance, excluding Q3')
                                   .when(pl.col('pct_diff_prev2').sign() == 1)
                                   .then('Concerning performance in last half')
                            )).alias('trend')
                    
                    )

    return df


input_file = Path('./input/AllChains Sales.xlsx')

# read all sheets and change the columns accordingly
sheets = pl.read_excel(input_file, 
                       sheet_id=0, 
                       read_csv_options=
                            { "new_columns" : ['bike_type', 'store', 
                                                'model', 'sales', 'order_date']
                            }
                      )

# append all data frames together
df = pl.concat([ clean_sheet(month, sheet)
                 for month, sheet in sheets.items()]
      )


# enrich the data
df = ( df.with_columns(# creating clean date column                   
                     #day
                     (pl.col('order_date').str.split(',')
                       .list.get(1).str.strip() + 
                     pl.col('month') +
                     #year
                     pl.col('order_date').str.split(',')
                       .list.get(-1).str.strip()).str.to_date('%d%B%Y')
                    )
         .drop('month')
         .pipe(fiscal_year)
)

# df = df.pipe(fiscal_year)


df_grouped = ( df.groupby(['store', 'fiscal_quarter'])
                 .agg(pl.sum('sales'))
                 .with_columns(pl.col('fiscal_quarter').cast(pl.Int8))
                 .sort('store', 'fiscal_quarter')
                 .with_columns(pl.col('sales').shift(1).over('store').alias('prev_quarter_sales'))
                 .with_columns(((pl.col('sales') / pl.col('prev_quarter_sales') - 1)*100).round(1).alias('pct_diff'))
                 .with_columns(pl.col('pct_diff').shift(1).over('store').alias('pct_diff_prev1'),
                               pl.col('pct_diff').shift(2).over('store').alias('pct_diff_prev2'),
                              )
                 .with_columns(
                               (pl.col('pct_diff').sign() + 
                                pl.col('pct_diff_prev1').sign() + 
                                pl.col('pct_diff_prev2').sign()
                               ).alias('sign')
                              )
                 .pipe(categorization)
                 .with_columns(pl.col('trend').max().over('store')
                                 .alias('store_evaluation'))
                 .select(pl.all().exclude(['^pct.*$', 'trend', 'sign']))
                 .rename({'prev_quarter_sales' : '%_diff_QoQ'})
            )

# -----------------------------------------------------------------------------
# output the data
# -----------------------------------------------------------------------------

df_grouped.write_csv(f"{Path('./output')}/py-sol-output.csv")

# %%
