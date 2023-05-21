import polars as pl
from pathlib import Path

# --------------------------------------------------------------------------
# load the data
# --------------------------------------------------------------------------

pl.Config.set_tbl_rows(50)
pl.Config.set_tbl_cols(50)

input_file = Path(__file__).parents[0] / 'DSB Customer Survery.csv'
df = pl.read_csv(input_file, has_header=True, sep=',')



# --------------------------------------------------------------------------
# Transfrom the data
# --------------------------------------------------------------------------


# get a list of all cols 
cols_names = df.columns
# remove customer id column
cols_names.remove('Customer ID')

# unpivot the data set
df = ( df.melt(
        id_vars='Customer ID', 
        value_vars=cols_names, 
        value_name='score')
       # extract the info needed from the variable column
       .with_columns([pl.col('variable').str.split(by=' - ').arr.get(0).alias('platform'),
                      pl.col('variable').str.split(by=' - ').arr.get(-1).alias('question')
                      ])
       #.drop('variable')
       # pivot again to a 'wide' form
       .pivot(
            values='score', 
            index=['Customer ID','question'],
            columns='platform',
            aggregate_fn='min'
       )
       # exclude overall rating question
       .filter(pl.col('question') != 'Overall Rating')
       # create the mean calcs for each platform by customer
       .with_columns([
                pl.col('Mobile App').mean().over('Customer ID').alias('avg_score_mobile_per_cus')
              , pl.col('Online Interface').mean().over('Customer ID').alias('avg_score_online_per_cus') 
       ])
       .groupby(by=['Customer ID']).agg([pl.col('avg_score_mobile_per_cus').min(),
                                                pl.col('avg_score_online_per_cus').min()])
       .with_columns((pl.col('avg_score_mobile_per_cus') - pl.col('avg_score_online_per_cus'))
                     .alias('platform_diff'))
       .with_columns([pl.when(pl.col('platform_diff') >= 2)
                      .then('Mobile App Superfan')
                      .when(pl.col('platform_diff') >= 1)
                      .then('Mobile App Fan')
                      .when(pl.col('platform_diff').is_between(-1, 1))
                      .then('Neutral')
                      .when(pl.col('platform_diff') <= -2)
                      .then('Online Interface Superfan')
                      .when(pl.col('platform_diff') <= -1)
                      .then('Online Interface Fan')
                      .alias('group')
                       , pl.col('Customer ID').count().alias('num_unique_customers')])
        .select(pl.col(['group', 'num_unique_customers']))
        .with_columns(pl.col('group').count().over('group').alias('count_of_group'))
        .groupby(['group', 'num_unique_customers', 'count_of_group'], maintain_order=True).min()
        .with_columns((pl.col('count_of_group') / pl.col('num_unique_customers')*100).round(1).alias('% total'))
        .select(pl.col(['group', '% total']))
    )

# tests
# print(df.filter(pl.col('Customer ID') == 556201))
# print(df.select('Customer ID').n_unique())

# checking uniqueness
# print(df.is_unique().sum())
#print(df.filter(pl.col('name') == 'Online Interface Superfan'))


# --------------------------------------------------------------------------
# output the data
# --------------------------------------------------------------------------

cwd = Path(__file__).parents[0]
output_file_name = 'output-py-solution.csv'

df.write_csv(f'{cwd}/{output_file_name}',has_header=True, sep=',')
