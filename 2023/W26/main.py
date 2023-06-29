#%%
from pathlib import Path

import polars as pl

# 1. load and transform the main data set
# 2. join the lookup table
df = ( pl.scan_csv( Path('./Part 1 Output.csv'),
                    with_column_names=lambda cols: 
                        [col.lower().replace(' ', '_')
                         for col in cols],
                    try_parse_dates=True
                  )
        # fetching only the first 2 initials for every name 
        # (a name can have more than 2 items)
         .with_columns(pl.col('full_name')
                            .str.split(' ')
                            .list.eval(pl.element().str.slice(0,1))
                            .list.slice(0,2)
                            # .list.lengths()
                            .list.join('')
                            .alias('initials'))
        # join the lookup table
         .join(pl.scan_csv(Path('./Additional Info Lookup.csv'),
                            with_column_names=lambda cols: 
                                [col.lower().replace(' ', '_')
                                 for col in cols],
                             try_parse_dates=True
                          ),
                on=['initials', 'school_name', 'date_of_birth',
                    'maths', 'english', 'science'],
                 
                how='inner'
              )
     )


# 3. creating the ranking by Grade Score within 
#    their specified Subject Selection and Region
df = (df
        .sort(by=['grade_score', 'distance_from_school_(miles)'], 
              descending=[True, False])
        .with_columns(pl.col('grade_score')                    
                        .rank(method='ordinal', descending=True)
                        .over(['subject_selection', 'region'])
                        .alias('student_rank'))
        .filter(pl.when(
                            ( (pl.col('region').str.to_lowercase()=='east') &
                              (pl.col('student_rank').le(15))
                            ) 
                             | # or
                            ( (pl.col('region').str.to_lowercase()=='west') &
                              (pl.col('student_rank').le(5))
                            )
                       ).then(True)
                        .otherwise(False)
                        .alias('accepted_flag')
                      )
    )

# 3. Find the total number of accepted applicants per secondary school and 
#    represent this as a percentage of the total spaces that were available
#    for that region.
# 4. For each region, label their highest performing school as 
#    “High Performing”, the lowest performing school as “Low Performing” 
#    Give all other schools the status “Average Performing”, 
#    in a new column named “School Status”.

df_status = ( df.groupby(['region', 'school_name'])
                    .agg(count=pl.count('student_id'))
                .with_columns(pl.col('count')
                                .truediv(pl.col('count').sum()
                                           .over('region')
                                        )
                                .round(2)
                                .alias('pct_total_per_region')
                             )
                .with_columns(pl.col('pct_total_per_region').max()
                                .over('region')
                                .alias('highest_rate'),
                              pl.col('pct_total_per_region').min()
                                .over('region')
                                .alias('lowest_rate')
                            )
                .with_columns(pl.when(pl.col('pct_total_per_region')==pl.col('highest_rate'))
                                .then('High Performing')
                                .when(pl.col('pct_total_per_region')==pl.col('lowest_rate'))
                                .then('Low Performing')
                                .otherwise('Average Performing')
                                .alias('school_status'))
                .select(['school_name', 'school_status'])
            )

# join df_status to the main data set

df_out = ( df.join(df_status, on='school_name')
             .select(pl.exclude(['home_address', 
                                 'distance_from_school_(miles)',
                                 'student_rank', 'initials', 'school_id']
                               )
                    )
         )


# output the results
df_out.collect().write_csv(f"{Path('.')}/output-py-sol.csv")




