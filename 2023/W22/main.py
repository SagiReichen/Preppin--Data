#%%
from pathlib import Path

import polars as pl

# --------------------------------------------------------------------------------------
# load the file into a path object, and load the data sets
# --------------------------------------------------------------------------------------
file_path = Path('Student Attendance vs Scores.xlsx')

# loading attendance_fig sheet into a df
attendance_fig = pl.read_excel(file_path, sheet_id=1, 
                               read_csv_options={"has_header": True}
                              )


# loading student test scores sheet into a df
# transforming the dataset and combine it with the attendance_fig df
df = ( pl.read_excel(file_path, sheet_id=2,
                            read_csv_options={"has_header": True,
                                              "skip_rows": 1,
                                              "try_parse_dates": True})
              
                 .select(pl.all().exclude('test_date'))
                 .select(pl.all().exclude('subject'),
                         pl.col('subject').str.replace(r'^Eng.*?$', 'English')
                                          .str.replace(r'^Sci.*?$', 'Science'),
                         pl.col('student_name').str.split('_')
                                               .arr.to_struct()
                                               .struct.rename_fields(['first_name', 'last_name'])
                                               .alias('name_list'),
                         pl.col('test_score').round(0).alias('test_score_int')
                        )
                 .unnest('name_list')
                 # joining stud_scores with attendance_fig df
                 .join(attendance_fig, 
                       how='inner',
                       on='student_name')
                 .select(pl.all().exclude('student_name', 'attendance_percentage'),
                         pl.when(pl.col('attendance_percentage').lt(.7))
                           .then('Low Attendance')
                           .when(pl.col('attendance_percentage').gt(.9))
                           .then('High Attendance')
                           .otherwise('Medium Attendance')
                           .alias('attendance_flag'),
                         pl.col('attendance_percentage').cast(pl.Decimal(3, 2))
                        )
              )


# output the data set into a csv file
output_path = Path('.').absolute() / 'output_py_sol.csv'

df.write_csv(output_path, separator=",", has_header=True)



