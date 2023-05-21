from collections.abc import Iterator
from glob import iglob  # returns a generator instead of a list
from pathlib import Path

import polars as pl

# polars settings
# --------------------------------
pl.Config.set_fmt_str_lengths(100)
pl.Config.set_tbl_cols(15)
pl.Config.set_tbl_rows(30)


# user's input to allow the user to select the rolling number of trades to be incorporated into the moving average

def get_n_for_moving_avg() -> int:
    is_number:bool = False
    while not is_number:

        user_input:str = input('Type a number for setting up the window frame for the calculated moving average: ')

        if user_input.isdigit():
            n_prev_trades:int = int(user_input)
            is_number:bool = True
        else:
            print('Incorrect input. Enter only numbers [0-9]')
    return n_prev_trades



# ----------------------------------------------------------------------------------------
# load the data files into a data frame and transform it as required
# ----------------------------------------------------------------------------------------

def transform(files: Iterator[str]) -> pl.DataFrame:
    rolling_window_size = get_n_for_moving_avg()

    df = ( pl.concat([ pl.scan_csv(file, has_header=True, separator=',', 
                                with_column_names=lambda cols: [col.lower() for col in cols])
                        # creating the file path name column for each file and converting the purchase price field to be a float value
                        .with_columns([pl.lit(file).str.extract(r'DATA-(.*?).csv$', group_index=1)
                                        .fill_null(1)
                                        .cast(pl.Int8)
                                        .alias('month_number'),
                                        pl.col('purchase price').str.replace('$', '', literal=True)
                                        .cast(pl.Float64)
                                        .alias('purchase_price')])
                    for file in files ], 
                    how='vertical')
            # creating the trade_order col by using the struct data type to group both month and id
            .with_columns([pl.struct('month_number', 'id').rank('dense').over('sector').alias('trade_order'),
                            pl.struct('month_number', 'id').alias('struct')])
            # selecting only required fields
            .select(['trade_order', 'sector', 'purchase_price'])
            .sort(by=['sector', 'trade_order'], descending=[False, False])
            # creating the moving avg | window's size is according to user's input
            .with_columns([
                            pl.col('purchase_price').rolling_mean(window_size=rolling_window_size).over('sector')
                            .round(2)
                            .alias(f'{rolling_window_size}_prev_moving_avg')
            ])
        ).collect()


            # creating a rank column for keeping only 100 most recent rows per sector
    df_out = df.with_columns([pl.col('trade_order').sort(descending=True).rank('dense').over('sector').alias('most_recent')]) \
            .filter(pl.col('most_recent') <= 100) \
            .with_columns(pl.col('most_recent').sort(descending=False).rank('dense').over('sector').alias('prev_trades')) \
            .select(pl.all().exclude(['most_recent', 'purchase_price']))


    # ---------------------------------------------------------------------------------
    # output the data into a csv file
    # ---------------------------------------------------------------------------------

    df_out.write_csv(f'{Path.cwd()}/output-py-sol.csv', has_header=True, separator=',', float_precision=2)



if __name__ == '__main__':
    inputs = Path(__file__).parent.joinpath('input')
    data_files = iglob(f'{inputs}/*.csv')
    transform(data_files)

