
from pathlib import Path

import polars as pl

# import all files from the input directory
files_dir = Path('./input').absolute()
files = list(Path.glob(files_dir, '*.csv'))


# --------------------------------------------------------------------------------------------
# load all data sets and transform these
# --------------------------------------------------------------------------------------------

def main() -> pl.DataFrame:
    # input 1
    df_session_desc = ( pl.scan_csv(files[1], has_header=True,
                                with_column_names=lambda cols: [col.lower().replace(' ', '_') 
                                                                for col in cols])
                        .with_columns(pl.col('description').str.split_exact(':', 1))
                        .unnest('description')
                        .with_columns(
                                        (pl.col('field_0').str.slice(0, 1) + 
                                        pl.col('field_0').str.extract('\s+(.*)$').str.slice(0, 1))
                                        .alias('speaker_name_initials'),
                                        
                                        pl.col('field_1').str.strip()
                                                        .str.extract('(?i)(prep|server|community|desktop)')
                                                        .alias('subject'),

                                        pl.col('field_1').str.contains('dedup', literal=True).alias('on_deduplication?'),
                                        pl.col('field_1').str.strip().alias('presentation_description')
                                    )
                        .filter(pl.col('on_deduplication?')=='true')
                        .select('subject', 'speaker_name_initials', 'session_number', 'on_deduplication?')
                                                            
                    )


    # input 2
    df_input_2 = ( pl.scan_csv(files[0], 
                        with_column_names=lambda cols: [ col.lower().replace(' ', '_')
                                                            for col in cols])
                .melt(id_vars='room', variable_name='floor', value_name='presentation_desc')
                .filter(pl.col('presentation_desc').is_not_null())
                .with_columns(
                                (pl.col('floor').str.extract('^.*_(\d{1})$') +
                                pl.lit('0') +
                                pl.col('room').cast(pl.Utf8))
                                .cast(pl.Int16)
                                .alias('room_number'),

                                pl.col('presentation_desc')
                                    .str.split_exact('-', 1)
                                    .struct.rename_fields(['session_detail', 'speaker_subject'])
                                    .alias('fields')
                                )
                .unnest('fields')
                .select(
                            pl.all().exclude(['room', 'floor', 'speaker_subject']),
                            pl.col('speaker_subject').str.strip().str.slice(0, 2).alias('speaker_initials'),
                            pl.col('speaker_subject').str.strip().str.slice(6).alias('subject'),
                        )
                # joining the data set with the cleaned data set of output_1
                .join(df_session_desc,
                        how='inner',
                        left_on=['subject', 'speaker_initials'],
                        right_on=['subject', 'speaker_name_initials'])
                .filter(pl.col('room_number').str.starts_with('2'))
                .select('speaker_initials', 'subject', 'session_number', 'room_number')
            )



    # input 3
    df = ( pl.scan_csv(files[2], has_header=True,
                    with_column_names=lambda cols: [col.lower()
                                                    for col in cols]
                    )
            .melt(id_vars='room', 
                variable_name='room_num',
                value_name='distance_in_meters')
            .filter(pl.col('distance_in_meters').is_not_null())
            .select(pl.all().cast(pl.Int16))
            .filter(pl.col('room') != pl.col('room_num'))
            .select(
                    pl.all().exclude('session_number'),
                    ((pl.col('distance_in_meters') / 1.2) / 60).ceil().alias('mins_to_walk')
                    )
            .join(df_input_2, 
                left_on='room_num',
                right_on='room_number')
        ).rename({
                    'room' : 'room_A',
                    'room_num' : 'room_B',
                })

    return df.collect()



def filter_and_output(df: pl.DataFrame, room_num: int, output_path: Path) -> None:
    df_filtered = df.filter(pl.col('room_A')==room_num)
    df_filtered.write_csv(f'{output_path}/output-py-sol.csv')



if __name__ == '__main__':

    df_main = main()
    value_list = df_main['room_A'].to_list()

    options_str = '\n'.join([f'   {idx} - {item}'
                        for idx, item in enumerate(df_main['room_A'], 1)])
    
    room_col_len = len(value_list)
 

    while True:
        user_input = input(f'select a room (number between 1 - {room_col_len}) to check how long it takes to get there'
                           + f'or press Enter to exit the program: \n{options_str}\n')
                           
                           
        if user_input.isnumeric() and int(user_input) in range(1, room_col_len+1):
            room_x = value_list[int(user_input)-1]
            filter_and_output(df_main, room_x, Path.cwd())
            print(f'*** Output file was sucessfuly executed ***')
            break
        elif user_input == '':
            break
        else: 
            print(f'*** What you entered isn\'t a valid input')
