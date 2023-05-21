

from pathlib import Path

import polars as pl
from pandas import read_excel

from custom_exceptions import InputIsNotNumber, InputNumberIsNotValid, YearIsOutOfRange

# -------------------------------------------------------------------------
# creating the paths for the data sets
# -------------------------------------------------------------------------

pop_path = Path('./input/Population Data.xls').absolute()
countries_path = Path('./input/country size.csv').absolute()


# -------------------------------------------------------------------------
# defining function to get the years range from the data set population
# -------------------------------------------------------------------------

def extract_range() -> tuple[str, str]:
    """
    Getting the years range in the data set
    """
    df_population = pl.from_pandas(read_excel(pop_path, sheet_name=0, skiprows=3, header=0))
    df_pop_years = [int(col)
                    for col in df_population.columns
                    if col.isnumeric()]

    df_pop_min_year, df_pop_max_year = str(min(df_pop_years)), str(max(df_pop_years))

    return df_pop_min_year, df_pop_max_year



# -------------------------------------------------------------------------
# load the data and clean them before merging 
# -------------------------------------------------------------------------


def main(min_year: str, max_year: str) -> None:
    df_pop = ( pl.from_pandas(read_excel(
                                            pop_path, 
                                            sheet_name=0,
                                            skiprows=3,
                                            header=0
                                        )
                            )
                .select(
                        pl.col('Country Name').str.strip().alias('country_name'),
                        pl.all().exclude(['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']),
                        )
                .melt(
                        id_vars='country_name',
                        variable_name='year',
                        value_name='population'
                    )
                .select(
                        pl.all().exclude(['population','year']),
                        pl.col('year').cast(pl.Int16),
                        pl.col('population').cast(pl.Int64).alias('population_num')
                        )
            ).lazy()


    df_cntrs = ( pl.read_csv(
                            countries_path, has_header=True,
                            separator=',', encoding='utf8-lossy'
                            )
                .select(
                            pl.col('Country / Dependency').str.strip()
                                                        .str.replace('\[|\(', '***')
                                                        .str.split(by='***')
                                                        .arr.first()
                                                        .str.strip()
                                                        .str.replace('Jersey|Guernsey','Channel Islands')
                                                        .alias('country'),
                            pl.col('Total in km2 (mi2)').str.split(by='(')
                                                        .arr.first()
                                                        .str.strip()
                                                        .str.replace_all(',', '', literal=True)
                                                        .cast(pl.Float32)
                                                        .alias('land_size_km2')
                        )
                .filter(pl.col('country')!='World')
                .groupby(by='country').agg(pl.sum('land_size_km2'))
            ).lazy()



    # -------------------------------------------------------------------------
    # load the data and clean them before merging 
    # -------------------------------------------------------------------------

    # join the two data sets together and use the min_year and max_year variable
    df = ( df_pop.join(df_cntrs,
                    left_on='country_name',
                    right_on='country',
                    how='inner'
                    )
                .select( 
                        pl.all().exclude(['population_num', 'land_size_km2']),
                        (pl.col('population_num') / pl.col('land_size_km2')).round(2).alias('population_density')
                        )
        )

    df = ( df.filter(pl.col('year').is_in([int(min_year), int(max_year)])).collect()
            .pivot(values='population_density', index='country_name', columns='year', aggregate_function='max')
            .with_columns((pl.col(max_year).truediv(pl.col(min_year)) -1).round(2).alias('population_growth_pct'))
            .with_columns(
                            pl.col(max_year).rank(method='dense', descending=True).alias('rank_by_max_year_pop_density'),
                            pl.col('population_growth_pct').rank(method='dense', descending=True).alias('rank_by_growth_rate')
                        )
        )



    # -------------------------------------------------------------------------
    # outputs to csv files
    # -------------------------------------------------------------------------

    df_output1 = ( df.select(pl.all().exclude('rank_by_max_year_pop_density'))
                    .top_k(k=10, by='rank_by_growth_rate', descending=True)
                 )

    df_output2 = ( df.select(pl.all().exclude('rank_by_growth_rate'))
                    .top_k(k=10, by='rank_by_max_year_pop_density', descending=True)
                )
    

    df_output1.write_csv(f'{Path().absolute()}/output/output1.csv')
    df_output2.write_csv(f'{Path().absolute()}/output/output2.csv')



# -------------------------------------------------------------------------
# user's input for years' comaprison
# -------------------------------------------------------------------------

if __name__ == "__main__":
    
    min_year_in_data, max_year_in_data = extract_range()

    min_year_input = input(f'Select a minimum year ({min_year_in_data} - {max_year_in_data}): ')
    max_year_input = input(f'Select a minimum year ({min_year_in_data} - {max_year_in_data}): ')

    # checks for the inputs
    if not ( (min_year_input.isnumeric()) and (max_year_input.isnumeric()) ):
        raise InputIsNotNumber('The values entered must include only digits (0-9)')

    if not ( (len(min_year_input)==4) and (len(max_year_input)==4) ):
        raise InputNumberIsNotValid('The values for the years must be 4 digits') 

    if not ( (int(min_year_input) >= int(min_year_in_data)) and (int(max_year_input) <= int(max_year_in_data)) ):
        raise YearIsOutOfRange('The year entered was out of range')
                

    # run the main funciton
    main(min_year=min_year_input, max_year=max_year_input)

    print('**********\n SUCCESS \n**********')



