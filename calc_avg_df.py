import argparse
import time
import pandas as pd


def calculate_average_dataframe(
    file_name: str = 'measurements.txt',
    separator: str = ';'
) -> None:
    '''
    Function to calculate values for each station from the provided file using the pandas dataframe
    '''

    station_df = pd.DataFrame(columns= ['station', 'min_value', 'max_value', 'sum_value', 'count'])

    chunksize = 100_000_000
    chunk: pd.DataFrame = None
    processed = 0

    for chunk in pd.read_csv(file_name, sep=separator, encoding='utf-8', chunksize = chunksize, names=['station', 'temp']):


        result = chunk.groupby(
            'station'
        ).agg(
            min_value = ('temp', 'min'),
            max_value = ('temp', 'max'),
            sum_value = ('temp', 'sum'),
            count = ('station', 'count'),
        )
        
        for row in result.iterrows():           
            if not row[0] in list(station_df['station']):
                station_df.loc[len(station_df)] = {
                        'station' : row[0],
                        'min_value' : row[1]['min_value'],
                        'max_value' : row[1]['max_value'],
                        'sum_value': row[1]['sum_value'],
                        'count': row[1]['count']
                    }
                # print('created')
            else:
                station_instance = station_df.loc[station_df['station'] == row[0]]
                station_df.loc[station_instance.index, 'min_value'] = min(station_instance['min_value'].values[0],row[1]['min_value'])
                
                station_df.loc[station_instance.index, 'max_value'] = max(station_instance['max_value'].values[0],row[1]['min_value'])
                
                station_df.loc[station_instance.index, 'sum_value'] = station_instance['sum_value'].values[0] + row[1]['min_value']
                
                station_df.loc[station_instance.index, 'count'] = station_instance['count'].values[0] + 1

        processed = sum(station_df['count'])
        print(
            f"~ Processed {processed} lines"
        )

 
    station_df['mean_value'] = (station_df['sum_value']/station_df['count']).map("{:.1f}".format).map(float)
    print('s',sum(station_df['count']))

    station_df = station_df.drop('sum_value', axis = 1)
    station_df = station_df.drop('count', axis = 1)
    
    print('{')
    
    for row in station_df.iterrows():
        row = row[1] 
        print(row['station'], row['max_value'])
        # break
        # print(f"{row[0]}={float(row[1]):.1f}/{float(row[3]):.1f}/{float(row[1]):.1f}, ", end='')
        print(
            f"{row['station']}={row['min_value']:.1f}/{row['mean_value']:.1f}/{row['max_value']:.1f}",
            end = ", "
        )
    print('}')


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Calculate average(min, mean, max) for the measurement's file")

    parser.add_argument(
        '-f',
        '--file',
        type = str,
        dest = "file_name",
        default= "measurements.txt",
        help = "Provide the measurement file to read"
    )

    parser.add_argument(
        '-s',
        '--separator',
        type = str,
        dest = "separator",
        default= "l",
        help = "Split the files based on the separator"
    )


    parser.add_argument(
        '-t',
        '--test',
        type = int,
        choices=[1,2,3,4,5],
        dest = "number_of_times",
        default= 1,
        help = "Number of times to check(default = 1, max = 3)"
    )
    parsed_args = parser.parse_args()
    test_times = parsed_args.number_of_times

    if test_times < 0:
        raise Exception('Atleast 1 test case should be there')
    
    elif test_times > 5:
        raise Exception('Cannot use more than 5 test cases')
    

    dataset = []

    for testcase in range(test_times):
        start_time = time.time()
        calculate_average_dataframe(
            file_name = parsed_args.file_name,
            separator = parsed_args.separator,
        )

        dataset.append(time.time() - start_time)
        print(
            f"It took {dataset[-1]:.12f} to measure them!"
        )
    
    if test_times < 2 :
        pass
    
    print('\n\n ** Results **')
    for i in range(len(dataset)):
        print(
            f"Testcase {i} : Took {dataset[i]:.12f} to measure"
        )