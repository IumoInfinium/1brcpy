import argparse
import time
import pandas as pd

import os
import concurrent.futures
import multiprocessing
import io

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


def _process_df(df: pd.DataFrame, *args, **kwargs) -> dict:
    '''
    Process the dataframe and return a dict of unique objects
    '''
    new_df = df.groupby(
        'station'
    ).agg(
        min_value = ('temp', 'min'),
        max_value = ('temp', 'max'),
        sum_value = ('temp', 'sum'),
        count = ('station', 'count'),
    )
    return new_df.to_dict('index')

def get_chunks(file_name, separator, chunksize):
    result = []

    for chunk in pd.read_csv(file_name, sep=separator, encoding='utf-8', chunksize= chunksize, names = ['station', 'temp']):
        result.append(chunk)
    
    print(len(result), type(result))
    map(print(type) , result)
    return result

def calculate_average_dataframe_2(
    file_name: str = 'measurements.txt',
    separator: str = ';'
) -> None:
    '''
    Function to calculate values for each station from the provided file using the pandas dataframe
    '''

    chunksize = 50_000_000
    chunk: pd.DataFrame = None

    station_map = {}
    future_to_chunks = {}

    def print_result():
        all_stations = sorted(station_map.items())
        
        print('{', end='')    
        for station, station_info in all_stations:
            print(
                f"{station};{station_info[0]};{station_info[1]};{station_info[2]/station_info[3]:.1f}", end=", "
            )

        print("\b\b} ")

        
    with concurrent.futures.ThreadPoolExecutor(max_workers= os.cpu_count()) as executor:
        # st = time.time()
        # cnt = 1
        for chunk in pd.read_csv(file_name, sep=separator, encoding='utf-8', chunksize= chunksize, names = ['station', 'temp']):
            # pass
            future_to_chunks[executor.submit(_process_df, chunk)] = chunksize
            # break
            # print(time.time() - st, cnt)
            # cnt += 1
    
        for future in concurrent.futures.as_completed(future_to_chunks):
                chunk_result = future.result()
        
                for k,v in chunk_result.items():
                    if k not in station_map:
                        station_map[k] = [v['min_value'], v['max_value'], v['sum_value'], v['count']]
                    else:
                        station_map[k][0] =  min(station_map[k][0], v['min_value'])
                        station_map[k][1] =  min(station_map[k][1], v['max_value'])
                        station_map[k][2] += v['sum_value']
                        station_map[k][3] += v['count']


    # chunks = get_chunks(file_name, separator, chunksize)
    # process_chunks(chunks)
    print_result()


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
        default= ";",
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
        calculate_average_dataframe_2(
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