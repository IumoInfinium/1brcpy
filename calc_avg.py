'''
Calculates the min, mean, max values for the stations in the measurements file 
'''
import argparse
# from collections import Counter
# from typing import Dict
import time

def calculate_average(records: int, file_name: str = 'measurements.txt', separator: str = ';') -> None:
    '''
    Function to calculate values for each station from the provided file
    '''

    # mapping station name to the min value, max value, total sum of temp, total count  
    station_map = {}

    # read the input measurements file
    with open(file_name, 'r') as measurements_file:
        
        # progress counter
        progress_report_unit = records // 100
        
        # line indicator
        i: int = 0

        # read file line by line
        for line in measurements_file:
            station_name, temp = line.strip().split(';')

            # parse the temperature as a float with precision of 2
            try:
                temp = float(f"{float(temp):.2f}")
            except:
                raise Exception(f'Unable to parse the temperature parameter {temp}')
            
            # station occurs first time 
            if station_map.get(station_name, None) == None:
                station_map[station_name] = [temp, temp, temp, 1]
            
            # station occurs again
            else:
                station = station_map.get(station_name, None)
                station_map[station_name] = [
                    min(station[0], temp),
                    max(station[1], temp),
                    station[2] + temp,
                    station[3] + 1
                ]
            # print(station_map[station_name])
                
            # if i > 0 and i % progress_report_unit == 0:
                # print(
                #     f"~ Checked {i:,} measurements in {time.time() - start_time:.2f} sec"
                # )
            i += 1


    print('{', end='')    
    all_stations = sorted(station_map.items())
    
    for station, station_info in all_stations:
        print(
            f"{station};{station_info[0]};{station_info[1]};{station_info[2]/station_info[3]:.1f}", end=", "
        )

    print("\b\b} ")

def args_parse_records(records: int ) -> int:
    '''
    Parses the Value in the records parameter of the arguments to integer in the range [1,n]
    '''
    try:
        value  = int(records)
    except Exception:
        raise argparse.ArgumentTypeError(f"invalid value, support integer {records}") 
    
    if value < 1:
        raise argparse.ArgumentTypeError(f"Minimum number of records need to be created is 1")
    
    else:
        return value

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
        '-r',
        '--records',
        type = int,
        dest = "records",
        default= 10000,
        help = "Number of records in the file"
    )

    parser.add_argument(
        '-t',
        '--test',
        type = int,
        dest = "number_of_times",
        default= 1,
        help = "Number of times to check(default = 1, max = 3)"
    )
    parsed_args = parser.parse_args()
    records = args_parse_records(parsed_args.records)
    test_times = parsed_args.number_of_times

    if test_times < 0:
        raise Exception('Atleast 1 test case should be there')
        # test_times = 1
    elif test_times > 3:
        raise Exception('Cannot use more than 3 test cases')
        # test_times = 3

    dataset = []

    for testcase in range(test_times):
        start_time = time.time()
        calculate_average(
            records= records,
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