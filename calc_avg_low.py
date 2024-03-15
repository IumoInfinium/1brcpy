'''
Calculates the min, mean, max values for the stations in the measurements file 
'''
import argparse
import time
import mmap

def calculate_average(records: int, file_name: str = 'measurements.txt', separator: str = ';') -> None:
    '''
    Function to calculate values for each station from the provided file
    '''

    # mapping station name to the min value, max value, total sum of temp, total count  
    station_map = {}

    with open(file_name, 'r+b') as measurements_file:
        mmapped_file = mmap.mmap(
            fileno= measurements_file.fileno(),
            length= 0,
            access= mmap.ACCESS_READ,
            offset= 0
        )

        # count = 0
        line = ""
        while True:
            line = mmapped_file.readline()
            if line == b"":
                break

            line = str(line.decode('utf-8')).strip()
            station, temp = line.split(separator)
            temp = float(temp)

            if station not in station_map:
                station_map[station] = [temp, temp, temp, 1]
            else:
                station_map[station][0] = min(station_map[station][0], temp)
                station_map[station][1] = max(station_map[station][1], temp)
                station_map[station][2] += temp
                station_map[station][3] += 1
            # if count % 1_000_000 == 0:
            #     print(count)
            # count += 1

        mmapped_file.close()

    
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
        default= ";",
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