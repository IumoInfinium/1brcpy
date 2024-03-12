'''
Calculates the min, mean, max values for the stations in the measurements file  with two pointer approach
'''
import argparse
import time
import concurrent.futures
import io
import os

def calculate_average(records: int, file_name: str = 'measurements.txt', separator: str = ';') -> None:
    '''
    Function to calculate values for each station from the provided file
    '''
    station_map = {}
    chunk_count = 0
    line_count = 0
    chunk_size = 10_000
    max_workers = 2

    def process_chunk(chunk: list):
        '''
        Process a chunk for measurements of line and add them in the map 
        '''
        mp = {}
        for line in chunk:
            station, temp = line.strip().split(';')
            temp = float(temp)
            if mp.get(station, None) == None:
                mp[station] = [temp, temp, temp, 1]
            else:
                mp[station][0] = min(mp[station][0], temp)
                mp[station][1] = max(mp[station][1], temp)
                mp[station][2] = mp[station][2] + temp
                mp[station][3] += 1
        print('Doe')
        return mp
    
    with concurrent.futures.ThreadPoolExecutor(max_workers= max_workers) as executor:

        future_to_process = {}

        with open(file_name, 'rb') as f:
            # start_pos = 0
            # end_pos = os.stat(file_name).st_size
            # print(start_pos, end_pos)
            curr_context = []
            curr_line = ""
            c = f.read()
            for i in range(10):
                print(c)
                c = f.read()

            f.close()
            # while start_pos <= end_pos:
            #     c = f.read(1)
            #     print(str(c))
            #     while str(c) != '\n':
            #         print(str(c))
            #         curr_line += str(c)
            #         c = f.read()
                
            #     if c == '\n':
            #         print(f"~ {line_count }")
            #         line_count += 1
            #         curr_context.append(curr_line)
            #         curr_line = ""

            #     if len(curr_context) == chunk_size:
            #         future_to_process[executor.submit(process_chunk, curr_context)] = chunk_count
            #         curr_context = []
            #         curr_line = ""

            #     start_pos = f.tell()


        # with open(file_name, 'r', encoding='utf-8') as measurement_file:
            
            # curr_context = []
            
            # while True:
            #     line_char = measurement_file.read(1)
            #     print(f"~ {line_count }")
            #     if line_char == '':
            #         break
            #     while len(curr_context) < chunk_size and measurement_file.read(1) != '':
            #         curr_context.append(measurement_file.readline())
            #         line_count += 1

            #     if len(curr_context) == chunk_size:
            #         # future_to_process[executor.submit(process_chunk, curr_context)] = chunk_count
            #         print(f'Chunk = {chunk_count}')
            #         chunk_count += 1
            #         curr_context.clear()
        
                

        # for future in concurrent.futures.as_completed(future_to_process):
        #     chunk_number = future_to_process[future]
        #     try:
        #         mapped_chunk = future.result()

        #         for k,v in mapped_chunk:
        #             if station_map.get(k, None) == None:
        #                 station_map[k] = v
        #             else:
        #                 station_map[k][0] = min(station_map[k][0], v[0])
        #                 station_map[k][1] = max(station_map[k][1], v[1])
        #                 station_map[k][2] += v[2]
        #                 station_map[k][3] += v[3]

                
        #     except Exception as exc:
        #         print(f"O_O/ Generated an exception {exc} in chunk number {chunk_number}")
        #     else:
        #         print(
        #             f"~ Updated {len(mapped_chunk)}"
        #         )

    
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