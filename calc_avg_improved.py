'''
Calculates the min, mean, max values for the stations in the measurements file  with two pointer approach
'''
import argparse
import time
import multiprocessing
import io
import os

def _process_file_chunk(file_name, start, end) -> dict:
    '''
    Process the measurement file from start and end chunk positions
    Based in parallet
    '''
    mp = {}

    with open(file_name, 'r') as f:
        f.seek(start)
        for line in f:
            start += len(line)

            if start > end:
                break
            
            station, temp = line.strip().split(';')
            temp = float(temp)

            if station not in mp:
                mp[station] = [temp, temp, temp, 1]
            else:
                mp[station][0] =  min(mp[station][0], temp)
                mp[station][1] =  min(mp[station][1], temp)
                mp[station][2] += temp
                mp[station][3] += 1
    return mp


def get_chunks(file_name):
        cpu_count = os.cpu_count()
        file_size = os.path.getsize(file_name)
        chunk_size = file_size // cpu_count
        print(f"** File size  : {file_size}")
        print(f"** Chunk size : {chunk_size}")

        chunk_boundaries = []

        with open(file_name, 'r+b') as f:

            def is_new_line(pos):
                if pos == 0:
                    return True
                else:
                    f.seek(pos-1)
                    return f.read(1) == b'\n'
            
            def next_line(pos):
                f.seek(pos)
                f.readline()
                return f.tell()
            
            chunk_start = 0

            while chunk_start < file_size:
                chunk_end = min(chunk_start + chunk_size, file_size)

                while not is_new_line(chunk_end):
                    chunk_end -= 1
                
                if chunk_start == chunk_end:
                    chunk_end = next_line(chunk_end)
                
                print(f"~ Chunk start: {chunk_start}")
                print(f"~ Chunk end: {chunk_end}")
                chunk_boundaries.append((file_name, chunk_start, chunk_end))
                
                chunk_start = chunk_end

        return (cpu_count, chunk_boundaries)

def calculate_average(records: int, file_name: str = 'measurements.txt', separator: str = ';') -> None:
    '''
    Function to calculate values for each station from the provided file
    '''
    
    def process_file(cpu_count: int, chunks: list) -> None:
        station_map = {}
        with multiprocessing.Pool(cpu_count) as p:
            chunk_results = p.starmap(_process_file_chunk, chunks)

            for chunk_result in chunk_results:
                for k,v in chunk_result.items():
                    if k not in station_map:
                        station_map[k] = v
                    else:
                        station_map[k][0] =  min(station_map[k][0], v[0])
                        station_map[k][1] =  min(station_map[k][1], v[1])
                        station_map[k][2] += v[2]
                        station_map[k][3] += v[3]

        print('{', end='')    
        all_stations = sorted(station_map.items())
        
        for station, station_info in all_stations:
            print(
                f"{station};{station_info[0]};{station_info[1]};{station_info[2]/station_info[3]:.1f}", end=", "
            )

        print("\b\b} ")


    cpu_count, *chunks = get_chunks(file_name)
    process_file(cpu_count, chunks[0])    

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
            f"\nIt took {dataset[-1]:.12f} to measure them!"
        )
    
    if test_times < 2 :
        pass
    
    print('\n\n ** Results **')
    for i in range(len(dataset)):
        print(
            f"Testcase {i} : Took {dataset[i]:.12f} to measure"
        )