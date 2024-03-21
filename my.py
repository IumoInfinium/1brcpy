import time
import os
import concurrent.futures
# import pandas as pd
import polars as pl
# import io
import argparse

# FILE_NAME = 'vals.txt'
# FILE_NAME = 'm_1_000_000.txt'
# FILE_NAME = 'm_100_000_000.txt'
# FILE_NAME = 'm_1_000_000_000.txt'
READ_BUFFER = 2048*2048

def consumer(buffer: bytes)->dict:
    # mp = {}
    # line_bytes = []
    # for val in buffer:
    #     if val != 10:
    #         line_bytes.append(val)
    #         continue

    #     line = str(bytes(line_bytes).decode('utf-8')).strip()

    #     if line == '' :
    #         line_bytes = []
    #         continue
    #     # print(line)
    #     station, temp = line.split(';')
    #     temp = float(temp)

    #     if mp.get(station, None) == None:
    #         mp[station] = [temp, temp, temp, 1]
    #     else:
    #         mp[station][0] = min(mp[station][0], temp)
    #         mp[station][1] = max(mp[station][1], temp)
    #         mp[station][2] += temp
    #         mp[station][3] += 1

    #     # removes the previous line, making space for new input
    #     line_bytes = []
    # return mp

    # -------------------------

    # df = pd.read_csv(io.StringIO(buffer.decode('utf-8')), encoding='utf-8', sep=';', names=['station', 'temp'])

    # df = df.groupby('station').aggregate(
    #     min_value = ('temp', 'min'),
    #     max_value = ('temp', 'max'),
    #     sum_value = ('temp', 'sum'),
    #     count = ('temp', 'count'),
    # )
    # return df.to_dict('index')
    
    # ---------------
    df = pl.read_csv(buffer, separator=';', new_columns=['station', 'temp'])

    df = df.groupby('station').agg(
        pl.min("temp").alias("min_value"),
        pl.max("temp").alias("max_value"),
        pl.sum("temp").alias("sum_value"),
        pl.count("temp").alias("count"),
    )
    # return 's'
    return df.to_dicts()

def print_result(results: list):
    print('{')

    for res in results:
        print(
            f'{res[0]}={res[1][0]:.1f}/{res[1][2]/res[1][3]:.1f}/{res[1][1]:.1f}, ',
            end=''
        )
    print('\b\b}')

def run(FILE_NAME):
    
    f = open(FILE_NAME, 'rb')

    op = []

    last_buffer=b''
    with concurrent.futures.ProcessPoolExecutor(os.cpu_count()) as executor:
        while True:

            buff = f.read(READ_BUFFER)
            if buff == b'':
                break
            
            end = 0
            # print(len(buff))
            for i in range(len(buff)-1, -1, -1):
                if buff[i] == 10:
                    end = i
                    break

            if (end > len(buff)):
                end = len(buff)

            data = last_buffer + buff[:end]
            last_buffer = buff[end:]
            op.append(executor.submit(consumer, data))
            f.flush()

        f.close()
        station_mp = {}

        # for i in range(len(tasks)):
        for f in concurrent.futures.as_completed(op):
            res = f.result()
            # for k,v in res.items():
            #     if station_mp.get(k, None) == None:
            #         station_mp[k] = v
            #     else:
            #         station_mp[k][0] = min(station_mp[k][0], v[0]) 
            #         station_mp[k][1] = max(station_mp[k][1], v[1]) 
            #         station_mp[k][2] += v[2]
            #         station_mp[k][3] += 1

            # ------
            # for k,v in res.items():
            #     if station_mp.get(k, None) == None:
            #         station_mp[k] = [v['min_value'], v['max_value'], v['sum_value'], v['count']]
            #     else:
            #         station_mp[k][0] = min(station_mp[k][0], v['min_value']) 
            #         station_mp[k][1] = max(station_mp[k][1], v['max_value']) 
            #         station_mp[k][2] += v['sum_value']
            #         station_mp[k][3] += v['count']

            # ------
            for row in res:
                if station_mp.get(row['station'], None) == None:
                    station_mp[row['station']]  = [row['min_value'], row['max_value'], row['sum_value'], row['count']]
                else:
                    station_mp[row['station']][0] = min(station_mp[row['station']][0], row['min_value'])
                    station_mp[row['station']][1] = max(station_mp[row['station']][1], row['max_value'])
                    station_mp[row['station']][2] += row['sum_value']
                    station_mp[row['station']][3] += row['count']
                
        results = station_mp.items()
        results = sorted(results, key= lambda x: x[0])

        print_result(results)

if __name__ == '__main__':
    
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
        st = time.time()
        
        run(parsed_args.file_name)
        dataset.append(time.time() - st)
        
        print(f"It took {dataset[-1]:.12f} to measure them!")
        
    if test_times < 2 :
        pass

    print('\n\n ** Results **')
    for i in range(len(dataset)):
        print(f"Testcase {i} : Took {dataset[i]:.12f} to measure")