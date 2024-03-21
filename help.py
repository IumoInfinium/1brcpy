import time
import asyncio
import io

file_name = 'm_100_000_000.txt'
buffer_read_size = 4096*4096
WORKERS = 5


async def consumer(curr_worker, input, output_channels):
    idx = 0

    n = len(input)

    mp = {}
    line = []

    for val in input:
        # print(val)
        
        line.append(val)
        
        if val != 10:
            continue
        
        words = str(bytes(line).decode('utf-8')).strip()
        try:
            station, temp = words.strip().split(';')
        except:
            print('asdasd', line, bytes(line), words)
            break
        temp = float(temp)
        if mp.get(station, None) == None: 
            mp[station] = [temp, temp, temp, 1]
        else:
            mp[station][0] = min(mp[station][0], temp)
            mp[station][1] = max(mp[station][1], temp)
            mp[station][2] += temp
            mp[station][3] += 1

        line = []
    # while idx < n:
        

    #     while idx < n and input[idx] != 10:
    #         line.append(input[idx])
    #         idx += 1


    #     words = str(bytes(line).decode('utf-8')).strip()
    #     if(words == ''):
    #         idx += 1
    #         continue
    #     try:
    #         station, temp = words.strip().split(';')
    #     except:
    #         print(line, '+++', bytes(line), '+++ ',words)
    #         raise Exception(f"___{words}____")
    #     temp = float(temp)
    #     if mp.get(station, None) == None: 
    #         mp[station] = [temp, temp, temp, 1]
    #     else:
    #         mp[station][0] = min(mp[station][0], temp)
    #         mp[station][1] = max(mp[station][1], temp)
    #         mp[station][2] += temp
    #         mp[station][3] += 1
    #     idx += 1
    #     line = []

    output_channels[curr_worker] = mp

async def aggregate(output_channels):
    # aggregate the results from all the output channels
    stations = {}

    for i in range(len(output_channels)):
        for k,v in output_channels[i].items():
            if stations.get(k, None) == None:
                stations[k] = v
            else:
                stations[k][0] = min(stations[k][0], v[0])
                stations[k][1] = max(stations[k][1], v[1])
                stations[k][2] += v[2]
                stations[k][3] += v[3]

    return stations

async def print_results(res: list):
    # print the results
    print('{')

    # print(res)
    for i in range(len(res)):
        # print(res[i])
    #     print(res[i][0], type(res[i][0]))
        print(
            f"""{res[i][0]}={float(res[i][1][0]):.1f}/{float(res[i][1][2]/res[i][1][3]):.1f}/{float(res[i][1][1])}, """, end=''
        ) 

    print('\b\b}')

async def run():
    try:
        f = open(file_name, 'rb')
    except:
        raise Exception('not able to read file')
    
    stations = {}
    last_buffer=b''
    left_over_size = 0

    # input_channels = ['' * WORKERS]
    output_channels = [{} for _ in range(WORKERS)]
    print(len(output_channels))
    curr_worker = 0
    while True:
        buff = f.read(buffer_read_size)

        if buff == b'':
            break
        
        n = len(buff)
        
        end = 0

        for i in range(n-1, -1, -1):
            print(buff[i])
            if buff[i] == 10:
                end = i
                break
            
        data = last_buffer[:left_over_size]
        data = data[left_over_size:] + buff[:end]
        last_buffer = buff[end+1: n]
        left_over_size = n - end -1
        # print(data)    
        print(' ----------------------------- ')
            
        print(curr_worker)
        # input_channels[curr_worker] = data
        
        await consumer(curr_worker, data, output_channels)
        
        curr_worker += 1

        if curr_worker >= WORKERS:
            curr_worker = 0
        # await consumer(data, stations)
        f.flush()
    f.close()

    
    results = await aggregate(output_channels)

    results = [[k, v] for k,v in results.items()]

    results = sorted(results, key = lambda x: x[0])

    await print_results(results)

if __name__ == '__main__':

    st = time.time()
    # await run()
    asyncio.run(run())
    print(time.time() - st)