# 1brc Challenge - Python

## Intro

This is my version of 1BRC - 1 billion row challenge, where you need to process the 1 billion lines of text with the format ***station;temperature***
And calculate the minimum, maximum and average temperature of each and every station.

Accomplish this with only internal libraries. There is hardware limitation as well(check blog post to know)

Blog Post : [Morling's blog on 1BRC](https://www.morling.dev/blog/one-billion-row-challenge/)

Github Repository - [Morling's 1BRC](https://github.com/gunnarmorling/1brc/)

> Note : Don't raat after checking the files ('>')/
> Also, other people have used methods like red-bl

### Pre-check System Config

- Windows 10 - i5 10300H
- 8 GB, super-fast 2933 MHz RAM
- Python 3.12

## Problems

- I/O read limitation 
- Python's GIL, parallelism is slow because of this! (Ahh.. pain)

## Final Results


### UPDATE (21/03/2024)

Using `buffering + multiprocessing + polars`, to get the result under a minute, to be exact ***43 sec***!(GIL still exists though)

#### With polars as a external module

From : [calc_avg_df.py](#third-try)

Best Result :  ~ 205 sec

#### without any external module

Avg Results :  ~ 300 sec

> Ran the code on 8-core linux machine without any changes, it's now < `191 sec`.

> Btw, running this code using PyPy3, may even execute it in `~ 100 sec`

### Naive Solution

Logic : Map(using `dict`) every Station with 4 values -> min, max, sum and count

Go through every line in the file one by one, update the map. At the end we have a structure of all the stations.
Now just iterate through the dict structure and print the stuff and calculate the mean based on ithsum and count.

- Runtime approximately : ~ 1657.77 sec.
- Check the implementation in [`calc_avg.py`](./calc_avg.py)

### Trying with Pandas

#### first try

Failed. Loading the entire 1,000,000,000 records of text in the memory obviously fails...

#### second try

chunking the dataframe into 10 million rows and creating a two dataframes, `main` and `sub` datframe on those chunks and grouping them, and updating the **main** dataframe.

- Runtime : ~ 357 sec.
- Function name: `calculate_avg_dataframe`
- Check the implementation in [`calc_avg_df.py`](./calc_avg_df.py)

#### third try

this approach is same as the second approach but instead of processing after every chunk(dataframe 1-by-1)(not using boundaries here), you pass it into a parallel multiprocessing executor which evalutes it to a dict.

From all the dataframe chunks, we get a list a maps. Combine them together creating a single map.

A single map is the result of all the lines processes. Now just need to output it.

- Runtime : ~ 205 sec
- Function name: `calculate_avg_dataframe_2`
- Check the implementation in [`calc_avg_df.py `](./calc_avg_df.py)

### Parallelism

Get the chunks boundaries based on the number of processors on the system.

Parallely process them using the `multiprocessing.Pool` and using `starmap` to compute the chunk results(`dict`).

Now just iterate over all resultant dict and update them in a single map like last approach and get the final map.

- Runtime : ~ 361.6 sec
- Check the implementation in [`calc_avg_improvement.py`](./calc_avg_improvement.py)

### MMAP

#### raw version

Using the memory mapped files, avoiding the memory overheads, raw version use maps the whole file in the virtually mapped memory, and does normal Map/Reduce algorithm to calculate answer.

- Runtime : ~ 1286.2 sec
- Check the implementation in [`calc_avg_low.py`](./calc_avg_low.py)


#### mmap and parallelism

Instead for memory mapping whole file and computing 1 billions lines of text, first find chunk boundaries (N chunks, number of physical processors), then for each nmap the file and find move the file pointer to that memory location and calculate the chunk's answer. Do for N chunks, and them compute the combined res.

- Runtime: ~ 325.42 sec.
- Check the implmentation in [`mmap_multiprocessing.py`](./mmap_multiprocessing.py)


### Buffering

The idea is to still use the map approach but file read is done in batches of `4MB`(reducing the memory and cpu overhead a time) and those batches are multiprocessed(python's `ProcessPoolExecutor`) and each of them return a sub-result and later those are aggregated to get the final answer.

- Runtime: ~ 43 sec.
- Check the implmentation in [`my.py`](./my.py)

## side notes

- Create measurement files based on the following naming convention `m_1_000_000.txt`, shows the file content 1 million records.

- Creation of measurement file can be done using

    ```python create_measurements.py -o m_1_000_000.txt -r 1000000```

    - `-o` is the output file name
    - `-r` is the number of records in the file to be created  

- Calculation of measurement file can be done using

    ```python calc_avg_df.py -f m_1_000_000.txt -t 2```

    - `-f` is the input file name
    - `-t` is the number of times is test has to be done (limited 5 for now)  

### learnings

(what i learned)

- multiprocessing and multi-threading
- polars (dang it's fast)
- mmap, anonymous memory
- Python's GIL is a pain