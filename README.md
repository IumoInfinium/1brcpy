# 1brc Challenge - Python

> Note : Don't raat after checking the files ('>')/

### Naive Solution


Logic : Map(using `dict`) every Station with 4 values -> min, max, sum and count

Go through every line in the file one by one, update the map. At the end we have a structure of all the stations.
Now just iterate through the dict structure and print the stuff and calculate the mean based on ithsum and count.

Runtime approximately : ~ 28 min 15 sec

Check the implementation in [`calc_avg.py`](./calc_avg.py)

### Trying with Pandas

- First try failed. Loading the entire 1,000,000,000 records of text in the memory obviously fails...

- Second try,  chunking the dataframe into 10 million rows and creating a two dataframes, `main` amd `sub` datframe on those chunks and grouping them, and updating the **main** dataframe.

Runtime approximately : ~ 4.8 min

Check the implementation in [`calc_avg_df.py`](./calc_avg_df.py)

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


