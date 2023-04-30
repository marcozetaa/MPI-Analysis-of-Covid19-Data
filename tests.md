Tests were made without the prints of data, only printing the total time. <br>
(In the program, printResults() calls were commented)

<br>
Based on the dataset and systems used, we obtained the following results.
<br>

------------------------------------------------
Single machine with 3 cores, 2GB of memory, n total cores used in the computation (m,k)
- m is the local machine (the one running the command)
- k is the ssh machine
<br><br>

super-reduced-dataset
- 2 cores
    - 5, 3, 3 milliseconds
    - Average: 4 ms
- 3 cores
    - 2, 3, 2 milliseconds
    - Average: 2 ms

reduced-dataset
- 2 cores
    - 305, 304, 303 milliseconds
    - Average: 304 ms
- 3 cores
    - 324, 327, 339 milliseconds
    - Average: 330 ms

input
- 2 cores
    - 1244, 1230, 1238 milliseconds
    - Average: 1237 ms
- 3 cores
    - 1352, 1322, 1332 milliseconds
    - Average: 1335 ms

------------------------------------------------
Machines with 2 cores each, 2GB of memory each, n total cores used in the computation (m,k)
- m is the local machine (the one running the command)
- k is the ssh machine
<br><br>

super-reduced-dataset
- 2 total cores (1,1)
    - 26, 37, 30 milliseconds
    - Average: 31 ms
- 3 total cores (2,1)
    - 31, 26, 37 milliseconds
    - Average: 31 ms
- 4 total cores (2,2)
    - 57, 36, 49 milliseconds
    - Average: 47 ms

reduced-dataset
- 2 total cores (1,1)
    - 809, 771, 766 milliseconds
    - Average: 782 ms
- 3 total cores (2,1)
    - 675, 599, 651 milliseconds
    - Average: 641 ms
- 4 total cores (2,2)
    - 809, 714, 700 milliseconds
    - Average: 741 ms

input
- 2 total cores (1,1)
    - 2784, 2716, 2743 milliseconds
    - Average: 2747 ms
- 3 total cores (2,1)
    - 2270, 2274, 2443 milliseconds
    - Average: 2329 ms
- 4 total cores (2,2)
    - 2640, 3022, 2969 milliseconds
    - Average: 2877 ms

------------------------------------------------
Machines with 3 cores each, 2GB of memory each, n total cores used in the computation (m,k)
- m is the local machine (the one running the command)
- k is the ssh machine
<br><br>

super-reduced-dataset
- 2 total cores (1,1)
    - 34, 25, 25 milliseconds
    - Average: 28 ms
- 4 total cores (2,2)
    - 45, 60, 47 milliseconds
    - Average: 50 ms
- 4 total cores (3,1)
    - 35, 24, 33 milliseconds
    - Average: 30 ms
- 5 total cores (3,2)
    - 47, 58, 65 milliseconds
    - Average: 56 ms
- 6 total cores (3,3)
    - 62, 62, 73 milliseconds
    - Average: 65 ms

reduced-dataset
- 2 total cores (1,1)
    - 739, 742, 707 milliseconds
    - Average: 729 ms
- 4 total cores (2,2)
    - 860, 725, 747 milliseconds
    - Average: 777 ms
- 4 total cores (3,1)
    - 699, 703, 698 milliseconds
    - Average: 700 ms
- 5 total cores (3,2)
    - 832, 908, 870 milliseconds
    - Average: 870 ms
- 6 total cores (3,3)
    - 1119, 1014, 1126 milliseconds
    - Average: 1086 ms

input
- 2 total cores (1,1)
    - 2993, 2941, 2918 milliseconds
    - Average: 2950 ms
- 4 total cores (2,2)
    - 2938, 2932, 2976 milliseconds
    - Average: 2948 ms
- 4 total cores (3,1)
    - 2555, 2295, 2533 milliseconds
    - Average: 2461 ms
- 5 total cores (3,2)
    - 2732, 2757, 2812 milliseconds
    - Average: 2767 ms
- 6 total cores (3,3)
    - 4260, 4466, 4447 milliseconds
    - Average: 4391 ms
