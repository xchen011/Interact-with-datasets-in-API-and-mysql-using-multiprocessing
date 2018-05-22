

The script is for interacting with two datasets in mysql table and API, respectively, and finding intersection between thoes two datasets. The data in API was read one by one page. Multiprocessing was applied for speeding up the processes of reading data from API and querying intersections between two datasets.  The total processing time of 81.5 sec in the results.txt is for running this script with 4 cpu cores. It can be faster with more cpu cores. 

### Task of this project is to answer the following questions:
1.	How many users in the Liveworks dataset are also in Cogo's dataset? How many are just in Cogo's? How many are just in Liveworks'?
2.	For the users within that intersection, what percent have different job titles between the two sets?
In addition to answering those questions, your script should output data for the users within the intersection. Please output the following three column CSV:
emd5, JSON list of "job" and "company" key/value pairs for Cogo, JSON list of "job" and "company" key/value pairs for Liveworks

### For the output: 
•	Line 1: Total runtime of your script up to the time of output
•	Line 2: The number of users within both datasets
•	Line 3: The number of users unique to Cogo's dataset
•	Line 4: The number of users unique to Livework's dataset
•	Line 5: For users within both datasets, the % of users with different job titles between them
•	Lines 6 - 15: A 10 line sample of your CSV output
•	Remaining lines: The CREATE TABLE statement for the table that could house the CSV output.
