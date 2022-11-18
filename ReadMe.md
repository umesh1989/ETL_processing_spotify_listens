
This file is to explain the flow of the program.

This is part of an assignment I did so all the explainations are in regards with the assignment. I have not added the entire assignment for obvious reasons.

**static parameters**

All the static parameters(ex: db credentials, queries) are derived from static_data.ini file.
In the prod environment these can be derived from a key-vault.

**Input files**

All the input data is read from input directory. Input path is being read from static_data.ini file.
Dataset can be taken from https://listenbrainz.org/.

**Output files**

All the output file and graphs are stored in this file. Currently, output path is hardcoded in the code.

**Code:**

All the require code is present in src directory.
Driver script is the parent script which will trigger the processing of the file and create the data tables entries.

Mysql utility is a reusable file, it can be extended further as well.

load data script contains the code to read the file and create data tuples to be stored.

Solving queries file contains the code to process the question asked in the assignment.

**Note:** Except solving_queries.py all other python files are properly documented, contains logging mechanism and exception handling.

**Logs:**
There are a couple of logs present in the logs directory, to demonstrate how success and failure logs will look like.

**How to execute the code**
1. create a database in the mysql with name scalable(or create any db and replace the name in static_data.ini).
2. update the db credentials in the static_data.ini
3. Take create table queries from solution_description.md and create tables.(I could have made setup data store in the driver_file, but I realized it while preparing this file) 
4. put the project in a directory
5. run driver_script.py
6. run solving_queries.py for testing the solution given for the questions asked in assignment.

_Explanation of the question asked in the assignment are given in Solution_description.md._

