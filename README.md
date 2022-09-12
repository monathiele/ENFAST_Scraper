# Scraper_ENFAST
Using scrapy on ENFAST Europe’s Most Wanted List

ENFAST publishes a list of most wanted criminals here: https://eumostwanted.eu/

### Tasks
1) use some web extraction technique to extract information from each individual on the list.
2) the script should contain some error logging, making it possible to trace errors by reading the
log file
3) the result should be a well formed table, one row per individual, containing various data
points, including name, first name, last name, date of birth, gender, nationality, crime, and state
of case.

![image](https://user-images.githubusercontent.com/97023507/189533815-f0195afe-2b1f-4982-ace8-8be8edc30577.png)

### Solution

Task was solved by using python scrapy package: 

https://scrapy.org/

$ pip install scrapy

file 1 containing scraper and file 2 data wrangling
