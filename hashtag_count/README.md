# Apache_FLink
##Flink Version: 1.13.1

## installation: pip install apache-flink

##Trending Hashtags
* A very similar example to word count, but different source/sinks. The input data in this case is read off of disk, and the output is written as a csv. The file is generated dynamically at run time.

## step 1: create input file
* Creating a file containing thousands words which are randomly getting choose from a list of predefined hashtags.

## step 2: creating a source
* Getting the input's from the created file using connect() funtion which bassically assign the file as a source for the further operations.

## step 3: creating sink
* It is the output file where the output get's written in csv format

## step 4:map function
* Doing group by on the hashtags name and then the count funtion basically generate the total count.
