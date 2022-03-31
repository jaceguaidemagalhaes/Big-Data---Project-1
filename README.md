# Big-Data - Project-1

## Project Description

Project 1 is a Scala console appliction that retrieves data using Hive and MapReduce. It's a data analyser that use the dataset "tobycrabtree/nfl-scores-and-betting-datatobycrabtree" from Kaggle.com using Kaggle API. 

Data is sanitized to correct some errors and structures in a database with the metadata in a local file and the tables in the HDFS structure.

The Analyses run over NFL games from 1966 to 2021 and provided queries the user can interact defining specific queries' critereas.
The available queries are:

1 – How playing at Home or Away relates to games’ winners during regular season?

2 – How playing at Home or Away relates to games’ winners during playoffs?

3 – How temperature affects games winners regarding playing in home or away?

4 – How wind chill relates to games’ final score?

5 – What is the relation between the favorites’ spread and the games' winners?

6 – What is the relation between over-under line with final scores?

![image](https://github.com/jaceguaidemagalhaes/Big-Data---Project-1/blob/main/images/Project-Architecture.png)

## Tecnologies Used

- Scala 2.11.12
- Apache Spark 2.4.8
- MySql 8.0.28
- Hadoop 3.3.2
- Python 3.9
- Hive 3.1.2-3
- OpenJDK 8
- Kaggle CLI
- SHA-256

## Features

- User encrypted password using SHA-256
- User management 
- Activities restricteds according to user level (Admin, Basin)
- Queries results in .json files
- User iteraction theough APP defining query parameters
- API data managed using APP
- Data Sanitization and Database creation through APP 

### To-do list:

-Include more queries for new trends

## Getting Started
