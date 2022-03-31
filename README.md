# Big-Data - Project-1

## Project Description

Project 1 is a Scala console application that manipulates data using Hive and MapReduce. It's a data analyser that use the dataset "tobycrabtree/nfl-scores-and-betting-datatobycrabtree" from Kaggle.com using Kaggle API. 

Data is sanitized to correct some errors and structures, inserted in a database with the metadata in a local file, and the tables in the HDFS structure.

The analyses are over NFL games data from 1966 to 2021, and iteractive queries to the user.
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
- MySQL 8.0.28
- Hadoop 3.3.2
- Python 3.9
- Hive 3.1.2-3
- OpenJDK 8
- Kaggle CLI
- SHA-256
- IntelliJ IDEA 2021.3.2 (Community Edition)
- MySQL Workbench 8.0.28
- DBeaver 22 (Edition Community)

## Features

- User encrypted password using SHA-256
- User management 
- Activities restricteds according to user level (Admin, Basin)
- Queries results in .json files
- User iteraction theough APP defining query parameters
- API data managed using APP
- Data Sanitization and Database creation through APP 

### To-do list:

- Create validation to protect against user data entries incompatible with the queries
- Include more queries for new trends

## Getting Started

- Cloning the repository

git clone https://github.com/jaceguaidemagalhaes/Big-Data---Project-1 main

- Install the required softwares within technologies used list (for kagle API insatalattion follow instruction in https://www.kaggle.com/docs/api)
- Use the following scripts to create user table and database (this script will create a first user called jaceguai with password 123456)

///scripts database mysql///////

use analysethat;
create table user(user varchar(50), password varchar(1000, admin boolean), primary key (user));
insert into user(user, password)
// hash for password "123456"
values("jaceguai","1411501582391102022111941545898146128230134207012639390134175243202180201214658220108146");
select * from user;
update user set admin = 1 where user = "jaceguai";  

- Configure HDFS access through localhost:9000 port(or change this value in the main object AnalyseThat.scala for the desired value)
- Create an user in MySQL who can access AnalyseThat database and its tables. Update username and password for this user at class ExecuteQuery.scala
- Open Intellij and run object AnalyseThat.scala

## Usage

Follow the screen instructions to use the system. At this stage, user data entry is not yet validated in all cases. That way, it's important to enter proper data to avoid unexpected behavior. In any case, if the program stops because a wrong entry, just restart the app and it will return to work properly.

Bellow are listed some APP's screenshots.

![Image](https://github.com/jaceguaidemagalhaes/Big-Data---Project-1/blob/main/images/login.png)

![Image](https://github.com/jaceguaidemagalhaes/Big-Data---Project-1/blob/main/images/mainscreen.png)

![Image](https://github.com/jaceguaidemagalhaes/Big-Data---Project-1/blob/main/images/listavailableresults.png)

![Image](https://github.com/jaceguaidemagalhaes/Big-Data---Project-1/blob/main/images/QueryForaSpecificTeam.png)

![Image](https://github.com/jaceguaidemagalhaes/Big-Data---Project-1/blob/main/images/administrativetasks.png)

![Image](https://github.com/jaceguaidemagalhaes/Big-Data---Project-1/blob/main/images/DownloadDatafromKaggle.png)





