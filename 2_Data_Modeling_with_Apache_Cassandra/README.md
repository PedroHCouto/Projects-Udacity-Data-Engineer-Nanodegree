[![LinkedIn][linkedin-shield]][linkedin-url]

# Data Engineer Nanodegree - Project 2: Data Modeling with Apache Cassandra

![cassandra logo](./images/cassandra_logo.png)

<details open="open">
  <summary><h2 style="display: inline-block">Table of Contents</h2></summary>
  <ol>
    <li><a href="#goal">Goal</a></li>  
    <li><a href="#about-the-project">About the project</a></li>
    <li><a href="#data">Data</a></li>
    <li><a href="#etl">ETL</a></li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#built-with">Built With</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>

## Goal
The main goal of this project is to practice the concepts of a **NoSQL** data modeling with Apache Cassandra like Denormalization, Partion Key, Clustering Columns and Primary Key as well to develop an **ETL** with **CQL** by implementing queries and tables from scratch.   

---
## About the project 
The project takes place as a simulation of a Non-Relational database for analytical purpose of a fictional music streaming startup called Sparkify. The startup wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 

For this type of services, there are to constrains that must assured: high availability and fault-tolerance so the system will be always up and running no matter the problem that may occur on the nodes. A great way to guarantee these properties is using Apache Cassandra, that also brings advantages like optimization for writes.

The idea of the project is to create an ETL that can extract the data from different files, transform it into the desired form and load the data elements into different tables. These tables should be created to answer 3 questions:
1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession = 4;
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182;
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'.

---
## Data
For this project one big csv file is created by concatenating all infos present in event  csv file generated daily. It produces the `event_datafile_new.csv` file that will be used in the ETL part.
The columns present in this file, as well as the first row data, are showed below:
```
{
    "artist": "Barry Tuckwell/Academy of St Martin-in-the-Fie...", 
    "firstName": "Mohammad", 
    "gender": "M", 
    "itemInSession": 0, 
    "lastName": "Rodriguez", 
    "length": 277.15873, 
    "level": "paid", 
    "location": "Sacramento--Roseville--Arden-Arcade, CA", 
    "sessionId": 961, 
    "song": "Horn Concerto No. 4 in E flat K495: II. Romanc...",
    "userId": 88
}
```   
---
## ETL

In order to create a database that allow the right queries to answer the presented 3 questions, the ETL process was done in 3 parts - one part for each table. To help all the process of creating an ETL, the notebook [Exploration.ipynb](https://github.com/PedroHCouto/Projects-Udacity-Data-Engineering-Nanodegree/blob/master/2_Data_Modeling_with_Apache_Cassandra/Exploration.ipynb) was developed. 

In the file [Project_Apache_Cassandra.ipynb](https://github.com/PedroHCouto/Projects-Udacity-Data-Engineering-Nanodegree/blob/master/2_Data_Modeling_with_Apache_Cassandra/Project_Apache_Cassandra.ipynb) stands for the development of the ETL workflow to create the tables, to insert the data and to perform the desired queries. 

The implementation for each part are:

### Part 1 - Table usage_library

Using a combination of 'sessionId and itemInSession' it is produced a combination of 6806 unique combinations out of a total of 6820 rows. These two columns should be enough in order to perform the query for the first question properly.   
The implementation is as following:
- Create the table
```
query_table_1 = ("""CREATE TABLE IF NOT EXISTS usage_library
    (session_id int,
    item_in_session int,
    artist_name text,
    song_name text, 
    song_length float,
    PRIMARY KEY (session_id, item_in_session))""")

try:
    session.execute(query_table_1)
except Exception as e:
    print(e)
```
- Insert data
```
file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = """INSERT INTO usage_library
        (session_id, item_in_session, artist_name, song_name, song_length)
        VALUES (%s, %s, %s, %s, %s)
        """
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))
```
- Query
```
query = """SELECT artist_name, song_name, song_length
    FROM usage_library 
    WHERE session_id = 338 
    AND item_in_session = 4"""
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist_name, "|", row.song_name, "|", row.song_length)
```

Just applying these small steps, the code will extract all elements of data to create the table.    

### Part 2 - Table artist_and_song_library

In order to adress this question in a form of a table we must guarantee a unique primary key and it is done using a combination of userId and sessionId columns at it was requested that they must be used for filtering.

Another great idea would be to use these both columns as a composite partition key, as it would improve overall performance because the userId data will be spread among more than just one node and it'll be much faster to look for a specific sessionId!

To address the question entirely the columns artist name, song name and user's first and last name should be added as data columns and this will produce a great query to answer the question.   
The implementation is as following:
- Create the table
```
query_table_2 = ("""CREATE TABLE IF NOT EXISTS artist_and_song_library
    (user_id int,
    session_id int,
    item_in_session int,
    artist_name text,
    song_name text, 
    user_first_name text,
    user_last_name text,
    PRIMARY KEY ((user_id, session_id), item_in_session))""")

try:
    session.execute(query_table_2)
except Exception as e:
    print(e)
```
- Insert data
```
file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = """INSERT INTO artist_and_song_library
        (user_id, session_id, item_in_session, artist_name, song_name, user_first_name, user_last_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))
```
- Query
```
query = """SELECT artist_name, song_name, user_first_name, user_last_name 
    FROM artist_and_song_library
    WHERE user_id = 10 
    AND session_id = 182"""
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist_name, "|",row.song_name)
```

Just applying these small steps, the code will extract all elements of data to create the table.  

### Part 3 - Table user_library

As shown in Exploration notebook, there are 5190 unique songs and and 96 unique userId. These numbers show that just these two columns are enough to create a unique primary key and to allow the using of WHERE clause to get the desired results for the selected song.
For data columns, as asked, the features firstName and lastName must be included.   
The implementation is as following:
- Create the table
```
query_table_3 = ("""CREATE TABLE IF NOT EXISTS user_library
    (song_name text,
    user_id int,
    user_first_name text,
    user_last_name text, 
    PRIMARY KEY (song_name, user_id))""")

try:
    session.execute(query_table_3)
except Exception as e:
    print(e)
```
- Insert data
```
file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
        query = """INSERT INTO user_library
        (song_name, user_id, user_first_name, user_last_name)
        VALUES (%s, %s, %s, %s)
        """
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))
```
- Query
```
query = """SELECT user_first_name, user_last_name 
    FROM user_library
    WHERE song_name = 'All Hands Against His Own'"""
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.user_first_name, "|",row.user_last_name)
```

Just applying these small steps, the code will extract all elements of data to create the table.  

---
## Usage

Just follow the [Project_Apache_Cassandra.ipynb](https://github.com/PedroHCouto/Projects-Udacity-Data-Engineering-Nanodegree/blob/master/2_Data_Modeling_with_Apache_Cassandra/Project_Apache_Cassandra.ipynb) and in case of any doubt, the [Exploration.ipynb](https://github.com/PedroHCouto/Projects-Udacity-Data-Engineering-Nanodegree/blob/master/2_Data_Modeling_with_Apache_Cassandra/Exploration.ipynb) is available with all exploration part.

## Built with
* [Cassandra](https://www.postgresql.org/)
* [pandas](https://pandas.pydata.org/)
* [json](https://docs.python.org/3/library/json.html)
* [os](https://docs.python.org/3/library/os.html)
* [Jupyter Notebook](https://jupyter.org/try) 


## Contact

E-mail: pedrocouto39@gmail.com     
LinkedIn: https://www.linkedin.com/in/pdr-couto    
Kaggle: https://www.kaggle.com/pedrocouto39   
XING: https://www.xing.com/profile/Pedro_Couto8/cv     

Project Link: [https://github.com/PedroHCouto/Data-Eng-Nanodegree-Data-Modeling-with-Apache-Cassandra](https://github.com/PedroHCouto/Projects-Udacity-Data-Engineering-Nanodegree/tree/master/2_Data_Modeling_with_Apache_Cassandra)


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=flat-square&logo=linkedin&colorB=555
[linkedin-url]: https://www.linkedin.com/in/pdr-couto/
[product-screenshot]: images/screenshot.png 

