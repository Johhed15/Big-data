# This repository contains the labs from a Big Data course in my Master program



lab0: SQL

lab1: Pyspark

lab2: Spark-sql

lab3: Machine learning with pyspark


```Sql
/*
Question 8: Which items (note items, not parts) have been delivered by a supplier called Fisher-Price? Formulate without a subquery.
*/

select jbitem.name from jbitem, jbsupplier where jbitem.supplier=jbsupplier.id and jbsupplier.name='Fisher-Price';

/*
    +-----------------+
	| name            |
	+-----------------+
	| Maze            |
	| The 'Feel' Book |
	| Squeeze Ball    |
	+-----------------+

	Selects the item name and shows the names for where supplier from item and id from supplier matches has correct supplier name.
*/


/*
Question 9: Show all cities that have suppliers located in them. Formulate this query using a subquery in the where-clause.
*/

select name from jbcity where id IN (SELECT city from jbsupplier);

/*
    +----------------+
	| name           |
	+----------------+
	| Amherst        |
	| Boston         |
	| New York       |
	| White Plains   |
	| Hickville      |
	| Atlanta        |
	| Madison        |
	| Paxton         |
	| Dallas         |
	| Denver         |
	| Salt Lake City |
	| Los Angeles    |
	| San Diego      |
	| San Francisco  |
	| Seattle        |
	+----------------+

	Select name from city database and shows cities where city id exists as city name in supplier
*/


/*
Question 10: What is the name and color of the parts that are heavier than a card reader? 
             Formulate this query using a subquery in the where-clause. (The SQL query must not contain the weight as a constant.)
*/

select name, color from jbparts where weight > (Select weight from jbparts where name = 'card reader');

/*
    +--------------+--------+
	| name         | color  |
	+--------------+--------+
	| disk drive   | black  |
	| tape drive   | black  |
	| line printer | yellow |
	| card punch   | gray   |
	+--------------+--------+
	
	Select names and color from parts db and conditionals weight on where weight is bigger than what it is for the card reader column
*/


```

```Python

#!/usr/bin/env python3

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 2")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")

lines = temperature_file.map(lambda line: line.split(";"))
lines_pre = precipitation_file.map(lambda line: line.split(";"))

# (station) = (temperature)
temperature = lines.map(lambda x: ((x[0]),(float(x[3]))))
precipitation = lines_pre.map(lambda x:((x[0]+x[1]),(float(x[3]))))

# filter out max temp for each station
temperature = temperature.reduceByKey(lambda a,b: a if a >= b else b)

# Sum of rain for a station / day
precipitation = precipitation.reduceByKey(lambda x,y: x+y)

# mapping precipitation to get only station as key
precipitation = precipitation.map(lambda x: (x[0][0:5],x[1]))

# filtering out max for each station
precipitation = precipitation.reduceByKey(lambda a,b: a if a >= b else b)


#filter out temps that arent between 25-30.
temperature = temperature.filter(lambda x: x[1]>=25 and x[1]<=30)

precipitation = precipitation.filter(lambda x: x[1]>=100 and x[1]<=200)


# join the datasets on station
joined = precipitation.join(temperature)


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
joined.saveAsTextFile("BDA/output/ex3")


```

