#Ava Chong
#CS 1656 Project2
#Due 2/17/20

import sqlite3 as lite
import csv
import re
import pandas
import string 
import sqlalchemy
from sqlalchemy import create_engine
con = lite.connect('cs1656.sqlite')

with con:
	cur = con.cursor() 

	########################################################################		
	### CREATE TABLES ######################################################
	########################################################################		
	# DO NOT MODIFY - START 
	cur.execute('DROP TABLE IF EXISTS Actors')
	
	cur.execute("CREATE TABLE Actors(aid INT, fname TEXT, lname TEXT, gender CHAR(6), PRIMARY KEY(aid))")

	cur.execute('DROP TABLE IF EXISTS Movies')
	cur.execute("CREATE TABLE Movies(mid INT, title TEXT, year INT, rank REAL, PRIMARY KEY(mid))")

	cur.execute('DROP TABLE IF EXISTS Directors')
	cur.execute("CREATE TABLE Directors(did INT, fname TEXT, lname TEXT, PRIMARY KEY(did))")

	cur.execute('DROP TABLE IF EXISTS Cast')
	cur.execute("CREATE TABLE Cast(aid INT, mid INT, role TEXT)")

	cur.execute('DROP TABLE IF EXISTS Movie_Director')
	cur.execute("CREATE TABLE Movie_Director(did INT, mid INT)")
	# DO NOT MODIFY - END

	########################################################################		
	### READ DATA FROM FILES ###############################################
	########################################################################		
	# actors.csv, cast.csv, directors.csv, movie_dir.csv, movies.csv
	# UPDATE THIS

	with open('actors.csv', 'r') as actorFile:
		actorsDr = csv.DictReader(actorFile) 
		#actors_db = [(i['aid'], i['fname'], i['lname'], i['gender']) for i in actorsDr]
		
	with open('cast.csv', 'r') as castFile:
		castDr = csv.DictReader(castFile)
		#cast_db = [(i['aid'], i['mid'], i['role']) for i in castDr]

	with open('directors.csv','r') as directorsFile:
		dirDr = csv.DictReader(directorsFile)
		#dir_db = [(i['did'], i['fname'], i['lname']) for i in dirDr]
		
	with open('movie_dir.csv', 'r') as movie_dirFile:
		mvdirDr = csv.DictReader(movie_dirFile)
		#mvdir_db = [(i['did'], i['mid']) for i in mvdirDr]
	
	with open('movies.csv', 'r') as moviesFile:
		mvDr = csv.DictReader(moviesFile)
		#mv_db = [(i['mid'], i['title'], i['year'], i['rank']) for i in mvDr]



	########################################################################		
	### INSERT DATA INTO DATABASE ##########################################
	########################################################################		
	# UPDATE THIS TO WORK WITH DATA READ IN FROM CSV FILES

	#cur.executemany("INSERT INTO Actors (aid, fname, lname, gender) VALUES (?, ?, ?, ?);", actors_db)
	#cur.executemany("INSERT INTO Cast (aid, mid, role) VALUES (?, ?, ?);", cast_db)
	#cur.executemany("INSERT INTO Directors (did, fname, lname) VALUES (?, ?, ?);", dir_db)
	#cur.executemany("INSERT INTO Movie_Director (did, mid) VALUES (?, ?);", mvdir_db)
	#cur.executemany("INSERT INTO Movies (mid, title, year, rank) VALUES (?, ?, ?, ?);", mv_db)

	#con.commit()
	#con.close()

	#engine = create_engine("sqlite:///cs1656wed.sqlite")
	actorcolnames=['aid', 'fname', 'lname', 'gender'] 
	df1 = pandas.read_csv('actors.csv', names=actorcolnames)
	df1.to_sql('Actors', con, if_exists='append', index=False)


	castcolnames =['aid', 'mid', 'role']
	df2 = pandas.read_csv('cast.csv', names=castcolnames, header=None)
	df2.to_sql('Cast', con, if_exists='append', index=False)

	directorscolnames =['did', 'fname', 'lname']
	df3 = pandas.read_csv('directors.csv', names=directorscolnames, header=None)
	df3.to_sql('Directors', con, if_exists='append', index=False)

	mvdircolnames =['did', 'mid']
	df4 = pandas.read_csv('movie_dir.csv', names=mvdircolnames, header=None)
	df4.to_sql('Movie_Director', con, if_exists='append', index=False)

	mvcolnames=['mid', 'title', 'year', 'rank'] 
	df5 = pandas.read_csv('movies.csv', names=mvcolnames, header=None)
	df5.to_sql('Movies', con, if_exists='append', index=False)


    	
	########################################################################		
	### QUERY SECTION ######################################################
	########################################################################		
	queries = {}

	# DO NOT MODIFY - START 	
	# DEBUG: all_movies ########################
	queries['all_movies'] = '''
SELECT * FROM Movies
'''	
	# DEBUG: all_actors ########################
	queries['all_actors'] = '''
SELECT * FROM Actors
'''	
	# DEBUG: all_cast ########################
	queries['all_cast'] = '''
SELECT * FROM Cast
'''	
	# DEBUG: all_directors ########################
	queries['all_directors'] = '''
SELECT * FROM Directors
'''	
	# DEBUG: all_movie_dir ########################
	queries['all_movie_dir'] = '''
SELECT * FROM Movie_Director
'''	
	# DO NOT MODIFY - END

	########################################################################		
	### INSERT YOUR QUERIES HERE ###########################################
	########################################################################		
	# NOTE: You are allowed to also include other queries here (e.g., 
	# for creating views), that will be executed in alphabetical order.
	# We will grade your program based on the output files q01.csv, 
	# q02.csv, ..., q12.csv

	# Q01 ########################	order by last and first name	
	queries['q01'] = '''
	SELECT a.fname, a.lname
	FROM Cast AS c
	INNER JOIN Actors AS a ON c.aid = a.aid
	WHERE c.aid in (SELECT c.aid
	                FROM Cast AS c
	                INNER JOIN Movies AS m ON m.mid = c.mid
	                WHERE m.year >= 1980 AND m.year <= 1990)
	  AND c.aid in (SELECT c.aid
	                FROM Cast AS c
	                INNER JOIN Movies AS m ON m.mid = c.mid
	                WHERE m.year >= 2000)
	GROUP BY c.aid
	ORDER BY a.lname ASC, a.fname ASC
'''	
	
	# Q02 ######################## sort by movie title aplphabetically		
	queries['q02'] = '''
	SELECT title
	FROM Movies 
	WHERE year IN (SELECT year 
				   FROM Movies
				   WHERE title = "Rogue One: A Star Wars Story")
	   AND rank > (SELECT rank
	   			   FROM Movies
				   WHERE title = "Rogue One: A Star Wars Story")
	ORDER BY title ASC
'''	

	# Q03 ########################	FIX THIS, how to check any star wars movie 	
	queries['q03'] = '''
	SELECT a.fname, a.lname, COUNT(DISTINCT m.mid) as numFilms
	FROM Actors AS a
	INNER JOIN Cast AS c ON a.aid = c.aid
	INNER JOIN Movies AS m ON c.mid = m.mid
	WHERE m.title LIKE '%Star Wars%'
	GROUP BY a.aid
	ORDER BY numFilms DESC, a.lname ASC, fname ASC

'''	

	# Q04 ########################	Sort alphabetically	
	queries['q04'] = '''
	SELECT a.fname, a.lname
	FROM Actors AS a 
	WHERE NOT a.aid IN (SELECT a2.aid 
						FROM Actors as a2
						INNER JOIN Cast AS c2 ON a2.aid = c2.aid
						INNER JOIN Movies AS m2 ON c2.mid = m2.mid
						WHERE m2.year > 1984)
	ORDER BY a.lname ASC, a.fname ASC
'''	

	# Q05 ########################		
	queries['q05'] = '''
	SELECT d.fname, d.lname, COUNT(DISTINCT md.mid) AS numFilms
	FROM Directors AS d 
	INNER JOIN Movie_Director AS md ON d.did = md.did
	GROUP BY d.did
	ORDER BY numFilms DESC
	LIMIT 10 
'''	

	# Q06 ########################		
	queries['q06'] = '''
	SELECT m.title, COUNT(DISTINCT c.aid) as numCast
	FROM Movies AS m
	INNER JOIN Cast AS c ON c.mid = m.mid 
	GROUP BY m.mid
	HAVING numCast >= (SELECT MIN(numCast2)
						FROM (SELECT COUNT(c2.aid) AS numCast2
								FROM Movies AS m2
								INNER JOIN Cast AS c2 on c2.mid = m2.mid
								GROUP BY m2.mid
								ORDER BY numCast2 DESC
								LIMIT 10))
	ORDER BY numCast DESC, m.title ASC
'''	

	# Q07 ######################## Sort alphabetically 
	queries['q07'] = '''
	SELECT m.title, IFNULL(WT.num_women_wt, 0) AS numWomen, IFNULL(MT.num_men_mt, 0) AS numMen
	FROM Movies AS m
	INNER JOIN Cast AS c ON c.mid = m.mid
	INNER JOIN Actors AS a ON c.aid = a.aid
	LEFT JOIN (SELECT m2.mid, COUNT(*) AS num_men_mt
				FROM Movies as m2
				INNER JOIN Cast AS c2 ON c2.mid = m2.mid
				INNER JOIN Actors AS a2 ON c2.aid = a2.aid
				WHERE a2.gender = "Male"
				GROUP BY m2.mid) MT ON MT.mid = m.mid
	LEFT JOIN (SELECT m3.mid, COUNT(*) AS num_women_wt
				FROM Movies AS m3
				INNER JOIN Cast AS c3 ON c3.mid = m3.mid 
				INNER JOIN Actors AS a3 ON c3.aid = a3.aid
				WHERE a3.gender = "Female"
				GROUP BY m3.mid) WT ON WT.mid = m.mid
	WHERE numWomen > numMen 
	GROUP BY m.mid
	ORDER BY m.title ASC
'''	

	# Q08 ########################	 	
	queries['q08'] = '''
	SELECT a.fname, a.lname, COUNT(DISTINCT md.did) AS numDirectors
	FROM Actors AS a 
	INNER JOIN Cast AS c ON a.aid = c.aid
	INNER JOIN Movie_Director AS md ON c.mid = md.mid 
	GROUP BY a.aid
	HAVING numDirectors > 6
		AND a.aid IN (SELECT a2.aid
						FROM Actors AS a2
						INNER JOIN Cast AS c2 on c2.aid = a2.aid
						INNER JOIN Movies AS m2 ON m2.mid = c2.mid
						GROUP BY a2.aid 
						HAVING COUNT(DISTINCT m2.mid) > 6)
	ORDER BY numDirectors DESC
'''	

	# Q09 ########################		
	queries['q09'] = '''
	SELECT a.fname, a.lname, COUNT(m.mid) AS num_movies
	FROM Actors AS a
	INNER JOIN Cast AS c ON a.aid = c.aid
	INNER JOIN Movies AS m ON c.mid = m.mid
	WHERE SUBSTR(a.fname, 1, 1) = 'D'
	    AND m.mid IN (SELECT m2.mid
	                  FROM Actors AS a2
	                  INNER JOIN Cast AS c2 ON a2.aid = c2.aid
	                  INNER JOIN Movies AS m2 ON c2.mid = m2.mid
	                  WHERE m.year = (SELECT MIN(m3.year)
	                                  FROM Actors AS a3
	                                  INNER JOIN Cast AS c3 ON a3.aid = c3.aid
	                                  INNER JOIN Movies AS m3 ON c3.mid = m3.mid
	                                  WHERE a3.aid = a.aid))
	GROUP BY a.aid
	ORDER BY num_movies DESC, a.lname ASC

'''	

	# Q10 ########################		
	queries['q10'] = '''
	SELECT a.lname, m.title
	FROM Actors AS a
	INNER JOIN Cast AS c ON a.aid = c.aid
	INNER JOIN Movies AS m ON c.mid = m.mid
	INNER JOIN Movie_Director AS md ON m.mid = md.mid
	INNER JOIN Directors AS d ON d.did = md.did
	WHERE a.lname = d.lname
	ORDER BY a.lname ASC
'''	

	# Q11 ########################		
	queries['q11'] = '''
	SELECT DISTINCT(a.fname), a.lname
	FROM Actors AS a
	INNER JOIN Cast AS c ON a.aid = c.aid
	INNER JOIN Movies AS m ON m.mid = c.mid
	WHERE m.mid IN (SELECT DISTINCT(m2.mid)
	                FROM Actors AS a2
	                INNER JOIN Cast AS c2 ON a2.aid = c2.aid
	                INNER JOIN Movies AS m2 ON m2.mid = c2.mid
	                WHERE c2.aid IN (SELECT DISTINCT(a3.aid)
	                                 FROM Actors AS a3
	                                 INNER JOIN Cast AS c3 ON a3.aid = c3.aid
	                                 INNER JOIN Movies AS m3 ON c3.mid = m3.mid
	                                 WHERE m3.mid IN (SELECT c4.mid
	                                                  FROM Actors AS a4
	                                                  INNER JOIN Cast AS c4 ON a4.aid = c4.aid
	                                                  INNER JOIN Movies AS m4 ON c4.mid = m4.mid
	                                                  WHERE a4.fname = "Kevin" AND a4.lname = "Bacon")
	                                    AND NOT (a3.fname = "Kevin" AND a3.lname = "Bacon"))
	                    AND NOT (a2.fname = "Kevin" AND a2.lname = "Bacon")
	                    AND NOT m2.mid IN (SELECT c5.mid
	                                       FROM Actors AS a5
	                                       INNER JOIN Cast AS c5 ON a5.aid = c5.aid
	                                       INNER JOIN Movies AS m5 ON c5.mid = m5.mid
	                                       WHERE a5.fname = "Kevin" AND a5.lname = "Bacon"))
	    AND NOT (a.fname = "Kevin" AND a.lname = "Bacon")
	    AND NOT m.mid IN (SELECT c6.mid
	                      FROM Actors AS a6
	                      INNER JOIN Cast AS c6 ON a6.aid = c6.aid
	                      INNER JOIN Movies AS m6 ON c6.mid = m6.mid
	                      WHERE a6.fname = "Kevin" AND a6.lname = "Bacon")
	    AND NOT a.aid IN (SELECT DISTINCT(a7.aid)
	                      FROM Actors AS a7
	                      INNER JOIN Cast AS c7 ON a7.aid = c7.aid
	                      INNER JOIN Movies AS m7 ON c7.mid = m7.mid
	                      WHERE m7.mid IN (SELECT c8.mid
	                                       FROM Actors AS a8
	                                       INNER JOIN Cast AS c8 ON a8.aid = c8.aid
	                                       INNER JOIN Movies AS m8 ON c8.mid = m8.mid
	                                       WHERE a8.fname = "Kevin" AND a8.lname = "Bacon")
	                          AND NOT (a7.fname = "Kevin" AND a7.lname = "Bacon"))
	ORDER BY a.lname ASC, a.fname ASC
'''	

	# Q12 ########################		
	queries['q12'] = '''
	SELECT a.fname, a.lname, COUNT(m.mid), AVG(m.rank) AS popularity
	FROM Actors AS a
	INNER JOIN Cast AS c ON a.aid = c.aid
	INNER JOIN Movies AS m ON c.mid = m.mid
	GROUP BY a.aid 
	ORDER BY popularity DESC, a.lname ASC, a.fname ASC
	LIMIT 20 
'''	


	########################################################################		
	### SAVE RESULTS TO FILES ##############################################
	########################################################################		
	# DO NOT MODIFY - START 	
	for (qkey, qstring) in sorted(queries.items()):
		try:
			cur.execute(qstring)
			all_rows = cur.fetchall()
			
			print ("=========== ",qkey," QUERY ======================")
			print (qstring)
			print ("----------- ",qkey," RESULTS --------------------")
			for row in all_rows:
				print (row)
			print (" ")

			save_to_file = (re.search(r'q0\d', qkey) or re.search(r'q1[012]', qkey))
			if (save_to_file):
				with open(qkey+'.csv', 'w') as f:
					writer = csv.writer(f)
					writer.writerows(all_rows)
					f.close()
				print ("----------- ",qkey+".csv"," *SAVED* ----------------\n")
		
		except lite.Error as e:
			print ("An error occurred:", e.args[0])
	# DO NOT MODIFY - END
	
