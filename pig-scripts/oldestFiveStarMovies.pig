-- Author: Matheus dos Santos Lima

-- loading the u.data file from the hadoop cluster
-- ratings = LOAD 'hdfs://localhost:9000/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);
ratings = LOAD 'u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

-- loading the u.data item from the hadoop cluster
--metadata = LOAD 'hdfs://localhost:9000/u.item' USING PigStorage('|')
metadata = LOAD 'u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdblink:chararray);

-- looking each line of the u.item file and organizing by movieID, movieTitle, releaseTime
nameLookup = FOREACH metadata GENERATE movieID, movieTitle,
	ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating;

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

fiveStarsWithData = JOIN fiveStarMovies BY movieID, nameLookup BY movieID;

oldestFiveStarMovies = ORDER fiveStarsWithData BY nameLookup::releaseTime;

STORE oldestFiveStarMovies INTO 'myoutput';
--DUMP oldestFiveStarMovies;