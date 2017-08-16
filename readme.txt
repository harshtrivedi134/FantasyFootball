Files included, in chronological order:

DATA RETRIEVAL

Play-by-Play data
nfl_pbp_game.py — retrieves data from NFL API
pbp_data_download.png — screenshot of retrieval of API data

Weather data
scrapeWeatherData.R - retrieves weather data from internet

Injury data
nfl_injury_extact.py - retrieves injury data from internet

FanDuel data
salarylist.csv - downloaded from FanDuel.com


DATA PROFILING

Play-by-Play data
pbp_profile_map.py - map class for profiling, uses data from API
pbp_profile_reduce.py - reduce class for profiling
pbp_profile_log.txt - logs from running map and reduce on API data

Weather data
ProfileDriver.java - MapReduce driver, uses data scraped by ‘scrapeWeatherData.r’
ProfileMapper.java - Mapper class
ProfileReducer.java - Reducer class
Data_Profile_Screenshot_1.png and Data_Profile_Screenshot_2.png - screenshots of the above

Injury data
NFL_Injury.java - MapReduce driver, uses data scraped by ‘nfl_injury_extact.py’
NFL_Injury_Mapper - Mapper class
NFL_Injury_Reducer - Reducer class
Profiling1.png, Profiling3.png, Profiling5.png - screenshots of the above

FanDuel data
ProfileData.java - MapReduce driver, uses ‘salarylist.csv’ as input
ProfileMapper.java - Mapper class
ProfileReducer.java - Reducer class
output.txt - the output of the profiling code above


DATA CLEANING AND MERGING

Play-by-Play data
impala_clean_merge.txt - cleans play by play and merges with weather, injury data. Takes as input the API data and cleaned data from weather and injury datasets.
pbp_clean_merge.png - screenshot of the cleaning and merging process

Weather data
NFLDriver.java - MapReduce driver, uses output of ’scrapeWeather.r’ as input
NFLMapper.java - Mapper class
NFLReducer.java - Reducer class
Data_Cleaning_Screenshot_1.png and Data_Cleaning_Screenshot_2.png - screenshots of the above

Injury data
Cleaning_Data.txt - Hive code for data cleaning

FanDuel data
newDriver.java - MapReduce driver, uses ‘salarylist.csv’ as input
newMapper.java - Mapper class
newReducer.java - Reducer class
cleaning_screenshot.png - screenshot of the above


ANALYTIC

Point prediction
rdd_regression4.1.py - takes as input the output of ‘impala_clean_merge.py’ and returns predicted point values for each player
score_prediction.png - screenshot of ‘rdd_regression4.1.py’ running
scores.txt — output from ‘rdd_regression4.1.py’
readPreds.java - takes ‘scores.txt’ as input, along with salary data and the list of eligible for a particular contest and returns input for the team selection model as well as ‘scores.txt’ reformatted to a .csv
Position.java and Player.java are Object classes upon which ‘readPreds.java’ relies
playerlist.csv - output from ‘readPreds.java’, input to team selection model
goodnessinput.csv - output from ‘readPreds.java’, used offline for goodness-of-fit tests in R

Team selection
Team_Selection.py - takes ‘playlist.csv’ as input and prints the chosen team
Team_Selection_Model.jpg - a screenshot of the above running and the output it produced 

Goodness testing
goodnesstesting.R - takes in our predictions and the actual scores from a given week and displays various statistics about the goodness of fit
goodnessinput.csv - the output of the prediction model reformatted to a .csv, input to ‘goodness testing.R’
week14.csv - the actual scores for players last week
