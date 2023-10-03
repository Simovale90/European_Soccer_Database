/*First step: I created the 'European Soccer Database' on Google BigQuery
Second step: I loaded the 4 tables into it in csv (match, leagues, team and player)

Below is the list of queries used for data analysis
Remember to REPLACE `European_Soccer_Database` with your Dataset ID 
BE CAREFUL, when using Google BigQuery you have to put your FROM between these quotes `` */

/*How many matches in total are we analyzing?*/

SELECT COUNT(id) Tot_match FROM `European_Soccer_Database.match`

/*How many leagues in total are we analyzing?*/
  
SELECT DISTINCT(name) FROM `European_Soccer_Database.leagues`

/*Calculate the difference between the most recent date and the most distant one and the two dates at the extremes*/

SELECT DATE_DIFF(max(date), min(date), day) AS Total_Range,
  FORMAT_TIMESTAMP('%m-%d-%Y', TIMESTAMP(max(date))) AS Most_recent,
  FORMAT_TIMESTAMP('%m-%d-%Y', TIMESTAMP(min(date))) AS Less_recent
FROM `European_Soccer_Database.match`;

/*Count how many seasons are there in total*/

SELECT DISTINCT season Season
FROM  `European_Soccer_Database.match`

/*How many matches for each season?*/

SELECT COUNT(id) TotMatch, season Season
FROM `European_Soccer_Database.match`
GROUP BY Season
ORDER BY Season

/*Count how many matches have been played for each League*/

SELECT DISTINCT (m.season) Season, l.name LeagueName, COUNT(match_api_id) TotMatch
FROM `European_Soccer_Database.match` m
LEFT JOIN `European_Soccer_Database.leagues` l
ON m.league_id = l.id
GROUP BY m.season, l.name  
ORDER BY TotMatch DESC

/*Do we notice anything out of the ordinary?*/

SELECT DISTINCT (m.season) Season, l.name LeagueName, COUNT(match_api_id) TotMatch
FROM `European_Soccer_Database.match` m
LEFT JOIN `European_Soccer_Database.leagues` l
ON m.league_id = l.id
GROUP BY m.season, l.name  
ORDER BY TotMatch ASC
LIMIT 1

/*How many matches were there for each month of the year? I used this query to download a dataset match_per_month*/

SELECT COUNT(id) Tot_match, EXTRACT(MONTH FROM date) Month FROM `European_Soccer_Database.match`
GROUP BY Month

/*Produce a table that shows for each Season and League Name, the following statistics about the home goals scored: min, average, mid-range, max and sum*/

SELECT m.season Season, l.name LeagueName, 
MIN(m.home_team_goal) minHTG, 
ROUND(AVG(m.home_team_goal),2) avgHTG,
CAST(((MIN(m.home_team_goal) + MAX(m.home_team_goal))/2) AS INT64) midrangeHTG,  
MAX(m.home_team_goal) maxHTG,
SUM(m.home_team_goal) sumHTG
FROM `European_Soccer_Database.match` m
LEFT JOIN `European_Soccer_Database.leagues` l
ON m.league_id = l.id
GROUP BY m.season, l.name 
ORDER BY sumHTG desc

/*Create a new 'PlayerBMI' table in which we insert:
 - the weight in kg (kg_weight);
 - the height in meters (m_height);
 - the player's body mass index (BMI).
We filter the table to only show players with an optimal BMI (18.5 to 24.9)*/

CREATE TABLE `European_Soccer_Database.PlayerBMI` AS SELECT *,
ROUND ((weight / 2.205),2) AS kg_weight,
ROUND ((height / 100),2) AS m_heigth,
ROUND ((weight / 2.205) / power(height / 100,2),2) AS BMI,
FROM `European_Soccer_Database.player`
WHERE (weight/2.205)/power(height/100, 2) between 18.5 and 24.9

/*How many players do not have an optimal BMI? */

SELECT
 (SELECT count(id)
  FROM `European_Soccer_Database.player`) - 
  (SELECT count(id)
   FROM `European_Soccer_Database.PlayerBMI`) as PlayerNoBMI

/*Which Team has scored the highest total number of goals during the most recent available season?*/

SELECT h.team_long_name, h.SumOfGoalHome, a.SumOfGoalAway, 
h.SumOfGoalHome + a.SumOfGoalAway AS TotalGoal
FROM ( SELECT t.team_long_name, SUM(m.home_team_goal) AS SumOfGoalHome
FROM `European_Soccer_Database.match` m 
INNER JOIN `European_Soccer_Database.team` t 
ON m.home_team_api_id = t.team_api_id 
WHERE m.season = (SELECT MAX(season) FROM `European_Soccer_Database.match`)
GROUP BY t.team_long_name ORDER BY SumOfGoalHome) h 
INNER JOIN
(SELECT t.team_long_name, SUM(m.away_team_goal) AS SumOfGoalAway
FROM `European_Soccer_Database.match` m 
INNER JOIN `European_Soccer_Database.team` t 
ON m.away_team_api_id = t.team_api_id 
WHERE m.season = (SELECT MAX(season) FROM `European_Soccer_Database.match`)
GROUP BY t.team_long_name ORDER BY SumOfGoalAway) a 
ON h.team_long_name = a.team_long_name
ORDER BY TotalGoal DESC
LIMIT 1 

/*For each season, which team ranks first in terms of total goals scored?*/

SELECT * FROM
(SELECT h.season, h.team_long_name, h.SumOfGoalHome, a.SumOfGoalAway, h.SumOfGoalHome + a.SumOfGoalAway AS TotalGoal,
RANK() OVER (PARTITION BY a.season ORDER BY h.SumOfGoalHome + a.SumOfGoalAway DESC) AS rank_season FROM
(SELECT m.season, t.team_long_name, SUM(m.home_team_goal) AS SumOfGoalHome
FROM `European_Soccer_Database.match` m INNER JOIN
`European_Soccer_Database.team` t ON m.home_team_api_id = t.team_api_id
GROUP BY m.season, t.team_long_name ORDER BY SumOfGoalHome) h
INNER JOIN
(SELECT m.season,t.team_long_name, sum(m.away_team_goal) AS SumOfGoalAway
FROM `European_Soccer_Database.match` m INNER JOIN
`European_Soccer_Database.team` t ON m.away_team_api_id = t.team_api_id
GROUP BY m.season, t.team_long_name ORDER BY SumOfGoalAway) a
ON h.team_long_name = a.team_long_name AND h.season=a.season)
WHERE rank_season = 1
ORDER BY season DESC

/*Create a new table 'TopScorer' containing the top 10 teams in terms of total goals scored*/

CREATE TABLE `European_Soccer_Database.TopScorer` AS
(SELECT h.team_api_id ,h.team_long_name, h.SumOfGoalHome, a.SumOfGoalAway, 
h.SumOfGoalHome + a.SumOfGoalAway AS TotalGoal FROM
(SELECT t.team_api_id ,t.team_long_name, SUM(m.home_team_goal) AS SumOfGoalHome
FROM `European_Soccer_Database.match` m INNER JOIN
`European_Soccer_Database.team` t ON m.home_team_api_id = t.team_api_id 
where m.season = (select MAX(season) FROM `European_Soccer_Database.match`)
GROUP BY t.team_api_id, t.team_long_name ORDER BY SumOfGoalHome) h INNER JOIN
(SELECT t.team_long_name, SUM(m.away_team_goal) AS SumOfGoalAway
FROM `European_Soccer_Database.match` m INNER JOIN 
`European_Soccer_Database.team` t ON m.away_team_api_id = t.team_api_id 
WHERE m.season = "2015/2016"
GROUP BY t.team_long_name ORDER BY SumOfGoalAway) a ON h.team_long_name = a.team_long_name
ORDER BY TotalGoal DESC
LIMIT 10)


