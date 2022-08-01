--We first want to look at response times, for our purposes, we can just use the mean times data.
--The response time data must be transformed into a usable format.
--This creates an edited table with the response times CAST into the string datatype.
CREATE TABLE `big-dynamo-354814.Response_time.string`
AS(SELECT 
  Month,
  Region,
  CAST(C1_Mean__min_sec_ AS STRING) AS C1_Mean,
  CAST(C1T_Mean__min_sec_ AS STRING) AS C1T_Mean,
  CAST(C2_Mean__min_sec_ AS STRING) AS C2_Mean,
  CAST(C3_Mean__min_sec_ AS STRING) AS C3_Mean,
  CAST(C4_Mean__min_sec_ AS STRING) AS C4_Mean,


FROM `big-dynamo-354814.Response_time.Cleaned_data`)


--Next, we want to change the format to a more usable format.
--This creates a table with the response time data in hh,mm,ss columns from the string table.
CREATE TABLE `big-dynamo-354814.Response_time.int_AVERAGE`
AS(SELECT
  Region,
  Month,
  --This splits the time into the hour column.
  LEFT(C1_Mean,2) AS C1_MeanHH,
  SUBSTRING(C1_Mean, 4,2) AS C1_MeanMM,
  SUBSTRING(C1_Mean, 7,2) AS C1_MeanSS,

  LEFT(C1T_Mean,2) AS C1T_MeanHH,
  --This splits the time into the minute column.
  SUBSTRING(C1T_Mean, 4,2) AS C1T_MeanMM,
  SUBSTRING(C1T_Mean, 7,2) AS C1T_MeanSS,

  LEFT(C2_Mean,2) AS C2_MeanHH,
  SUBSTRING(C2_Mean, 4,2) AS C2_MeanMM,
  SUBSTRING(C2_Mean, 7,2) AS C2_MeanSS,

  LEFT(C3_Mean,2) AS C3_MeanHH,
  SUBSTRING(C3_Mean, 4,2) AS C3_MeanMM,
  --This splits the time into the seconds column.
  SUBSTRING(C3_Mean, 7,2) AS C3_MeanSS,

  LEFT(C4_Mean,2) AS C4_MeanHH,
  SUBSTRING(C4_Mean, 4,2) AS C4_MeanMM,
  SUBSTRING(C4_Mean, 7,2) AS C4_MeanSS,
 

FROM `big-dynamo-354814.Response_time.string`)


--We can look at a couple of replationships using our transformed data.
--First we can look at the average response times for each month of our data.

SELECT
--These statements use the hh,mm,ss columns and create a new column with the total time in seconds.
  Month,
  SUM(CAST(C1_MeanHH AS INT64) * 3600 + CAST(C1_MeanMM AS INT64) * 60 + CAST(C1_MeanSS AS INT64)) AS C1_AVG_Time_Seconds,
  SUM(CAST(C1T_MeanHH AS INT64) * 3600 + CAST(C1T_MeanMM AS INT64) * 60 + CAST(C1T_MeanSS AS INT64)) AS C1T_AVG_Time_Seconds,
  SUM(CAST(C2_MeanHH AS INT64) * 3600 + CAST(C2_MeanMM AS INT64) * 60 + CAST(C2_MeanSS AS INT64)) AS C2_AVG_Time_Seconds,
  SUM(CAST(C3_MeanHH AS INT64) * 3600 + CAST(C3_MeanMM AS INT64) * 60 + CAST(C3_MeanSS AS INT64)) AS C3_AVG_Time_Seconds,
  SUM(CAST(C4_MeanHH AS INT64) * 3600 + CAST(C4_MeanMM AS INT64) * 60 + CAST(C4_MeanSS AS INT64)) AS C4_AVG_Time_Seconds

  

FROM `big-dynamo-354814.Response_time.int_AVERAGE`

GROUP BY Month

--We can also look at the total mean response times grouped by the different regions.

SELECT
--These statements use the hh,mm,ss columns and create a new column with the total time in seconds.
  Region,
  SUM(CAST(C1_MeanHH AS INT64) * 3600 + CAST(C1_MeanMM AS INT64) * 60 + CAST(C1_MeanSS AS INT64)) AS C1_AVG_Time_Seconds,
  SUM(CAST(C1T_MeanHH AS INT64) * 3600 + CAST(C1T_MeanMM AS INT64) * 60 + CAST(C1T_MeanSS AS INT64)) AS C1T_AVG_Time_Seconds,
  SUM(CAST(C2_MeanHH AS INT64) * 3600 + CAST(C2_MeanMM AS INT64) * 60 + CAST(C2_MeanSS AS INT64)) AS C2_AVG_Time_Seconds,
  SUM(CAST(C3_MeanHH AS INT64) * 3600 + CAST(C3_MeanMM AS INT64) * 60 + CAST(C3_MeanSS AS INT64)) AS C3_AVG_Time_Seconds,
  SUM(CAST(C4_MeanHH AS INT64) * 3600 + CAST(C4_MeanMM AS INT64) * 60 + CAST(C4_MeanSS AS INT64)) AS C4_AVG_Time_Seconds
  
  

FROM `big-dynamo-354814.Response_time.int_AVERAGE`

GROUP BY Region


--We can also look at trends in the number of incidents in each category across the different regions.
SELECT
  Region,
  --This returns the average count as a whole integer.
  FLOOR(AVG(C1_Count)) AS AVG_C1_Count,
  FLOOR(AVG(C1T_Count)) AS AVG_C1T_Count,
  FLOOR(AVG(C2_Count)) AS AVG_C2_Count,
  FLOOR(AVG(C3_Count)) AS AVG_C3_Count,
  FLOOR(AVG(C4_Count)) AS AVG_C4_Count

FROM `big-dynamo-354814.Response_time.Cleaned_data`

GROUP BY 
  Region


--We can also look at the same trend across time.
SELECT
  Month,
  --This returns the average count as a whole integer.
  FLOOR(AVG(C1_Count)) AS AVG_C1_Count,
  FLOOR(AVG(C1T_Count)) AS AVG_C1T_Count,
  FLOOR(AVG(C2_Count)) AS AVG_C2_Count,
  FLOOR(AVG(C3_Count)) AS AVG_C3_Count,
  FLOOR(AVG(C4_Count)) AS AVG_C4_Count


FROM `big-dynamo-354814.Response_time.Cleaned_data`

GROUP BY 
  Month