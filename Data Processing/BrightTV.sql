-- STEP 1: Exploring data
-- Count number of user profile records (checking data size)
SELECT COUNT(*) FROM brighttv.raw.user_profiles;

-- Count number of view sessions (checking data size)
SELECT COUNT(*) FROM BRIGHTTV_ANALYTICS.RAW.view_sessions;

-- Preview first 10 rows of view_sessions (initial data inspection)
SELECT * 
FROM BRIGHTTV_ANALYTICS.RAW.view_sessions LIMIT 10;

-- Preview viewership table from analysis schema
SELECT * FROM "BRIGHTTVANALYSIS"."ANALYSIS"."VIEWERSHIP" LIMIT 10;

-- Preview user profiles table from analysis schema
SELECT * FROM "BRIGHTTVANALYSIS"."ANALYSIS"."USER_PROFILES" LIMIT 10;

-- Check unique channels to understand content distribution
SELECT DISTINCT CHANNEL2 FROM "BRIGHTTVANALYSIS"."ANALYSIS"."VIEWERSHIP";

-- Check unique provinces to understand geographic coverage
SELECT DISTINCT PROVINCE FROM "BRIGHTTVANALYSIS"."ANALYSIS"."USER_PROFILES";


-- STEP 2: Using the correct database and schema
-- Ensure all transformations are applied within the correct database & schema
USE DATABASE BRIGHTTVANALYSIS;
USE SCHEMA ANALYSIS;


-- STEP 3: Removing duplicate column (userid)
-- Create a cleaned version of VIEWERSHIP that contains only the required fields.
-- Keeping UserID, channel information, timestamp, and duration.
CREATE OR REPLACE TABLE VIEWERSHIP_CLEAN AS
SELECT 
    "UserID",
    CHANNEL2,
    RECORDDATE2,
    DURATION_2
FROM BRIGHTTVANALYSIS.ANALYSIS.VIEWERSHIP;


-- STEP 4: Separating date and time from RECORDDATE2
-- Convert RECORDDATE2 (string) into proper timestamp, date, and time fields.
-- This helps with time-based analysis (hour, day, month trends).
CREATE OR REPLACE TABLE VIEWERSHIP_TRANSFORMED AS
SELECT
    "UserID",
    CHANNEL2,
    RECORDDATE2,

    -- Convert full string to timestamp
    TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI') AS RECORD_TS,

    -- Extract date only
    TO_DATE(TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')) AS RECORD_DATE,

    -- Extract time only
    TO_TIME(TO_TIMESTAMP(RECORDDATE2, 'YYYY/MM/DD HH24:MI')) AS RECORD_TIME,

    DURATION_2
FROM BRIGHTTVANALYSIS.ANALYSIS.VIEWERSHIP_CLEAN;


-- STEP 5: Merging user profiles with viewership data
-- This joins session data with user demographic info.
-- Also cleans and standardizes fields (age, names, missing values).
CREATE OR REPLACE TABLE BRIGHTTVANALYSIS.ANALYSIS.MERGED_VIEWERSHIP AS
SELECT
    -- Unique viewer info
    v."UserID",
    
    -- Clean NAME, replacing "None" or empty fields with 'Unknown'
    COALESCE(NULLIF(u.NAME, 'None'), 'Unknown') AS NAME,
    COALESCE(NULLIF(u.SURNAME, 'None'), 'Unknown') AS SURNAME,
    COALESCE(NULLIF(u.EMAIL, 'None'), 'Unknown') AS EMAIL,
    COALESCE(NULLIF(u.GENDER, 'None'), 'Unknown') AS GENDER,
    COALESCE(NULLIF(u.RACE, 'None'), 'Unknown') AS RACE,
    COALESCE(NULLIF(u.PROVINCE, 'None'), 'Unknown') AS PROVINCE,
    COALESCE(NULLIF(u.SOCIAL_MEDIA_HANDLE, 'None'), 'Not Provided') AS SOCIAL_MEDIA_HANDLE,

    -- Clean and categorize age
    -- NULLIF converts 0 → NULL (invalid ages)
    NULLIF(u.AGE, 0) AS AGE,

    -- Creating meaningful age segments for insights/dashboards
    CASE
        WHEN u.AGE IS NULL OR TRIM(LOWER(u.AGE)) IN ('null', '') THEN 'Unknown'
        WHEN CAST(u.AGE AS INT) < 18 THEN 'Teenagers (<18)'
        WHEN CAST(u.AGE AS INT) BETWEEN 18 AND 24 THEN 'Young Adults (18–24)'
        WHEN CAST(u.AGE AS INT) BETWEEN 25 AND 34 THEN 'Adults (25–34)'
        WHEN CAST(u.AGE AS INT) BETWEEN 35 AND 44 THEN 'Middle-aged Adults (35–44)'
        WHEN CAST(u.AGE AS INT) BETWEEN 45 AND 54 THEN 'Mature Adults (45–54)'
        ELSE 'Seniors (55+)' -- default category
    END AS AGE_CATEGORY,

    -- Channel the user watched
    v.CHANNEL2,
    
    -- Convert UTC to South African Standard Time (UTC+2)
    DATEADD(HOUR, 2, v.RECORD_DATE) AS RECORD_DATE_SA,
    
    -- Extract day name (Mon–Sun) from SA time
    DAYNAME(DATEADD(HOUR, 2, v.RECORD_DATE)) AS DAY_NAME,

    -- Categorize day as weekday/weekend
    CASE
        WHEN DAYNAME(DATEADD(HOUR, 2, v.RECORD_DATE)) IN ('Sat', 'Sun') THEN 'Weekend'
        ELSE 'Weekday'
    END AS DAY_TYPE,

    -- Extract month name for monthly trend analysis
    MONTHNAME(DATEADD(HOUR, 2, v.RECORD_DATE)) AS MONTH_NAME,

    -- Adjust viewing time to SA timezone
    DATEADD(HOUR, 2, v.RECORD_TIME) AS RECORD_TIME_SA,

    -- Extract hour of the day (0–23) to analyze peak watching times
    HOUR(DATEADD(HOUR, 2, v.RECORD_TIME)) AS HOUR_OF_DAY_SA,

    -- Duration of viewing session
    v.DURATION_2,

    -- Convert duration to total seconds
    (DATE_PART('HOUR', v.DURATION_2) * 3600 +
     DATE_PART('MINUTE', v.DURATION_2) * 60 +
     DATE_PART('SECOND', v.DURATION_2)) AS DURATION_SECONDS,

    -- Duration in minutes (rounded)
    ROUND(
        (DATE_PART('HOUR', v.DURATION_2) * 3600 +
         DATE_PART('MINUTE', v.DURATION_2) * 60 +
         DATE_PART('SECOND', v.DURATION_2)) / 60,
    2) AS DURATION_MINUTES,

    -- Categorizing how long users watched
    -- Helpful for audience engagement analytics
    CASE
        WHEN (DATE_PART('HOUR', v.DURATION_2) * 3600 +
              DATE_PART('MINUTE', v.DURATION_2) * 60 +
              DATE_PART('SECOND', v.DURATION_2)) = 0 THEN 'No Viewing'
        WHEN (DATE_PART('HOUR', v.DURATION_2) * 3600 +
              DATE_PART('MINUTE', v.DURATION_2) * 60 +
              DATE_PART('SECOND', v.DURATION_2)) <= 60 THEN 'Very Short (<1 min)'
        WHEN (DATE_PART('HOUR', v.DURATION_2) * 3600 +
              DATE_PART('MINUTE', v.DURATION_2) * 60 +
              DATE_PART('SECOND', v.DURATION_2)) BETWEEN 61 AND 300 THEN 'Short (1–5 min)'
        WHEN (DATE_PART('HOUR', v.DURATION_2) * 3600 +
              DATE_PART('MINUTE', v.DURATION_2) * 60 +
              DATE_PART('SECOND', v.DURATION_2)) BETWEEN 301 AND 900 THEN 'Moderate (5–15 min)'
        WHEN (DATE_PART('HOUR', v.DURATION_2) * 3600 +
              DATE_PART('MINUTE', v.DURATION_2) * 60 +
              DATE_PART('SECOND', v.DURATION_2)) BETWEEN 901 AND 1800 THEN 'Long (15–30 min)'
        ELSE 'Very Long (>30 min)' -- Extended viewing session
    END AS VIEW_DURATION_CATEGORY

FROM BRIGHTTVANALYSIS.ANALYSIS.VIEWERSHIP_TRANSFORMED v
JOIN BRIGHTTVANALYSIS.ANALYSIS.USER_PROFILES u
    ON v."UserID" = u.USERID

-- Group by all selected non-aggregated fields
GROUP BY 
    v."UserID", u.NAME, u.SURNAME, u.EMAIL, u.GENDER, u.RACE, u.AGE,
    u.PROVINCE, u.SOCIAL_MEDIA_HANDLE, v.CHANNEL2, v.RECORD_DATE, 
    v.RECORD_TIME, v.DURATION_2;

-- STEP 6: Verify the merged data
-- Quick preview to check if the merge worked
SELECT * FROM BRIGHTTVANALYSIS.ANALYSIS.MERGED_VIEWERSHIP LIMIT 10;

-- Full dataset check 
SELECT * FROM BRIGHTTVANALYSIS.ANALYSIS.MERGED_VIEWERSHIP;
