-- Level 1: Data Exploration and Basic Operations
-- Task 1.1: Load and Inspect Data
-- Upload your dataset to Snowflake:
CREATE DATABASE TrainData;
USE DATABASE TrainData;
CREATE SCHEMA PublicSchema;
USE SCHEMA PublicSchema;
CREATE
OR REPLACE STAGE TrainStage;
// PUT file://C:/DataEngineer/Railway_info.csv @TrainStage; //run on cmd
LIST @TrainStage;
// creating table
CREATE TABLE TrainDetails (
    Train_No INT,
    Train_Name STRING,
    Source_Station_Name STRING,
    Destination_Station_Name STRING,
    days STRING
);
// storing into tables from stage
COPY INTO TrainDetails
FROM
    @TrainStage FILE_FORMAT = (
        TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1
    );
// Inspect Data:
SELECT
    *
FROM
    TrainDetails
LIMIT
    10;
DESCRIBE TABLE TrainDetails;
// Task 1.2: Basic Statistics
    // Count Trains:
SELECT
    COUNT(*) AS TotalTrains
FROM
    TrainDetails;
//Unique Source and Destination Stations:
SELECT
    COUNT(DISTINCT Source_Station_Name) AS UniqueSourceStations,
    COUNT(DISTINCT DESTINATION_STATION_NAME) AS UniqueDestinationStations
FROM
    TrainDetails;
// Most Common Source and Destination Stations:
SELECT
    SOURCE_STATION_NAME,
    COUNT(*) AS Count
FROM
    TrainDetails
GROUP BY
    SOURCE_STATION_NAME
ORDER BY
    Count DESC
LIMIT
    1;
SELECT
    DESTINATION_STATION_NAME,
    COUNT(*) AS Count
FROM
    TrainDetails
GROUP BY
    DESTINATION_STATION_NAME
ORDER BY
    Count DESC
LIMIT
    1;
// Task 1.3: Data Cleaning
    // Handle Missing Values:
UPDATE
    TrainDetails
SET
    Source_Station_Name = 'UNKNOWN'
WHERE
    Source_Station_Name IS NULL;
UPDATE
    TrainDetails
SET
    Source_Station_Name = UPPER(Source_Station_Name),
    Destination_Station_Name = UPPER(Destination_Station_Name);
select
    *
from
    TRAINDETAILS;
UPDATE
    TrainDetails
SET
    Days = UPPER(Days);
-- Level 2: Data Transformation and Aggregation
    -- Task 2.1: Data Filtering
    -- Filter Trains Operating on Specific Day:
SELECT
    *
FROM
    TrainDetails
WHERE
    Days LIKE '%SATURDAY%';
//Extract Trains Starting from a PUNE JN.:
SELECT
    *
FROM
    TrainDetails
WHERE
    SOURCE_STATION_NAME = 'PUNE JN.';
//Extract Trains Starting from a KUSUNDA.:
SELECT
    *
FROM
    TrainDetails
WHERE
    DESTINATION_STATION_NAME = 'KUSUNDA';
-- Task 2.2: Grouping and Aggregation
    -- Trains Originating per Station:
SELECT
    SOURCE_STATION_NAME,
    COUNT(*) AS TrainCount
FROM
    TrainDetails
GROUP BY
    SOURCE_STATION_NAME
ORDER BY
    TrainCount DESC;
// Average Number of Trains Per Day for Each Station:
SELECT
    Source_Station_Name,
    Round(AVG(TrainCount)) AS AvgTrainsPerDay
FROM
    (
        SELECT
            Source_Station_Name,
            COUNT(*) AS TrainCount
        FROM
            TrainDetails
        GROUP BY
            Source_Station_Name,
            Days
    ) AS DailyCounts
GROUP BY
    Source_Station_Name;
//Number of Trains Per Day for Each Station:
SELECT
    Source_Station_Name,
    COUNT(Days) as TrainPerDay
From
    TRAINDETAILS
GROUP BY
    Source_Station_Name,
    Days;
-- Task 2.3: Data Enrichment
    -- Add a Categorization Column:
ALTER TABLE
    TrainDetails
ADD
    COLUMN DayCategory STRING;
UPDATE
    TrainDetails
SET
    DayCategory = CASE
        WHEN Days LIKE '%Saturday%'
        OR Days LIKE '%Sunday%' THEN 'Weekend'
        ELSE 'Weekday'
    END;
select
    *
from
    traindetails;
-- Level 3: Advanced Data Analysis
    -- Task 3.1: Pattern Analysis
    -- Distribution of Train Journeys:
SELECT
    Days,
    COUNT(*) AS TrainCount
FROM
    TrainDetails
GROUP BY
    Days
ORDER BY
    TrainCount DESC;
-- Task 3.2: Correlation and Insights
    -- Correlation Between Days and Train Count:
SELECT
    Days,
    COUNT(*) AS TrainCount
FROM
    TrainDetails
GROUP BY
    Days;
//Top 5 Busiest Routes
SELECT
    SOURCE_STATION_NAME,
    DESTINATION_STATION_NAME,
    COUNT(*) AS TrainCount
FROM
    TrainDetails
GROUP BY
    SOURCE_STATION_NAME,
    DESTINATION_STATION_NAME
ORDER BY
    TrainCount DESC
LIMIT
    5;
-- Pivot Table: Count Trains Operating Each Day
    -- Create a pivot-like table to count trains operating on each day:
SELECT
    COUNT(
        CASE
            WHEN DAYS LIKE '%MON%' THEN 1
        END
    ) AS MONDAY,
    COUNT(
        CASE
            WHEN DAYS LIKE '%TUE%' THEN 1
        END
    ) AS TUESDAY,
    COUNT(
        CASE
            WHEN DAYS LIKE '%WED%' THEN 1
        END
    ) AS WEDNESDAY,
    COUNT(
        CASE
            WHEN DAYS LIKE '%THU%' THEN 1
        END
    ) AS THURSDAY,
    COUNT(
        CASE
            WHEN DAYS LIKE '%FRI%' THEN 1
        END
    ) AS FRIDAY,
    COUNT(
        CASE
            WHEN DAYS LIKE '%SAT%' THEN 1
        END
    ) AS SATUARDAY,
    COUNT(
        CASE
            WHEN DAYS LIKE '%SUN%' THEN 1
        END
    ) AS SUNDAY
FROM
    TrainDetails;
// exporting as csv file
    COPY INTO 'file://C:/DataEngineer/cleaned_TrainData.csv'
FROM
    TrainDetails FILE_FORMAT = (TYPE = '' CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"') SINGLE = TRUE;
-- Ensures a single file is generated