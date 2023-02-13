--Write the SQL statements that allow you to answer the following questions
--• Total number of rows;
SELECT COUNT(*) FROM sensors;

--• Number of distinct sensors present on the database;
SELECT COUNT(DISTINCT name) FROM sensors;

--• Number of rows for the sensor PPL340;
SELECT COUNT(*) FROM sensors WHERE name='PPL340';

--• The number of rows by year for the sensor PPL340;
SELECT year, COUNT(*) FROM sensors WHERE name='PPL340' GROUP BY year;

--• Average number of readings by year for the sensor PPL340;
SELECT year, AVG(COUNT(*)) FROM sensors WHERE name='PPL340' GROUP BY year;

--• For PPL340, Identify the years in which the number of readings is less than the average;

WITH avg_readings AS (
  SELECT year, AVG(COUNT(*)) AS avg_readings FROM sensors WHERE name='PPL340' GROUP BY year
)
SELECT year FROM avg_readings WHERE (SELECT COUNT(*) FROM sensors WHERE name='PPL340' AND year=avg_readings.year) < avg_readings;

