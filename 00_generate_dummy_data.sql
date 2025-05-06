--ALTER SESSION SET "store.parquet.block-size" = 10000000;
CREATE TABLE sizingtest."<INSERT_RUN_ID_HERE>".raw_data."<INSERT_DATE_HERE>"
STORE AS (type => 'parquet') AS
WITH ints AS (
    SELECT * 
    FROM ( VALUES 
        ROW(0), ROW(1), ROW(2), ROW(3), ROW(4), ROW(5), ROW(6), ROW(7), ROW(8), ROW(9)
) AS v(r)
), chars AS (
    SELECT * 
    FROM ( VALUES 
        ROW('A'), ROW('B'), ROW('C'), ROW('D'), ROW('E'), ROW('F'), ROW('G'), 
        ROW('H'), ROW('I'), ROW('J'), ROW('K'), ROW('L'), ROW('M'), ROW('N'), 
        ROW('O'), ROW('P'), ROW('Q'), ROW('R'), ROW('S'), ROW('T'), ROW('U'), 
        ROW('V'), ROW('W'), ROW('X'), ROW('Y'), ROW('Z')
) AS v(r)
), mo_mn AS(
    SELECT 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxmo-' managedObjectId, 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxmn-' measuringNodeId
)
SELECT
    NOW() AS creationTime,
    mo_mn.managedObjectId || c1.r AS managedObjectId,
    mo_mn.measuringNodeId || c2.r AS measuringNodeId,
    CAST('<INSERT_DATE_HERE> ' || CAST(i6.r AS VARCHAR) || CAST(i5.r AS VARCHAR) || ':' || CAST(i4.r AS VARCHAR) ||CAST(i3.r AS VARCHAR) || ':' || CAST(i2.r AS VARCHAR) || CAST(i1.r AS VARCHAR) AS TIMESTAMP) AS "_time",
    FALSE isC8y,
    c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r AS C_H101,
    c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c2.r AS C_H102,
    c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c2.r || c2.r AS C_H103,
    c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c2.r || c2.r || c2.r AS C_H104,
    c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c1.r || c2.r || c2.r || c2.r || c2.r AS C_H105,
    c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c1.r || c1.r || c1.r || c1.r AS C_H106,
    c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c1.r || c1.r || c1.r AS C_H107,
    c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c1.r || c1.r AS C_H108,
    c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c1.r AS C_H109,
    c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r || c2.r AS C_H110,
    CAST(i1.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS INTEGER) AS C_H111,
    CAST(i2.r + 10*i3.r + 100*i4.r + 1000*i5.r + 10000*i6.r + 100000*i1.r AS INTEGER) AS C_H112,
    CAST(i3.r + 10*i4.r + 100*i5.r + 1000*i6.r + 10000*i1.r + 100000*i2.r AS INTEGER) AS C_H113,
    CAST(i4.r + 10*i5.r + 100*i6.r + 1000*i1.r + 10000*i2.r + 100000*i3.r AS INTEGER) AS C_H114,
    CAST(i5.r + 10*i6.r + 100*i1.r + 1000*i2.r + 10000*i3.r + 100000*i4.r AS INTEGER) AS C_H115,
    CAST(i6.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS INTEGER) AS C_H116,
    CAST(i2.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS INTEGER) AS C_H117,
    CAST(i3.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS INTEGER) AS C_H118,
    CAST(i4.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS INTEGER) AS C_H119,
    CAST(i5.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS INTEGER)AS C_H120,
    CAST(i1.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS FLOAT) AS C_H121,
    CAST(i2.r + 10*i3.r + 100*i4.r + 1000*i5.r + 10000*i6.r + 100000*i1.r AS FLOAT) AS C_H122,
    CAST(i3.r + 10*i4.r + 100*i5.r + 1000*i6.r + 10000*i1.r + 100000*i2.r AS FLOAT) AS C_H123,
    CAST(i4.r + 10*i5.r + 100*i6.r + 1000*i1.r + 10000*i2.r + 100000*i3.r AS FLOAT) AS C_H124,
    CAST(i5.r + 10*i6.r + 100*i1.r + 1000*i2.r + 10000*i3.r + 100000*i4.r AS FLOAT) AS C_H125,
    CAST(i6.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS FLOAT) AS C_H126,
    CAST(i2.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS FLOAT) AS C_H127,
    CAST(i3.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS FLOAT) AS C_H128,
    CAST(i4.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS FLOAT) AS C_H129,
    CAST(i5.r + 10*i2.r + 100*i3.r + 1000*i4.r + 10000*i5.r + 100000*i6.r AS FLOAT)AS C_H130
FROM ints i1
JOIN mo_mn ON TRUE
JOIN ints i2 ON i2.r < 6
JOIN ints i3 ON TRUE
JOIN ints i4 ON i4.r < 6
JOIN ints i5 ON TRUE
JOIN ints i6 ON (i6.r < 2 OR i6.r = 2 AND i5.r < 4)
JOIN chars c1 ON TRUE
JOIN chars c2 ON TRUE
ORDER BY managedObjectId, measuringNodeId, "_time"
;