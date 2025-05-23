INSERT INTO <INSERT_SOURCE_PATH_HERE>."<INSERT_RUN_ID_HERE>"."processed_table" (
    "managedObjectId",
    "measuringNodeId",
    "time",
    "date",
    "hour",
    "D1",
    "D2",
    "D3",
    "D4",
    "D5",
    "D6",
    "D7",
    "D8",
    "D9",
    "D10",
    "D11",
    "D12",
    "D13",
    "D14",
    "D15",
    "D16",
    "D17",
    "D18",
    "D19",
    "D20",
    "V1",
    "V2",
    "V3",
    "V4",
    "V5",
    "V6",
    "V7",
    "V8",
    "V9",
    "V10"
)
SELECT 
    "managedObjectId",
    "measuringNodeId",
    "time",
    "date",
    "hour",
    "C_H111" AS "D1",
    "C_H112" AS "D2",
    "C_H113" AS "D3",
    "C_H114" AS "D4",
    "C_H115" AS "D5",
    "C_H116" AS "D6",
    "C_H117" AS "D7",
    "C_H118" AS "D8",
    "C_H119" AS "D9",
    "C_H120" AS "D10",
    "C_H121" AS "D11",
    "C_H122" AS "D12",
    "C_H123" AS "D13",
    "C_H124" AS "D14",
    "C_H125" AS "D15",
    "C_H126" AS "D16",
    "C_H127" AS "D17",
    "C_H128" AS "D18",
    "C_H129" AS "D19",
    "C_H130" AS "D20",
    "C_H101" AS "V1",
    "C_H102" AS "V2",
    "C_H103" AS "V3",
    "C_H104" AS "V4",
    "C_H105" AS "V5",
    "C_H106" AS "V6",
    "C_H107" AS "V7",
    "C_H108" AS "V8",
    "C_H109" AS "V9",
    "C_H110" AS "V10"
FROM <INSERT_SOURCE_PATH_HERE>."<INSERT_RUN_ID_HERE>".raw_table
WHERE "date" = '<INSERT_DATE_HERE>' AND "hour" = <INSERT_HOUR_HERE>