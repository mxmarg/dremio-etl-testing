MERGE INTO <INSERT_SOURCE_PATH_HERE>."<INSERT_RUN_ID_HERE>".processed_table p
USING (
    SELECT
        "managedObjectId",
        "measuringNodeId",
        "time",
        "date",
        "hour",
        CAST("C_H111" AS DOUBLE) AS "D1", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H112" AS DOUBLE) AS "D2", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H113" AS DOUBLE) AS "D3", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H114" AS DOUBLE) AS "D4", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H115" AS DOUBLE) AS "D5", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H116" AS DOUBLE) AS "D6", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H117" AS DOUBLE) AS "D7", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H118" AS DOUBLE) AS "D8", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H119" AS DOUBLE) AS "D9", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H120" AS DOUBLE) AS "D10", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H121" AS DOUBLE) AS "D11", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H122" AS DOUBLE) AS "D12", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H123" AS DOUBLE) AS "D13", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H124" AS DOUBLE) AS "D14", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H125" AS DOUBLE) AS "D15", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H126" AS DOUBLE) AS "D16", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H127" AS DOUBLE) AS "D17", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H128" AS DOUBLE) AS "D18", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H129" AS DOUBLE) AS "D19", -- CAST is needed on v24.2.x, but not v25.2.x
        CAST("C_H130" AS DOUBLE) AS "D20", -- CAST is needed on v24.2.x, but not v25.2.x
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
    WHERE "date" = '<INSERT_DATE_HERE>' AND <INSERT_HOUR_RANGE_HERE>
) AS r
ON p."date" = r."date" AND p."hour" = r."hour" AND p."managedObjectId" = r."managedObjectId" AND p."measuringNodeId" = r."measuringNodeId" AND p."time" = r."time"
WHEN MATCHED THEN UPDATE SET
    D1 = r.D1,
    D2 = r.D2,
    D3 = r.D3,
    D4 = r.D4,
    D5 = r.D5,
    D6 = r.D6,
    D7 = r.D7,
    D8 = r.D8,
    D9 = r.D9,
    D10 = r.D10,
    D11 = r.D11,
    D12 = r.D12,
    D13 = r.D13,
    D14 = r.D14,
    D15 = r.D15,
    D16 = r.D16,
    D17 = r.D17,
    D18 = r.D18,
    D19 = r.D19,
    D20 = r.D20,
    V1 = r.V1,
    V2 = r.V2,
    V3 = r.V3,
    V4 = r.V4,
    V5 = r.V5,
    V6 = r.V6,
    V7 = r.V7,
    V8 = r.V8,
    V9 = r.V9,
    V10 = r.V10
WHEN NOT MATCHED THEN INSERT (
    "managedObjectId",
    "measuringNodeId",
    "time",
    "date",
    "hour",
    "D1" ,
    "D2" ,
    "D3" ,
    "D4" ,
    "D5" ,
    "D6" ,
    "D7" ,
    "D8" ,
    "D9" ,
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
    "V1" ,
    "V2" ,
    "V3" ,
    "V4" ,
    "V5" ,
    "V6" ,
    "V7" ,
    "V8" ,
    "V9" ,
    "V10"
) VALUES (
    r."managedObjectId",
    r."measuringNodeId",
    r."time",
    r."date",
    r."hour",
    r."D1" ,
    r."D2" ,
    r."D3" ,
    r."D4" ,
    r."D5" ,
    r."D6" ,
    r."D7" ,
    r."D8" ,
    r."D9" ,
    r."D10",
    r."D11",
    r."D12",
    r."D13",
    r."D14",
    r."D15",
    r."D16",
    r."D17",
    r."D18",
    r."D19",
    r."D20",
    r."V1" ,
    r."V2" ,
    r."V3" ,
    r."V4" ,
    r."V5" ,
    r."V6" ,
    r."V7" ,
    r."V8" ,
    r."V9" ,
    r."V10"
)