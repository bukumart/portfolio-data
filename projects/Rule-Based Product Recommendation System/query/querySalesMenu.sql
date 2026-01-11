SELECT
    tsm.salesNum,
    ts2.salesDate,
    '{dbName}' AS dbName,
    '{companyCode}' AS companyCode,
    ts2.branchID,
    mb.branchCode,
    tsm.menuID
FROM
(
    SELECT
        ts.salesNum,
        GROUP_CONCAT(ts.menuID SEPARATOR ', ') AS menuID,
        COUNT(*) AS menuCount
    FROM {dbName}.tr_salesmenu ts
    WHERE
        INSTR(ts.salesType, 'ezo') > 0
        AND ts.price > 0
        AND ts.statusID IN (13, 14, 34)
        AND EXISTS (
            SELECT 1
            FROM {dbName}.tr_saleshead h
            WHERE
                h.salesNum = ts.salesNum
                AND h.salesDate >= DATE_SUB(CURRENT_DATE(), INTERVAL {startWeek} WEEK)
                AND h.salesDate < DATE_SUB(CURRENT_DATE(), INTERVAL {endWeek} WEEK)
        )
    GROUP BY ts.salesNum
    HAVING menuCount > 1
) tsm
JOIN {dbName}.tr_saleshead ts2
    ON tsm.salesNum = ts2.salesNum
    AND ts2.statusID = 8
JOIN {dbName}.ms_branch mb
    ON ts2.branchID = mb.branchID