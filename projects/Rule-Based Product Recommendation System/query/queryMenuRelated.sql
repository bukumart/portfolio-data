select
	dbName,
	companyCode,
	branchID,
	branchCode,
	menuID
from
	fact_menurelated fm
where
    dbName = '{dbName}'