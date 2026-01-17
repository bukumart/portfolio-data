select
	salesNum,
	tes.salesDate,
	'{dbName}' as dbName,
	'{companyCode}' as companyCode,
	tes.branchID,
	mb.branchCode,
	orderData
from
	(
	select
		te.ezoOrderID as salesNum,
		date(te.createdDate) as salesDate,
		te.branchID,
		te.orderData
	from
		{dbName}.tr_ezodelivery te
	left join {dbName}.tr_customertransaction tc on
		te.ezoOrderID = tc.orderID
	where
		te.createdDate >= timestamp(DATE_SUB(CURRENT_DATE(), interval {startWeek} WEEK))
		and te.createdDate < timestamp(DATE_SUB(CURRENT_DATE(), interval {endWeek} WEEK))
        and tc.salesNum is null
		and te.statusID not in (50, 48, 37, 39, 24, 25, 23, 21, 19, 12, 2)) tes
left join {dbName}.ms_branch mb on
	tes.branchID = mb.branchID