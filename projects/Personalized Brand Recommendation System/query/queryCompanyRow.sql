select
	tt2.companyCode,
	count(*) as rowCount
from
	esb_loop_steroid.tr_transactiondetail tt
join esb_loop_steroid.tr_transaction tt2 on
	tt.salesNum = tt2.salesNum
group by
	1