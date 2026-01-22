select
	distinct companyCode,
    source
from
	esb_loop_steroid.tr_transaction tt
where
	tt.transactionDate >= timestamp(current_date() - interval 1 week)