select
	TABLE_SCHEMA as 'dbName'
from
	information_schema.COLUMNS
where
	TABLE_NAME = 'tr_ezodelivery'
	and COLUMN_NAME = 'orderData'
	and TABLE_SCHEMA in (
	select
		TABLE_SCHEMA
	from
		information_schema.TABLES
	where
		TABLE_NAME = 'tr_ezodelivery'
		and TABLE_ROWS > 0)
	and TABLE_SCHEMA in (
	select
		TABLE_SCHEMA
	from
		information_schema.COLUMNS
	where
		TABLE_NAME = 'tr_ezodelivery'
		and COLUMN_NAME = 'statusID')
	and TABLE_SCHEMA in (
	select
		TABLE_SCHEMA
	from
		information_schema.COLUMNS
	where
		TABLE_NAME = 'tr_customertransaction'
		and COLUMN_NAME = 'salesNum')
	and TABLE_SCHEMA in (
	select
		TABLE_SCHEMA
	from
		information_schema.COLUMNS
	where
		TABLE_NAME = 'ms_branch'
		and COLUMN_NAME = 'branchCode')