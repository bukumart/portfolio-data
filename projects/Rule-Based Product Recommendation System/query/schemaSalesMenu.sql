select
	TABLE_SCHEMA as 'dbName'
from
	information_schema.COLUMNS
where
	TABLE_NAME = 'tr_salesmenu'
	and COLUMN_NAME = 'salesType'
	and TABLE_SCHEMA in (
	select
		TABLE_SCHEMA
	from
		information_schema.TABLES
	where
		TABLE_NAME = 'tr_salesmenu'
		and TABLE_ROWS > 0)
	and TABLE_SCHEMA in (
	select
		TABLE_SCHEMA
	from
		information_schema.COLUMNS
	where
		TABLE_NAME = 'ms_branch'
		and COLUMN_NAME = 'branchCode')