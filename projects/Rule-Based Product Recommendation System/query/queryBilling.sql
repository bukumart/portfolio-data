select
	dbName,
	companyCode ,
	ms_company.companyId,
	companyName ,
	groupName as companyGroupName,
	isBilled
from
	ms_company
left join ms_companygroup on
	ms_company.groupId = ms_companygroup.groupId
group by
	1,
	2,
	3,
	4,
	5,
	6
union all
select
	'fnb_sci_prod' as dbName,
	companyCode ,
	ms_company.companyId,
	companyName ,
	groupName as companyGroupName,
	isBilled
from
	ms_company
left join ms_companygroup on
	ms_company.groupId = ms_companygroup.groupId
where
	dbName = 'fnb_sci'
group by
	1,
	2,
	3,
	4,
	5,
	6