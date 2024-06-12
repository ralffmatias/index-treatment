
--	Primeiro executar a query "indexes_repetidos" para inserir valores na tabela global ##buscar_indices
--	Gera o código para deletar os índices repetidos

	if object_id('tempdb..#code', 'u') is not null
	begin
		drop table #code
	end
	create table #code
		(
			texto varchar(1000)
		)

	declare	@nm_index			varchar(100),
			@nm_schema			varchar(5),
			@nm_table			varchar(100)

	declare del cursor for
	select	nm_index,
			nm_schema,	
			nm_table	
	from	##buscar_indices

	open del
	fetch next from del into @nm_index, @nm_schema, @nm_table

	while	@@FETCH_STATUS = 0
	begin

		insert	into #code
		select	'	if exists'																union all
		select	'                ('															union all
		select	'                   select  top 1 1'										union all
		select	'                   from    sys.objects ob'									union all
		select	'                   join    sys.schemas sc'									union all
		select	'                   on      ob.schema_id = sc.schema_id'					union all
		select	'                   join    sys.indexes id'									union all
		select	'                   on      ob.object_id = id.object_id'					union all
		select	'                   where   ob.name = ''' + @nm_table + ''''				union all
		select	'                   and     sc.name = ''' + @nm_schema + ''''				union all
		select	'                   and     id.name = ''' + @nm_index + ''''				union all
		select	'                )'															union all
		select	'    begin'																	union all
		select	'        drop index [' + @nm_index + '] on ' + @nm_schema + '.' + @nm_table	union all
		select	'    end'																	union all
		select	''																			union all
		select	''
		
		fetch next from del into @nm_index, @nm_schema, @nm_table
		
	end

	close del
	deallocate del

	select texto from #code

	if object_id('tempdb..##buscar_indices', 'u') is not null
	begin
		drop table ##buscar_indices
	end
