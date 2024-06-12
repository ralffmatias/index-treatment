
--	Cria tabela com indces de nivel 1 (apenas uma coluna).
    IF OBJECT_ID('tempdb..#indexes_columns_1', 'U') IS NOT NULL
    BEGIN
        DROP TABLE #indexes_columns_1
    END
    create table #indexes_columns_1
        (
            [object_id]        int,
            index_id           int,
            index_column_id    int,
            column_id          int
        )
    INSERT INTO #indexes_columns_1
        (
            [object_id],
            index_id,
            index_column_id,
            column_id
        )
	select	ic.object_id,
			ic.index_id,
			max(ic.index_column_id)	as index_column_id,
			ic.column_id
	from	sys.index_columns ic
	join	sys.objects ob
	on		ob.object_id = ic.object_id
	and		ob.type = 'u'
	join	sys.indexes ind
	on		ind.object_id = ic.object_id
	and		ind.index_id = ic.index_id
	and		ind.type = 2
	group	by ic.object_id,
			ic.index_id,
			ic.column_id
	having	max(index_column_id) = 1

-----------------------------------------------------

--	Cria tabela com indces de nivel 2 (Duas colunas ou mais)
	if object_id('tempdb..#indexes_columns_2', 'u') is not null
	begin
		drop table #indexes_columns_2
	end

	create table #indexes_columns_2
		(
			[object_id]			int,
			index_id			int,
			index_column_id		int,
			column_id			int,
			index_column_2_id	int,
			column_2_id			int
		)
	insert into #indexes_columns_2
		(
			[object_id],		
			index_id,		
			index_column_id
		)
	select	ic.object_id,
			ic.index_id,
			min(ic.index_column_id)	as index_column_id
	from	sys.index_columns ic
	join	sys.objects ob
	on		ob.object_id = ic.object_id
	and		ob.type = 'u'
	group	by ic.object_id,
			ic.index_id
	having	max(index_column_id) > 1


	update	ic2
	set		column_id			= ic.column_id,	
			index_column_2_id	= ic3.index_column_id,
			column_2_id			= ic3.column_id	
	from	#indexes_columns_2 ic2
	join	sys.index_columns ic
	on		ic.object_id = ic2.object_id
	and		ic.index_id = ic2.index_id
	join	sys.index_columns ic3
	on		ic3.object_id = ic2.object_id
	and		ic3.index_id = ic2.index_id
	and		ic3.index_column_id = 2

---------------------------------------------------

--	Retorna índices de nivel 1 que são repetidos
   IF OBJECT_ID('tempdb..#indexes_1', 'U') IS NOT NULL
    BEGIN
        DROP TABLE #indexes_1
    END
    create table #indexes_1
        (
            [type]             nvarchar(120),
            nm_index           varchar(256),
            index_id           int,
            index_column_id    int,
            column_id          int,
            nm_column          nvarchar(256),
            nm_schema          varchar(256),
            nm_table           varchar(256),
            table_id           int,
            [rows]             bigint,
            contagem           int,
            [rank]             bigint
        )
    INSERT INTO #indexes_1
        (
            [type],
            nm_index,
            index_id,
            index_column_id,
            column_id,
            nm_column,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            contagem,
            [rank]
        )
	select	distinct
			ind.type_desc		as type,
			ind.name			as nm_index,
			ind.index_id,
			ic.index_column_id,
			ic.column_id,
			ac.name				as nm_column,
			sc.name				as nm_schema,
			ob.name				as nm_table,
			ob.object_id		as table_id,
			par.rows,
			count(1)	over	(
									partition	by ob.object_id,
												ic.column_id
								)	as			contagem,
			rank()		over	(
									partition	by ob.object_id,
												ic.column_id
									order		by ind.index_id
								)	as			rank
	from	sys.objects ob
	join	sys.indexes ind
	on		ind.object_id = ob.object_id
	join	#indexes_columns_1 ic
	on		ob.object_id = ic.object_id
	and		ind.index_id = ic.index_id
	join	sys.all_columns ac
	on		ac.object_id = ob.object_id
	and		ac.column_id = ic.column_id
	join	sys.schemas sc
	on		sc.schema_id = ob.schema_id
	join	sys.partitions par
	on		ob.object_id = par.object_id
	and		ind.index_id = par.index_id


    IF OBJECT_ID('tempdb..#indexes_repetidos_1', 'U') IS NOT NULL
    BEGIN
        DROP TABLE #indexes_repetidos_1
    END
    create table #indexes_repetidos_1
        (
            [type]                    nvarchar(120),
            nm_index                  varchar(256),
            index_id                  int,
            index_column_id           int,
            column_id                 int,
            nm_column                 nvarchar(256),
            nm_schema                 varchar(256),
            nm_table                  varchar(256),
            table_id                  int,
            [rows]                    bigint,
            nm_index_remanecente      varchar(256),
            index_remanecent_id       int,
            type_index_remanecente    nvarchar(120)
        )
    INSERT INTO #indexes_repetidos_1
        (
            [type],
            nm_index,
            index_id,
            index_column_id,
            column_id,
            nm_column,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            nm_index_remanecente,
            index_remanecent_id,
            type_index_remanecente
        )
	select	ir1.type,
			ir1.nm_index,
			ir1.index_id,
			ir1.index_column_id,
			ir1.column_id,
			ir1.nm_column,
			ir1.nm_schema,
			ir1.nm_table,
			ir1.table_id,
			ir1.rows,
			ir2.nm_index		as nm_index_remanecente,
			ir2.index_id		as index_remanecent_id,
			ir2.type			as type_index_remanecente
	from	#indexes_1 ir1
	join	#indexes_1 ir2
	on		ir1.table_id = ir2.table_id
	and		ir1.column_id = ir2.column_id
	and		ir2.rank = 1
	where	ir1.contagem > 1
	and		ir1.rank > 1


	select	[type],
            nm_index,
            index_id,
            index_column_id,
            column_id,
            nm_column,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            nm_index_remanecente,
            index_remanecent_id,
            type_index_remanecente
	from	#indexes_repetidos_1
	order	by nm_table,
			index_id

------------------------------------------------------------

--	Retorna índices de nivel 1 que são equivalentes a índices de nivel 2
    IF OBJECT_ID('tempdb..#indexes_2', 'U') IS NOT NULL
    BEGIN
        DROP TABLE #indexes_2
    END
    create table #indexes_2
        (
            [type]             nvarchar(120),
            nm_index           varchar(256),
            index_id           int,
            index_nivel        int,
            index_column_id    int,
            column_id          int,
            nm_column          nvarchar(256),
            nm_schema          varchar(256),
            nm_table           varchar(256),
            table_id           int,
            [rows]             bigint,
            contagem           int,
            [rank]             bigint
        )
    INSERT INTO #indexes_2
        (
            [type],
            nm_index,
            index_id,
            index_nivel,
            index_column_id,
            column_id,
            nm_column,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            contagem,
            [rank]
        )
	select	[type],
			nm_index,
			index_id,
			index_nivel,
			index_column_id,
			column_id,
			nm_column,
			nm_schema,
			nm_table,
			table_id,
			[rows],
			count(1)	over	(
									partition	by table_id,
												column_id
								)	as			contagem,
			rank()		over	(
									partition	by table_id,
												column_id
									order		by index_nivel desc,
												index_id
								)	as			rank
	from	(
				select	distinct
						ind.type_desc				as type,
						ind.name					as nm_index,
						ind.index_id,
						sic.index_nivel,
						ic.index_column_id,
						ic.column_id,
						ac.name						as nm_column,
						sc.name						as nm_schema,
						ob.name						as nm_table,
						ob.object_id				as table_id,
						par.rows
				from	sys.objects ob
				join	sys.indexes ind
				on		ind.object_id = ob.object_id
				join	(
							select	object_id,
									index_id,
									index_column_id	,
									column_id
							from	#indexes_columns_2
							union	all
							select	object_id,
									index_id,
									index_column_id	,
									column_id
							from	#indexes_columns_1 ic1
							where	not exists	(
													select	object_id,
															table_id
													from	#indexes_repetidos_1 ir1
													where	ic1.object_id = ir1.table_id
													and		ic1.index_id = ir1.index_id
												)
						) ic
				on		ob.object_id = ic.object_id
				and		ind.index_id = ic.index_id
				join	(
							select	object_id,
									index_id,
									max(index_column_id) as index_nivel
							from	sys.index_columns 
							group	by object_id,
									index_id
						) sic
				on		sic.object_id = ic.object_id
				and		sic.index_id = ic.index_id	
				join	sys.all_columns ac
				on		ac.object_id = ob.object_id
				and		ac.column_id = ic.column_id
				join	sys.schemas sc
				on		sc.schema_id = ob.schema_id
				join	sys.partitions par
				on		ob.object_id = par.object_id
				and		ind.index_id = par.index_id
			) ta

    IF OBJECT_ID('tempdb..#indexes_repetidos_2', 'U') IS NOT NULL
    BEGIN
        DROP TABLE #indexes_repetidos_2
    END
    create table #indexes_repetidos_2
        (
            [type]                     nvarchar(120),
            nm_index                   varchar(256),
            index_id                   int,
            index_nivel                int,
            index_column_id            int,
            column_id                  int,
            nm_column                  nvarchar(256),
            nm_schema                  varchar(256),
            nm_table                   varchar(256),
            table_id                   int,
            [rows]                     bigint,
            nm_index_remanecente       varchar(256),
            index_remanecent_id        int,
            index_remanecente_nivel    int,
            type_index_remanecente     nvarchar(120)
        )
    INSERT INTO #indexes_repetidos_2
        (
            [type],
            nm_index,
            index_id,
            index_nivel,
            index_column_id,
            column_id,
            nm_column,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            nm_index_remanecente,
            index_remanecent_id,
            index_remanecente_nivel,
            type_index_remanecente
        )
	select	ir1.type,
			ir1.nm_index,
			ir1.index_id,
			ir1.index_nivel,
			ir1.index_column_id,
			ir1.column_id,
			ir1.nm_column,
			ir1.nm_schema,
			ir1.nm_table,
			ir1.table_id,
			ir1.rows,
			ir2.nm_index		as nm_index_remanecente,
			ir2.index_id		as index_remanecent_id,
			ir2.index_nivel		as index_remanecente_nivel,
			ir2.type			as type_index_remanecente
	from	#indexes_2 ir1
	join	#indexes_2 ir2
	on		ir1.table_id = ir2.table_id
	and		ir1.column_id = ir2.column_id
	and		ir2.rank = 1
	where	ir1.contagem > 1
	and		ir1.rank > 1
	and		ir1.index_nivel = 1

	
	select	[type],
            nm_index,
            index_id,
            index_nivel,
            index_column_id,
            column_id,
            nm_column,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            nm_index_remanecente,
            index_remanecent_id,
            index_remanecente_nivel,
            type_index_remanecente
	from	#indexes_repetidos_2
	order	by nm_table,
			column_id

------------------------------------------------------------
	
--	Retorna índices de nivel 2 onde a primeira e segunda coluna são iguais
    IF OBJECT_ID('tempdb..#indexes_3', 'U') IS NOT NULL
    BEGIN
        DROP TABLE #indexes_3
    END
    create table #indexes_3
        (
            [type]               nvarchar(120),
            nm_index             varchar(256),
            index_id             int,
            index_nivel          int,
            index_column_id      int,
            column_id            int,
            nm_column            nvarchar(256),
            index_column_2_id    int,
            column_2_id          int,
            nm_column_2          nvarchar(256),
            nm_schema            varchar(256),
            nm_table             varchar(256),
            table_id             int,
            [rows]               bigint,
            contagem             int,
            [rank]               bigint
        )
    INSERT INTO #indexes_3
        (
            [type],
            nm_index,
            index_id,
            index_nivel,
            index_column_id,
            column_id,
            nm_column,
            index_column_2_id,
            column_2_id,
            nm_column_2,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            contagem,
            [rank]
        )
	select	[type],
			nm_index,
			index_id,
			index_nivel,
			index_column_id,
			column_id,
			nm_column,
			index_column_2_id,
			column_2_id,
			nm_column_2,
			nm_schema,
			nm_table,
			table_id,
			[rows],
			count(1)	over	(
									partition	by table_id,
												column_id,
												column_2_id
								)	as			contagem,
			rank()		over	(
									partition	by table_id,
												column_id,
												column_2_id
									order		by index_nivel desc,
												index_id
								)	as			rank
	from	(
				select	distinct
						ind.type_desc				as type,
						ind.name					as nm_index,
						ind.index_id,
						sic.index_nivel,
						ic.index_column_id,
						ic.column_id,
						ac.name						as nm_column,
						ic.index_column_2_id,
						ic.column_2_id,
						ac2.name					as nm_column_2,
						sc.name						as nm_schema,
						ob.name						as nm_table,
						ob.object_id				as table_id,
						par.rows
				from	sys.objects ob
				join	sys.indexes ind
				on		ind.object_id = ob.object_id
				join	(
							select	object_id,
									index_id,
									index_column_id	,
									column_id,
									index_column_2_id,
									column_2_id
							from	#indexes_columns_2
						) ic
				on		ob.object_id = ic.object_id
				and		ind.index_id = ic.index_id
				join	(
							select	object_id,
									index_id,
									max(index_column_id) as index_nivel
							from	sys.index_columns 
							group	by object_id,
									index_id
						) sic
				on		sic.object_id = ic.object_id
				and		sic.index_id = ic.index_id	
				join	sys.all_columns ac
				on		ac.object_id = ob.object_id
				and		ac.column_id = ic.column_id	
				join	sys.all_columns ac2
				on		ac2.object_id = ob.object_id
				and		ac2.column_id = ic.column_2_id
				join	sys.schemas sc
				on		sc.schema_id = ob.schema_id
				join	sys.partitions par
				on		ob.object_id = par.object_id
				and		ind.index_id = par.index_id
			) ta

    IF OBJECT_ID('tempdb..#indexes_repetidos_3', 'U') IS NOT NULL
    BEGIN
        DROP TABLE #indexes_repetidos_3
    END
    create table #indexes_repetidos_3
        (
            [type]                     nvarchar(120),
            nm_index                   varchar(256),
            index_id                   int,
            index_nivel                int,
            index_column_id            int,
            column_id                  int,
            nm_column                  nvarchar(256),
            index_column_2_id          int,
            column_2_id                int,
            nm_column_2                nvarchar(256),
            nm_schema                  varchar(256),
            nm_table                   varchar(256),
            table_id                   int,
            [rows]                     bigint,
            nm_index_remanecente       varchar(256),
            index_remanecent_id        int,
            index_remanecente_nivel    int,
            type_index_remanecente     nvarchar(120)
        )
    INSERT INTO #indexes_repetidos_3
        (
            [type],
            nm_index,
            index_id,
            index_nivel,
            index_column_id,
            column_id,
            nm_column,
            index_column_2_id,
            column_2_id,
            nm_column_2,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            nm_index_remanecente,
            index_remanecent_id,
            index_remanecente_nivel,
            type_index_remanecente
        )
	select	ir1.type,
			ir1.nm_index,
			ir1.index_id,
			ir1.index_nivel,
			ir1.index_column_id,
			ir1.column_id,
			ir1.nm_column,
			ir1.index_column_2_id,
			ir1.column_2_id,
			ir1.nm_column_2,
			ir1.nm_schema,
			ir1.nm_table,
			ir1.table_id,
			ir1.rows,
			ir2.nm_index		as nm_index_remanecente,
			ir2.index_id		as index_remanecent_id,
			ir2.index_nivel		as index_remanecente_nivel,
			ir2.type			as type_index_remanecente
	from	#indexes_3 ir1
	join	#indexes_3 ir2
	on		ir1.table_id = ir2.table_id
	and		ir1.column_id = ir2.column_id
	and		ir2.rank = 1
	where	ir1.contagem > 1
	and		ir1.rank > 1

	select	[type],
            nm_index,
            index_id,
            index_nivel,
            index_column_id,
            column_id,
            nm_column,
            index_column_2_id,
            column_2_id,
            nm_column_2,
            nm_schema,
            nm_table,
            table_id,
            [rows],
            nm_index_remanecente,
            index_remanecent_id,
            index_remanecente_nivel,
            type_index_remanecente
	from	#indexes_repetidos_3
	order	by nm_table,
			column_id

--------------------------------------------------------------------
	
--	salva os índices para deleta-los
	if object_id('tempdb..##buscar_indices', 'u') is not null
	begin
		drop table ##buscar_indices
	end
	create table ##buscar_indices
		(
			id					int identity(1, 1),
			nm_index			varchar(100),
			nm_schema			varchar(5),
			nm_table			varchar(100)
		)
	insert into ##buscar_indices
		(
			nm_index,
			nm_schema,
			nm_table
		)

	select	nm_index,
			nm_schema,
			nm_table
	from	#indexes_repetidos_1
	union	all
	select	nm_index,
			nm_schema,
			nm_table
	from	#indexes_repetidos_2
	union	all
	select	nm_index,
			nm_schema,
			nm_table
	from	#indexes_repetidos_3
