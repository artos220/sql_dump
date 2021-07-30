# dump database tables data, limit each table by n rows.
# for each table create sql file, with queries: insert into table(c1, c2) values (v1, v2)

from pathlib import Path
from sqlalchemy import create_engine, MetaData, inspect

from literalquery import literalquery


N = 10  # limit rows
SERVER = 'biload'
DATABASE = 'dwh'

sys_databases = ['master', 'tempdb', 'model', 'msdb', 'SCB', 'SCB_2', 'querymon']
sys_schemas = ['db_accessadmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_ddladmin',
               'db_denydatareader', 'db_denydatawriter', 'db_owner', 'db_securityadmin', 'guest',
               'INFORMATION_SCHEMA', 'sys']

engine = create_engine(f'mssql+pyodbc://{SERVER}/{DATABASE}?driver=SQL+Server+Native+Client+11.0',
                       connect_args={'use_unicode': False})
databases = engine.execute('SELECT name FROM sys.databases;').fetchall()

for db in databases:
    db = db.values()[0]

    if db in sys_databases:
    #if db not in 'Forecasts':
        continue

    print(db)

    DIR = f'data/{db}'
    Path(DIR).mkdir(exist_ok=True)

    engine = create_engine(f'mssql+pyodbc://{SERVER}/{db}?driver=SQL+Server+Native+Client+11.0',
                           connect_args={'use_unicode': False})

    meta = MetaData()
    connection = engine.connect()

    insp = inspect(engine)
    sch_list = insp.get_schema_names()

    for schema in sch_list:
        if schema not in sys_schemas:
            adapt_schema = str(schema)
            if adapt_schema.find("\\") > 0\
                    or adapt_schema.find(".") > 0:
                adapt_schema = f'[{adapt_schema}]'
            meta.reflect(bind=engine, schema=adapt_schema)

    for table in meta.sorted_tables:
        if table.name in ('sysdiagrams',
                          'dimCatalogCities',
                          'execution_parameter_values',
                          'object_parameters',
                          'event_message_context',
                          'executable_statistics',
                          'Report_1_Regions_Polygons',
                          'Report_1_Users',
                          'Report_1_WH_Coordinates',
                          'Report_1_WH_Coordinates_part2',
                          '_index_defrag',
                          '_index_defrag_stage',
                          ):
        #if table.name not in 'Bat_CfgRole':
            continue
        print(table)

        select_query = table.select().limit(N).with_hint(table, text='with (nolock)')
        result = connection.execute(select_query)

        keys = list(result.keys())
        values = list(result)

        script_name = str(table).replace("\\", "")

        with open(f'{DIR}/{script_name}.sql', "a+", encoding="utf-8") as f:
            f.truncate(0)  # clear file
            f.write(f'USE {db};\n')
            f.write(f'SET IDENTITY_INSERT {table} ON;\n')

            for val in values:
                row = dict(zip(keys, val))
                insert_query = table.insert().values(row)
                # write insert statements
                # f.write(str(insert_query.compile(compile_kwargs={"literal_binds": True, "render_postcompile": True})))
                f.write(str(literalquery(insert_query)))

                f.write(';\n')
                f.write(f'SET IDENTITY_INSERT {table} OFF;\n')

    connection.close()
