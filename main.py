# dump database tables data, limit each table by n rows.
# for each table create sql file, with queries: insert into table(c1, c2) values (v1, v2)

from pathlib import Path
from sqlalchemy import create_engine, MetaData, inspect

from literalquery import literalquery


N = 10  # limit rows
SERVER = 'localhost'
DATABASE = 'dwh'

sys_databases = ['master', 'tempdb', 'model', 'msdb']
sys_schemas = ['db_accessadmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_ddladmin',
               'db_denydatareader', 'db_denydatawriter', 'db_owner', 'db_securityadmin', 'guest',
               'INFORMATION_SCHEMA', 'sys']

engine = create_engine(f'mssql+pyodbc://{SERVER}/{DATABASE}?driver=SQL+Server+Native+Client+11.0',
                       connect_args={'use_unicode': False})
databases = engine.execute('SELECT name FROM sys.databases;').fetchall()

for db in databases:
    db = db.values()[0]
    if db in sys_databases:
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
            meta.reflect(bind=engine, schema=schema)

    for table in meta.sorted_tables:
        if table.name in ('sysdiagrams', 'dimCatalogCities'):
        #if table.name not in 'Bat_CfgRole':
            continue
        print(table)

        select_query = table.select().limit(N).with_hint(table, text='with (nolock)')
        result = connection.execute(select_query)

        keys = list(result.keys())
        values = list(result)

        with open(f'{DIR}/{table}.sql', "a+", encoding="utf-8") as f:
            f.truncate(0)  # clear file
            f.write(f'SET IDENTITY_INSERT {table} ON;\n')

            for val in values:
                row = dict(zip(keys, val))
                insert_query = table.insert().values(row)
                # write insert statements
                # f.write(str(insert_query.compile(compile_kwargs={"literal_binds": True, "render_postcompile": True})))
                f.write(str(literalquery(insert_query)))

                f.write(';\n')

    connection.close()
