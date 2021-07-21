import sqlalchemy

engine = sqlalchemy.create_engine('mssql+pyodbc://localhost/Core?driver=SQL+Server+Native+Client+11.0')
meta = sqlalchemy.MetaData()
r = meta.reflect(bind=engine)

for table in meta.sorted_tables:
    table = table.name
    print(table)

    query = sqlalchemy.select(meta.tables[table]).limit(20)
    connection = engine.connect()
    result = connection.execute(query)

    keys = list(result.keys())
    values = list(result)

    with open(f'data/{table}.sql', "a+") as f:
        f.truncate(0)

        f.write(f'SET IDENTITY_INSERT {table} ON;\n')

        for val in values:
            row = (dict(zip(keys, val)))
            stmt = meta.tables[table].insert().values(row)

            try:
                f.write(str(stmt.compile(compile_kwargs={"literal_binds": True})))
            except UnicodeEncodeError:
                pass

            f.write(';\n')
