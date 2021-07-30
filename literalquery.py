from datetime import datetime
from sqlalchemy.dialects.mssql import MONEY, DATE, BINARY, VARBINARY, DATETIME, DATETIME2, TIME, UNIQUEIDENTIFIER,\
    IMAGE, TIMESTAMP, SQL_VARIANT
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType, DECIMAL
import sqlalchemy.orm


text = str
int_type = int
str_type = str
bytes_type = bytes
BINARY_type = BINARY
VARBINARY_type = VARBINARY
DECIMAL_type = DECIMAL
MONEY_type = MONEY


class StringLiteral(String):
    """Teach SA how to literalize various things."""
    def literal_processor(self, dialect):
        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, int_type):
                return text(value)
            if isinstance(value, MONEY_type):
                return float(value)
            if isinstance(value, bytes_type):
                #return hex(int(value.hex(), b))
                return '0x' + value.hex()
            if not isinstance(value, str_type):
                value = text(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = value.hex() #.decode(dialect.encoding)
            return result
        return process


class LiteralDialect(DefaultDialect):
    colspecs = {
        String: StringLiteral,
        DateTime: StringLiteral,
        datetime: StringLiteral,
        NullType: StringLiteral,
        DECIMAL: StringLiteral,
        MONEY: StringLiteral,
        DATE: StringLiteral,
        BINARY: StringLiteral,
        DATETIME: StringLiteral,
        DATETIME2: StringLiteral,
        TIME: StringLiteral,
        UNIQUEIDENTIFIER: StringLiteral,
        VARBINARY: StringLiteral,
        IMAGE: StringLiteral,
        TIMESTAMP: StringLiteral,
        SQL_VARIANT: StringLiteral,
    }


def literalquery(statement):
    """NOTE: This is entirely insecure. DO NOT execute the resulting strings."""
    if isinstance(statement, sqlalchemy.orm.Query):
        statement = statement.statement
    return statement.compile(
        dialect=LiteralDialect(),
        compile_kwargs={'literal_binds': True},
    ).string
