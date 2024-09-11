from frank.database.model import BaseModel
from frank.database.column import StringColumn, IntColumn, JsonColumn, BoolColumn, FloatColumn

class TestClass(BaseModel):
    name = StringColumn()
    counter = IntColumn()
    data = JsonColumn()
    maybe = BoolColumn()
    value = FloatColumn()