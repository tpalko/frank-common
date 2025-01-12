from frank.database.model import BaseModel
from frank.database.column import StringColumn, IntColumn, JsonColumn, BoolColumn, FloatColumn

class TestieWidgets(BaseModel):
    name = StringColumn(size=50)
    counter = IntColumn()
    data = JsonColumn()
    maybe = BoolColumn()
    value = FloatColumn()