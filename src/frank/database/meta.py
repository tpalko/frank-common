
class BaseMeta:        

    table = None 
    joins = None
    identity_col = None
    built_in_cols = None
    user_cols = None
    insert_col_names = None
    select_col_names = None

    def __init__(self, *args, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

class InstanceMeta:

    identity_col = None 
    built_in_cols = None 
    user_cols = None 
    built_in_col_lookup = None 
    user_col_lookup = None 