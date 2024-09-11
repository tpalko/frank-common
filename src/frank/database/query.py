

class Query:

    joins = []

    def __init__(self, *args, **kwargs):
        for k in kwargs:
            if k == 'join':
                self.joins.append(kwargs[k])

    @classmethod
    def all(cls, **kwargs):        
        return Query()
    
    @classmethod
    def get(cls, **kwargs):
        return Query()
    
    @classmethod
    def join(cls, base_class, **kwargs):
        return Query(join={**kwargs, 'base': base_class})
        
    def all(self, **kwargs):        
        return self.joins
        
    def get(self, **kwargs):
        return self.joins
        
    def join(self, base_class, **kwargs):
        self.joins.append({**kwargs, 'base': base_class})
        return self 
        