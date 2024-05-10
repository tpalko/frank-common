class FranKache(object):

    __types = None 

    def __init__(self, *args, **kwargs):
        if 'types' not in kwargs:
            raise Exception("Supply 'types' Enum")
        self.__types = kwargs['types']    

    @contextmanager
    def cache(self, read_only=True):
        
        contents = {}
        if not os.path.exists(self.cache_file):
            with open(self.cache_file, 'w') as f:
                # self.logger.debug(f'saving {contents}')
                f.write(json.dumps(contents, indent=4))
                
        with open(self.cache_file, 'r') as f:
            raw_contents = f.read()
            # self.logger.debug(raw_contents)
            contents = json.loads(raw_contents)
            yield contents 
        if not read_only:
            with open(self.cache_file, 'w') as f:
                f.write(json.dumps(contents, indent=4))

    def _get_cache_id(self, cache_type, target_name=None):

        cache_id = f'bucket-${self.bucket_name}'

        if cache_type == CacheType.RemoteStats:
            cache_id = f'remote-stats_{cache_id}'
        elif cache_type == CacheType.Archives:
            cache_id = f'archives_{cache_id}'
        
        if target_name:
            cache_id = f'{cache_id}_target-{target_name}'
        
        return cache_id 

    def _cache_store(self, cache_id, content):
        with self.cache(read_only=False) as contents:        
            # self.logger.debug(f'storing {content}')
            contents[cache_id] = { 'time': datetime.strftime(datetime.utcnow(), '%c'), 'content': content }            

    def _cache_fetch(self, cache_id):
        content = None 
        with self.cache() as contents:
            if cache_id in contents:
                content_record = contents[cache_id]
                if (datetime.utcnow() - datetime.strptime(content_record['time'], '%c')).total_seconds() < 900:
                    content = content_record['content']
        return content 

    def _cache_invalidate(self, target_name):
        with self.cache(read_only=False) as contents:
            for t in CacheType:
                del contents[self._get_cache_id(t, target_name)]