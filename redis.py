import redis

r_server = redis.Redis('localhost') #this line creates a new Redis object and
                                    #connects to our redis server
r_server.set('test_key', 'test_value') #with the created redis object we can
                                        #submits redis commands as its methods
                                        
print 'previous set key ' + r_server.get('test_key') # the previous set key is fetched

r_server.set('counter', 1) #set an integer to a key
r_server.incr('counter') #we increase the key value by 1, has to be int
print 'the counter was increased! '+ r_server.get('counter') #notice that the key is increased now

r_server.decr('counter') #we decrease the key value by 1, has to be int
print 'the counter was decreased! '+ r_server.get('counter') #the key is back to normal

r_server.rpush('list1', 'element1') #we use list1 as a list and push element1 as its element

r_server.rpush('list1', 'element2') #assign another element to our list
r_server.rpush('list2', 'element3') #the same

print 'our redis list len is: %s'% r_server.llen('list1') #with llen we get our redis list size right from redis

print 'at pos 1 of our list is: %s'% r_server.lindex('list1', 1) #with lindex we query redis to tell us which element is at pos 1 of our list

'''sets perform identically to the built in Python set type. Simply, sets are lists but, can only have unique values.'''

r_server.sadd("set1", "el1")
r_server.sadd("set1", "el2")
r_server.sadd("set1", "el2")

print 'the member of our set are: %s'% r_server.smembers("set1")

from contextlib import contextmanager


try:
    import hiredis
    HIREDIS_AVAILABLE = True
except ImportError:
    HIREDIS_AVAILABLE = False


def from_url(url, db=None, **kwargs):
    """
    Returns an active Redis client generated from the given database URL.
    Will attempt to extract the database id from the path url fragment, if
    none is provided.
    """
    from redis.client import Redis
    return Redis.from_url(url, db, **kwargs)


@contextmanager
def pipeline(redis_obj):
    p = redis_obj.pipeline()
    yield p
    p.execute()


class dummy(object):
    """
    Instances of this class can be used as an attribute container.
    """
    pass

def parse_object(response, infotype):
    "Parse the results of an OBJECT command"
    if infotype in ('idletime', 'refcount'):
        return int_or_none(response)
    return response


def parse_info(response):
    "Parse the result of Redis's INFO command into a Python dict"
    info = {}
    response = nativestr(response)

def get_value(value):
    if ',' not in value or '=' not in value:
         try:
         if '.' in value:
            return float(value)
         else:
            return int(value)
         except ValueError:
            return value
         else:
            sub_dict = {}
         for item in value.split(','):
            k, v = item.rsplit('=', 1)
            sub_dict[k] = get_value(v)
          return sub_dict

    for line in response.splitlines():
        if line and not line.startswith('#'):
            if line.find(':') != -1:
                key, value = line.split(':', 1)
                info[key] = get_value(value)
            else:
                # if the line isn't splittable, append it to the "__raw__" key
                info.setdefault('__raw__', []).append(line)

    return info

def parse_sentinel_master(response):
    return parse_sentinel_state(imap(nativestr, response))


def parse_sentinel_masters(response):
    result = {}
    for item in response:
        state = parse_sentinel_state(imap(nativestr, item))
        result[state['name']] = state
    return result


def parse_sentinel_slaves_and_sentinels(response):
    return [parse_sentinel_state(imap(nativestr, item)) for item in response]


def parse_sentinel_get_master(response):
    return response and (response[0], int(response[1])) or None


def pairs_to_dict(response):
    "Create a dict given a list of key/value pairs"
    it = iter(response)
    return dict(izip(it, it))



