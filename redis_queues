# redis_queues.py

"""This modules contains python objects that are useful for interacting with
redis-py, an interface for redis in python. For more information about
redis and redis-py, please refer here:

  - redis - https://redis.io/documentation
  - redis-py - https://redis-py.readthedocs.io/en/latest/

The purpose of this project is to standardize the conventions of the
different types of queues. For instance, adding a new item to a redis list
is done using the lpush() function, where adding a new item to a redis set
object is done using the sadd() function. This project will allow a user to
build queues using the prebuilt types and then simply calling the push()
method of that object.

Available classes:
- RedisQueue: A class that acts as the parent of the redis list classes.
- RedisListQueue: A class that acts as an interface to a redis list object.
- RedisSetQueue: A class that acts as an interface to a redis set object.
- RetryQueue: A class that manages timestamp-keyed redis list objects and a
    single redis set object.
- Queues: A registry for all Redis Objects. Acts as a singleton with the b

Available methods:
- get_queues: Returns a reference to the Queues object. It returns a global
    parameter _queues which is a reference to a Queues object. This allows
    the module to have Queues behave like a singleton.

Protected global variables:
- _queues: This is an instance of Queues. Use get_queues to call it. This
    allows for singleton behavior of Queues in this module.
"""

import sys
import time
import settings

class RedisQueue(object):
    """
    Base class for all of the queues. All queues are instantiated with a
    redis object, a redis pipeline object, and a string with the name of the
    queue.
    """
    def __init__(self, redis, pipe, name):
        """Intializes instance variables."""
        self.redis = redis
        self._pipe = pipe
        self.name = name

    def _src(self, pipe):
        """
        This method is called before all commands that directly interact
        with redis. Every function has an optional 'pipe' parameter that,
        if, true, will add the command to the redis pipe object.

        Redis pipes are useful for two primary reasons: they guarantee
        atomicity, and they allow the execution of multiple commands at the
        same time for faster performance (example: a for loop that adds
        multiple objects to the queue would best be served being piped,
        and executed once at the conclusion of the loop.

        Pipes are safe to use across multiple queues.

        """
        return self._pipe if pipe else self.redis

    def push(self, pipe=False):
        """ This function is used to add items to the queue."""
        raise NotImplementedError("Standard queue function")

    def pop(self, pipe=False):
        """ This function is used to remove items from the queue."""
        raise NotImplementedError("Standard queue function")

    def length(self, pipe=False):
        """ This function is used to get the number of items in the queue."""
        raise NotImplementedError("Standard queue function")

    def get_list(self, pipe=False):
        """ This function is used to show the items in the list."""

        # Depricated. Do not implement this going forward
        pass

    def items(self, pipe=False):
        """ This function is used to show the items in the list."""
        raise NotImplementedError("Standard queue function")

    # def clean(self, pipe=False):
    #     """ This item will"""
    #     return self._src(pipe).delete(self.name)


class RedisListQueue(RedisQueue):
    """
    This is an interface for using a redis list object as a queue.

    A redis list contains single values in an array data structure that is
    based on the direction it is inserted.

    Example:
        import redis
        r = redis.StrictRedis()
        r.lpush('test', 'a')
        >>> 1L
        r.lrange('test', 0, 10)
        >>> ['a']
        r.lpush('test', 'b')
        >>> 2L
        r.lrange('test', 0, 10)
        >>> ['b', 'a']
        r.rpush('test', 'b')
        >>> 3L
        r.lrange('test', 0, 10)
        >>> ['b', 'a', 'b']
    """

    def push(self, item, remove=None, pipe=False):
        """
        This function is used to add items to the queue. If from_active is
        true, the item will be atomically removed from the active queue and
        added to the queue.
        """
        if remove:
            queue, item = remove
            self._pipe.lrem(queue, 0, item)
            self._pipe.lpush(self.name, item)
            return self._pipe.execute()
        else:
            return self._src(pipe).lpush(self.name, item)

    def pop(self, to_active=False, from_active=None, pipe=False):
        """
        This function will remove an item from the queue. If
        to_active is true, the next item in the queue will be atomically
        added to the active queue.

        If from_active is true, the next item will be removed from the
        active queue and pushed to this queue.
        """
        if to_active:
            return self._src(pipe).rpoplpush(self.name, 'active')
        elif from_active: # cannot find usage for this. Perhaps when
                          # interacting with active queue directly?
            self._pipe.rpush(self.name, 0)
            self._pipe.rpoplpush('active', 0)
            return self._pipe.execute()
        else:
            return self._src(pipe).rpop(self.name)

    def length(self, pipe=False):
        """ This function gets the size of the queue."""
        return self._src(pipe).llen(self.name)

    def get_list(self, pipe=False):
        """ This function shows the items in the queue."""

        # Depricated, do now use
        return self._src(pipe).lrange(self.name, 0, sys.maxsize)

    # New feature. Included until get_list has been adjusted in code. Identical to 'get_list'
    def items(self, start=0, end=sys.maxsize, pipe=False):
        """ This function shows the items in the queue."""
        return self._src(pipe).lrange(self.name, start, end)

    def remove(self, item, pipe=False):
        """
        This item will remove up to 10 instances of the item in the
        queue.
        """
        return self._src(pipe).lrem(self.name, 10, item)


class RedisSetQueue(RedisQueue):
    """
    This is an interface for using a redis set object as a queue.

    A redis set is an object that contains single unique values.

    Example:
        r.sadd('test', 'b')
        >>> 1
        r.sadd('test', 'a')
        >>> 1
        r.smembers('test')
        >>> set(['a', 'b'])
        r.sadd('test', 'a')
        >>> 0
        r.smembers('test')
        >>> set(['a', 'b'])
    """

    def push(self, item, pipe=False):
        """ This function add an item to the queue."""
        return self._src(pipe).sadd(self.name, item)

    def pop(self, pipe=False):
        """ This function removes an item from the queue."""
        return self._src(pipe).spop(self.name)

    def length(self, pipe=False):
        """ This function returns the size of the queue."""
        return self._src(pipe).scard(self.name)

    def get_list(self, pipe=False):
        """ This function shows the size of the queue."""

        # Depricated. Do not use.
        return self._src(pipe).smembers(self.name)

    # New feature. Included until get_list has been adjusted in code. Identical to 'get_list'
    def items(self, pipe=False):
        """ This function shows the items in the queue."""
        return self._src(pipe).smembers(self.name)

    def is_member(self, item, pipe=False):
        """This function shows whether or not the item exists in the queue."""
        return self._src(pipe).sismember(self.name, item)

class RedisSortedSetQueue(RedisQueue):
    """
    This is an interface of a redis sorted set.
    A redis sorted set is an object that contains unique values, each with
    an associated score. The object sorts the values by score.

    Example:
        import redis
        r = redis.StrictRedis()
        > True
        r.zadd('test', 3, 'a')
        > 1  # returns 1 if not already in set, and 0 otherwise
        r.zadd('test', 2, 'b')
        > 1
        r.zadd('test', 1, 'b')
        > 0  # returned 0 because 'b' is in the set already
        r.zrange('test', 0, 100)
        > ['b', 'a']  # b has a score of 2, and a a score of 3
    """

    def push(self, item, pipe=False):
        """ This function adds an item to the queue."""
        return self._src(pipe).zadd(self.name, time(), item)

    def pop(self, item, pipe=False):
        """ This function removes a specific item from the queue."""
        return self._src(pipe).zrem(self.name, item)

    def length(self, pipe=False):
        """ This function shows the length of the queue."""
        return self._src(pipe).zcard(self.name)

    def items(self, min, max, pipe=False):
        """ This function shows the items of the queue."""
        return self._src(pipe).zrangebyscore(self.name, min, max)


class RetryQueue(RedisQueue):
    """
    This is a special case of a queue, as it manages mutiple redis objects.
    There is one redis set object that contains a list of timestamps,
    also referred to as buckets, and each bucket is a separate redis list
    object.

    Each of the functions below have an extra bucket argument, which is
    optional or mandatory depending on the function. The retry queue itself
    contains the list of all current timestamped buckets, while the buckets
    themselves are separate redis list queues managed by the retry queue
    structure.
    """

    def push(self, bucket, item, *, remove=None, pipe=False):
        """This function adds a value to the retry queue and the bucket.

        This function adds an item to a bucket, as well as the bucket to the
        set queue. Since the bucket registry is a set, repeatedly adding the
        same bucket will ignore all entries past the first.

        Args:
            bucket: A timestamp value denoting the bucket to insert into.
            item: The item to be inserted into the bucket.
            from_active: A boolean as to whether or not to pop from the
                active queue.
            pipe: Whether we should pipe the commands in this function or not.

        Returns:
            A list with the output of all piped commands.

        Raises:
            ResponseError: An operation was executed on a redis object of the
                wrong type
        """
        if remove:
            queue, item = remove
            self._pipe.lrem('active', 0, item)
            bucket = 0
        self._pipe.sadd(self.name, bucket)
        self._pipe.lpush(bucket, item)
        return self._pipe.execute()

    def pop(self, bucket, *, to_active=False, pipe=False):
        """This function removes the next item in the corresponding bucket.

        Args:
            bucket: A timestamp value denoting the bucket to pop the next
                item from.
            to_active: A boolean as to whether or not to pop from the active
                queue.
            pipe: A boolean denoting whether we should pipe the commands in
            this function or not.

        Returns:
            The value popped from the bucket queue.

        Raises:
            ResponseError: As operation was executed on a redis object of
                the wrong type.
        """
        if to_active:
            return self._src(pipe).rpoplpush(bucket, 'active')
        else:
            return self._src(pipe).rpop(bucket)

    def length(self, *, bucket=None, pipe=False):
        """This function returns the length of either the queue or a
        specific bucket, depending on whether or not a bucket value is
        passed.

        Args:
            bucket: A timestamp value denoting the bucket to get the length
                for.
            pipe: A boolean denoting whether we should pipe the commands in
                this function or not.

        Returns:
            The number of buckets in the retry queue if bucket is None or
            the length of the passed bucket.

        Raises:
            ResponseError: As operation was executed on a redis object of
                the wrong type.
        """
        if bucket is not None:
            return self._src(pipe).llen(bucket)
        else:
            return self._src(pipe).scard(self.name)

    def get_list(self, *, bucket=None, start=0, end=sys.maxsize, pipe=False):
        """This function returns bucket values in the retry queue or
        the contents of an individual bucket. The default is to grab all of
        the contents from the specified container. Redis safely accepts

        Args:
            start: This is the first element to get from the queue. The
                default is 0, which is always the first element.
            end: This is the last element to get from the queue. The default
                is sys.maxsize, which we use to represent an arbitrarily
                large number.
            bucket: A timestamp value denoting the bucket to get the length
                for.
            pipe: A boolean denoting whether we should pipe the commands in
                this function or not.

        Returns:
            The contents in the respective queue.

        Raises:
            ResponseError: As operation was executed on a redis object of
                the wrong type.
        """
        if bucket is not None:
            return self._src(pipe).lrange(bucket, start, end)
        else:
            return list(self._src(pipe).smembers(self.name))

    def items(self, *, bucket=None, start=0, end=sys.maxsize, pipe=False):
        """This function returns bucket values in the retry queue or
        the contents of an individual bucket. The default is to grab all of
        the contents from the specified container. Redis safely accepts

        Args:
            start: This is the first element to get from the queue. The
                default is 0, which is always the first element.
            end: This is the last element to get from the queue. The default
                is sys.maxsize, which we use to represent an arbitrarily
                large number.
            bucket: A timestamp value denoting the bucket to get the length
                for.
            pipe: A boolean denoting whether we should pipe the commands in
                this function or not.

        Returns:
            The contents in the respective queue.

        Raises:
            ResponseError: As operation was executed on a redis object of
                the wrong type.
        """
        if bucket is not None:
            return self._src(pipe).lrange(bucket, start, end)
        else:
            return list(self._src(pipe).smembers(self.name))


    def remove(self, bucket, pipe=False):
        """This function removes the bucket from the retry queue's set
        object.

        Args:
            bucket: A timestamp value for denoting the bucket to remove.
            pipe: A boolean denoting whether we should pipe the commands in
                this function or not.

        Returns:
            If the object exists and is removed, 1 is returned. Otherwise, 
                0 is returned.
                
        Raises:
            ResponseError: As operation was executed on a redis object of
                the wrong type.
        """
        return self._src(pipe).srem(self.name, 0, bucket)

    def clean(self, pipe=False):
        """This function removes the entire retry qeueue from redis.
        
        Args:
            pipe: A boolean denoting whether we should pipe the commands in
                this function or not.
                
        Returns:
            Nothing.
        
        Raises:
            ResponseError: As operation was executed on a redis object of
                the wrong type.
        """
        self._src(pipe).delete(self.name)
        for bucket in self.get_list():
            self._src(pipe).delete(bucket)

class Queues(dict):
    """This class serves as a manager for all of the queues used throughout
    the project. To add a new queue to a given queues object, call any
    method associated with the desired type and pass the method a name. For
    instance, Queues.new_list('test') will add a redis list object with the
    name 'test' to the queues object.

    This class inherits the dict class and points self.__dict__ to itself.
    The reason for doing so is as follows: the initial build of Queues was
    simply to manage each of the individual redis objects inside of a dict.
    I use iPython (Jupyter, specifically) for developing. It felt unnatural
    to repeatedly type queues['test'].items() etc. as opposed to
    queues.test.items(). I had already implemented the dict lookup syntax
    throughout the project, and rather than go in and correct it, I simply
    made it backwards compatible. That is, queues['test'].items() and
    queues.test.items() will do exactly the same thing.
    """
    def __init__(self, redis):
        super().__init__()
        self.__dict__ = self
        self.redis = redis
        self._pipe = self.redis.pipeline()


    def new_list(self, name):
        """This function will add a new RedisListQueue object to this class
        object.

        Args:
            name: The name used to reference the RedisListObject.

        Returns:
            Nothing.

        Raises:
            Nothing.
        """
        if not self.get(name):
            self.update({
                name: RedisListQueue(self.redis, self._pipe, name)
            })

    def new_set(self, name):
        """This function will add a new RedisSetQueue object to this class
        object.

        Args:
            name: The name used to reference the RedisSetObject.

        Returns:
            Nothing.

        Raises:
            Nothing.
        """
        if not self.get(name):
            self.update({
                name: RedisSetQueue(self.redis, self._pipe, name)
            })

    def new_sorted_set(self, name):
        """This function will add a new RedisSortedSetQueue object to this
        class
        object.

        Args:
            name: The name used to reference the RedisSortedSetObject.

        Returns:
            Nothing.

        Raises:
            Nothing.
        """
        if not self.get(name):
            self.update({
                name: RedisSortedSetQueue(self.redis, self._pipe, name)
            })

    def new_retry(self, name):
        """This function will add a new RedisRetryQueue object to this class
        object.

        Args:
            name: The name used to reference the RedisRetryObject.

        Returns:
            Nothing.

        Raises:
            Nothing.
        """
        if not self.get(name):
            self.update({
                name: RetryQueue(self.redis, self._pipe, name)
            })

    def execute(self):
        """
        This function is used to execute the pipe associated with this
        queue.

        Args:
            Nothing.

        Returns:
            A list containing the output for all of the commands in the
            executed pipeline.

        Raises:
            Nothing.
        """
        return self._pipe.execute()
