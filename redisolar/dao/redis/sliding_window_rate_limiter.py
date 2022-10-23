# Uncomment for Challenge #7
# import datetime
# import random
import datetime

from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase, RateLimitExceededException
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema


# Uncomment for Challenge #7
# from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""

    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        hit_key = self.key_schema.sliding_window_rate_limiter_key(name, int(self.window_size_ms), self.max_hits)
        timestamp_seconds = datetime.datetime.utcnow().timestamp() * 1000

        pipeline = self.redis.pipeline(transaction=True)
        pipeline.zadd(hit_key, {f"{timestamp_seconds}-IP-PLACEHOLDER": timestamp_seconds})
        pipeline.zremrangebyscore(hit_key, 0, timestamp_seconds - self.window_size_ms)
        pipeline.zcard(hit_key)
        _, __, hits = pipeline.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException()
        # END Challenge #7
