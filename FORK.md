This fork adds the FailoverRedisAppender.

log4j.appender.redis=com.ryantenney.log4j.FailoverRedisAppender
log4j.appender.redis.endpoints=host0:6380,host1:6380,host2:6380,host3:6380

2013 (c) ernestmicklei.com