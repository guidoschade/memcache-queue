# memcache-queue
Simple message queue for memcached - written in C++

This creates a message queue within memcached. Access is locked, so a queue can't be manipulated by more than one task at the same time. It works well however if there are too many messages and the latency is high, locking can cause problems as access is sequential.
