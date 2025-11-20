# pqueue

A simple job queue backed by PostgreSQL.

Uses a short-polling mechanism for fetching updates, making it compatible with connection poolers in transaction mode.
