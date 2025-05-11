

Deleting redis keys:

```bash
redis-cli -a 'password' --scan --pattern "logstate:*" | xargs redis-cli del
```
How many entries in the loghit_queue?

```bash
redis-cli -a 'password' LLEN loghit_queue
(integer) 293969
```
```bash
redis-cli -a 'password' LRANGE loghit_queue 0 9
```
