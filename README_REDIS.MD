

Deleting redis keys:

```bash
redis-cli -a 'password' --scan --pattern "logstate:*" | xargs redis-cli  -a 'password' del
```
What about a queue?

```bash
rediscli -a 'password' DEL loghit_queue
```

How many entries in the loghit_queue?

```bash
redis-cli -a 'password' LLEN loghit_queue
(integer) 293969
```
```bash
redis-cli -a 'password' LRANGE loghit_queue 0 9
```

Output:

 1) "{\"host\": \"10.0.0.153\", \"line_num\": 53147, \"raw\": \"2025-05-11T14:37:49+12:00\\t192.168.255.42\\tGET\\t/client?client=207&site=189\\tclient=207&site=189\\t200\\tMozilla/5.0 (iPhone; CPU iPhone OS 17_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/136.0.7103.56 Mobile/15E148 Safari/604.1\\thttps://insure.bizcover.co.nz/\\t161.29.97.70\"}"
 2) "{\"host\": \"10.0.0.153\", \"line_num\": 53146, \"raw\": \"2025-05-11T14:37:48+12:00\\t192.168.255.42\\tGET\\t/response?c=281&s=311\\tc=281&s=311\\t200\\tMozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36\\thttps://www.avis.co.nz/\\t115.188.85.100\"}"
 3) "{\"host\": \"10.0.0.153\", \"line_num\": 53145, \"raw\": \"2025-05-11T14:37:47+12:00\\t192.168.255.42\\tGET\\t/response?c=281&s=311\\tc=281&s=311\\t200\\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.111 Safari/537.36\\thttps://www.avis.co.nz/\\t118.93.233.77\"}"
 4) "{\"host\": \"10.0.0.153\", \"line_num\": 53144, \"raw\": \"2025-05-11T14:37:47+12:00\\t192.168.255.42\\tGET\\t/client?client=262&site=257%27%20style=%27width:%201px;%20height:%201px;%20border:%20none;%20position:%20absolute;%20top:%20-9999px;%20left:%20-9999px;&gtmcb=256942708\\tclient=262&site=257%27%20style=%27width:%201px;%20height:%201px;%20border:%20none;%20position:%20absolute;%20top:%20-9999px;%20left:%20-9999px;&gtmcb=256942708\\t200\\tMozilla/5.0 (iPhone; CPU iPhone OS 18_4_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/136.0.7103.91 Mobile/15E148 Safari/604.1\\thttps://www.bigsave.co.nz/\\t49.224.102.14\"}"
 5) "{\"host\": \"10.0.0.153\", \"line_num\": 53143, \"raw\": \"2025-05-11T14:37:46+12:00\\t192.168.255.42\\tGET\\t/health_check\\t-\\t200\\t-\\t-\\t-\"}"
 6) "{\"host\": \"10.0.0.153\", \"line_num\": 53142, \"raw\": \"2025-05-11T14:37:46+12:00\\t192.168.255.42\\tGET\\t/client?client=220&site=227&gtmcb=1361247287\\tclient=220&site=227&gtmcb=1361247287\\t200\\tMozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36\\t-\\t121.99.73.74\"}"
 7) "{\"host\": \"10.0.0.153\", \"line_num\": 53141, \"raw\": \"2025-05-11T14:37:45+12:00\\t192.168.255.42\\tGET\\t/response?c=281&s=330\\tc=281&s=330\\t200\\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36\\thttps://www.budget.co.nz/\\t68.197.136.181\"}"
 8) "{\"host\": \"10.0.0.153\", \"line_num\": 53140, \"raw\": \"2025-05-11T14:37:44+12:00\\t192.168.255.42\\tGET\\t/response?c=281&s=311\\tc=281&s=311\\t200\\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36\\thttps://www.avis.co.nz/\\t118.92.203.240\"}"
 9) "{\"host\": \"10.0.0.153\", \"line_num\": 53139, \"raw\": \"2025-05-11T14:37:44+12:00\\t192.168.255.42\\tGET\\t/client?client=262&site=257%27%20style=%27width:%201px;%20height:%201px;%20border:%20none;%20position:%20absolute;%20top:%20-9999px;%20left:%20-9999px;&gtmcb=1141223733\\tclient=262&site=257%27%20style=%27width:%201px;%20height:%201px;%20border:%20none;%20position:%20absolute;%20top:%20-9999px;%20left:%20-9999px;&gtmcb=1141223733\\t200\\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36\\thttps://www.bigsave.co.nz/\\t103.63.95.208\"}"
10) "{\"host\": \"10.0.0.153\", \"line_num\": 53138, \"raw\": \"2025-05-11T14:37:44+12:00\\t192.168.255.42\\tGET\\t/client?client=262&site=257%27%20style=%27width:%201px;%20height:%201px;%20border:%20none;%20position:%20absolute;%20top:%20-9999px;%20left:%20-9999px;&gtmcb=185240405\\tclient=262&site=257%27%20style=%27width:%201px;%20height:%201px;%20border:%20none;%20position:%20absolute;%20top:%20-9999px;%20left:%20-9999px;&gtmcb=185240405\\t200\\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36\\thttps://www.bigsave.co.nz/\\t14.137.23.25\"}"
