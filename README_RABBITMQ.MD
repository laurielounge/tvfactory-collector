# Rabbit MQ

It's used as a message broker to send messages between microservices.

## Is there anything in the queue?

```bash
rabbitmqctl list_queues --vhost infinitum_vhost
```

How do I delete a queue?
```bash
rabbitmqctl delete_queue webhits_queue --vhost infinitum_vhost
```