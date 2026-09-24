# LogisticsHub PoC
This application is a component of the LogisticsHub PoC.<br/>
To learn how this app fits into the whole system visit [logisticshub-poc](https://github.com/Se-Ku/logisticshub-poc)

# Order fulfillment service
This application mimics a resource-intensive processing of an order.
For the purpose of PoC, it fakes creating a PDF file containing a shipping label.<br/>

Submitted orders are dispatched to be processed asynchronously.
Once fulfilled, a message will be sent to the backend (which could then notify the user).

The app does not connect to any other service or database. It only works by exchanging messages with the backend.

# Implementation overview
This is an exercise with TypeScript, Node.js, Redis and RabbitMQ.

Libraries used:
- ioredis (https://github.com/redis/ioredis)
- amqplib (https://github.com/amqp-node/amqplib)

## Handling incoming messages (orders to process)
The app connects to a RabbitMQ queue and awaits OrderDispatched messages.<br/>
Once a message arrives, a link to a PDF label generated based on the order's ID and shipping data (obtained from the message).<br/>

To ensure idempotency, a Redis cache key containing order's ID is set. 
If it already exists, the message is considered a duplicate and ignored (not requeued).

If the message is unreadable, or readable but unprocessable, it is rejected (to a DLX queue).
If there's an unexpected error during processing (i.e. worker dies), the message is requeued.

When processing succeeds, a OrderFulfilled message is sent to the backend, with generated label.  

## Service scaling
The fulfilment microservice is designed to run in multiple instances (to scale with actual traffic).<br/>
All instances share state through a Redis cache, so no order is processed more than once (or concurrently).

# How to run
The app requires an already set up RabbitMQ and Redis.<br/>
See how the RabbitMQ and Redis are provisioned in the showcase repository [logisticshub-poc](https://github.com/Se-Ku/logisticshub-poc)

Two env variables are required to be set:
- RABBITMQ_URL=’amqp://user:pass@localhost:5672’
- REDIS_URL=’redis://user:pass@localhost:6379'

Once built, run with `node dist/index.js`. Or in dev mode with `npm run dev`.
