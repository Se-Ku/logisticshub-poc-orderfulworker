import amqp from 'amqp-connection-manager';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { Redis } from 'ioredis';
import 'dotenv/config';

function getRequiredEnv(key: string): string {
    const value = process.env[key];
    if (!value) {
        console.error(`[FATAL] Missing required environment variable: ${key}`);
        process.exit(1);
    }
    return value;
}

// 1. Environment Configuration
const RABBITMQ_URL = getRequiredEnv('RABBITMQ_URL');
const REDIS_URL = getRequiredEnv('REDIS_URL');
const EXCHANGE_NAME = process.env.RABBITMQ_EXCHANGE_NAME || 'fulfillment.exchange';
const INBOUND_QUEUE = 'order.fulfillment.queue';
const OUTBOUND_ROUTING_KEY = 'order.fulfilled';

// 2. Clients Setup
console.log('Connecting to the redis server: ' + REDIS_URL);
const redis = new Redis(REDIS_URL, {
    maxRetriesPerRequest: 3, // Fails fast if offline instead of hanging indefinitely
    connectTimeout: 5000,
});
redis.on('connect', () => console.log('[Redis] Initiating connection...'));
redis.on('ready', () => console.log('[Redis] Connected and ready to receive commands.'));
redis.on('error', (err) => console.error('[Redis] Connection Error:', err.message));
redis.on('close', () => console.warn('[Redis] Connection closed.'));
redis.on('reconnecting', (time: number) => console.log(`[Redis] Reconnecting in ${time}ms...`));

const amqpConnection = amqp.connect([RABBITMQ_URL]);
amqpConnection.on('connect', ({ url }) => {
    console.log(`[AMQP] Successfully connected to: ${url}`);
});
amqpConnection.on('disconnect', ({ err }) => {
    console.error('[AMQP] Disconnected from broker:', err?.message || err);
});

amqpConnection.on('connectFailed', ({ err, url }) => {
    console.error(`[AMQP] Failed to connect to ${url}:`, err?.message || err);
});

// 3. Define AMQP Topology and Channel Setup
const channelWrapper = amqpConnection.createChannel({
    json: true,
    setup: async (channel: ConfirmChannel) => {
        await channel.checkExchange(EXCHANGE_NAME);
        await channel.checkQueue(INBOUND_QUEUE);
        await channel.prefetch(1); // For the purpose of demonstration, we process one message at a time.
        await channel.consume(INBOUND_QUEUE, (msg) => handleMessage(channel, msg));
    },
});

channelWrapper.on('connect', () => console.log('[AMQP Channel] Connected & setup completed.'));
channelWrapper.on('error', (err, { name }) => console.error(`[AMQP Channel] Setup error in "${name}":`, err));
channelWrapper.on('close', () => console.warn('[AMQP Channel] Closed.'));

// 4. Message Handler with Redis Idempotency Guard
async function handleMessage(channel: ConfirmChannel, msg: ConsumeMessage | null) {
    if (!msg) return;

    interface OrderPayload {
        orderId: number;
        clientEmail: string;
        shippingAddress: Record<string, unknown>;
        items: unknown[];
    }

    let payload: OrderPayload;

    try {
        payload = JSON.parse(msg.content.toString());
    } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        console.error('[Order Fulfillment Worker] Malformed JSON message received, rejecting to DLX:', errorMessage);
        return channel.nack(msg, false, false); // Route to DLX
    }

    const orderId = payload.orderId;
    const lockKey = `idempotency:order:${orderId}`;

    // Atomic Lock Acquisition (TTL: 5 minutes)
    const lockAcquired = await redis.set(lockKey, 'PROCESSING', 'EX', 300, 'NX');

    if (!lockAcquired) {
        console.warn(`[Order Fulfillment Worker] Duplicate message received for Order #${orderId}. Skipping.`);
        // Safely acknowledge and discard duplicate. 
        // We assume that it the consumer holding the lock fails, their message will be requeued.
        return channel.ack(msg); 
    }

    try {
        console.log(`[Order Fulfillment Worker] Processing label generation for Order #${orderId}...`);

        // Simulate work (PDF Generation / External API call)
        await new Promise((resolve) => setTimeout(resolve, 1500));

        const fulfilledPayload = {
            orderId: orderId,
            trackingNumber: `TRK-${Math.floor(100000 + Math.random() * 900000)}`,
            labelUrl: `https://storage.example.com/labels/order-${orderId}.pdf`,
            fulfilledAt: new Date().toISOString(),
        };

        // Publish OrderFulfilledEvent back to RabbitMQ
        await channelWrapper.publish(EXCHANGE_NAME, OUTBOUND_ROUTING_KEY, fulfilledPayload);

        // Update Redis record state (retain key for 24h to block late retries)
        await redis.set(lockKey, 'COMPLETED', 'EX', 86400);

        console.log(`[Order Fulfillment Worker] Order #${orderId} fulfilled. Sent event.`);
        channel.ack(msg);
    } catch (error) {
        console.error(`[Order Fulfillment Worker] Error processing Order #${orderId}:`, error);

        // Release Redis lock so retry mechanisms can execute clean attempts
        await redis.del(lockKey);

        // Reject message (requeue=false routes it to Dead Letter Exchange)
        channel.nack(msg, false, false);
    }
}