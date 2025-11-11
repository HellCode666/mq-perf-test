package com.example;

import com.ibm.msg.client.wmq.WMQConstants;
import com.ibm.mq.jms.MQQueueConnectionFactory;

import javax.jms.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class Main {
    public static void main(String[] args) throws Exception {
        // Read connection info from environment variables. Required:
        //   MQ_HOST, MQ_PORT, MQ_CHANNEL, MQ_USER, MQ_PASSWORD
        // Optional:
        //   MQ_QMGR (default QMgr01), MQ_QUEUE (default TEST.QUEUE),
        //   MQ_THREADS (default 4), MQ_MSGS_PER_THREAD (default 10000), MQ_MSG_SIZE (default 256)

        java.util.function.Function<String, String> env = (name) -> {
            String v = System.getenv(name);
            return (v == null || v.isBlank()) ? null : v;
        };

        final String CONNECTION_HOST = "localhost";
        final String CONNECTION_PORT_STR = "1616";
        final String CONNECTION_CHANNEL = "DIDIO";
        final String CONNECTION_USER = "mqexplorer";
        final String CONNECTION_PASSWORD = "";
        final int CONNECTION_PORT;
        try {
            CONNECTION_PORT = Integer.parseInt(CONNECTION_PORT_STR);
        } catch (NumberFormatException nfe) {
            System.err.println("MQ_PORT must be a number: " + CONNECTION_PORT_STR);
            System.exit(2);
            return; // unreachable but keeps compiler happy
        }

        // optional parameters (from env, otherwise defaults)
        final String qmgr = env.apply("MQ_QMGR") != null ? env.apply("MQ_QMGR") : "QMgr01";
        final String queueName = env.apply("MQ_QUEUE") != null ? env.apply("MQ_QUEUE") : "DIDIO.Q";
        final int threads = env.apply("MQ_THREADS") != null ? Integer.parseInt(env.apply("MQ_THREADS")) : 30;
        final int msgsPerThread = env.apply("MQ_MSGS_PER_THREAD") != null ? Integer.parseInt(env.apply("MQ_MSGS_PER_THREAD")) : 10000;
        final int msgSize = env.apply("MQ_MSG_SIZE") != null ? Integer.parseInt(env.apply("MQ_MSG_SIZE")) : 256;

        System.out.printf("Connecting to %s:%d channel=%s qmgr=%s queue=%s threads=%d msgs/thread=%d msgSize=%d%n",
                CONNECTION_HOST, CONNECTION_PORT, CONNECTION_CHANNEL, qmgr, queueName, threads, msgsPerThread, msgSize);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        LongAdder totalSent = new LongAdder();

        // prepare payload buffer (reused)
        final byte[] payload = new byte[msgSize];
        String sample = "msg-";
        byte[] prefix = sample.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < payload.length; ++i) payload[i] = (byte) ('A' + (i % 26));

        Instant globalStart = Instant.now();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                Connection connection = null;
                Session session = null;
                MessageProducer producer = null;
                try {
                    // create factory per thread for best isolation
                    MQQueueConnectionFactory cf = new MQQueueConnectionFactory();
                    cf.setHostName(CONNECTION_HOST);
                    cf.setPort(CONNECTION_PORT);
                    cf.setQueueManager(qmgr);
                    cf.setChannel(CONNECTION_CHANNEL);
                    cf.setTransportType(WMQConstants.WMQ_CM_CLIENT);

                    connection = cf.createConnection(CONNECTION_USER, CONNECTION_PASSWORD);
                    // non-transacted, AUTO_ACK, faster for fire-and-forget
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    Queue queue = session.createQueue(queueName);
                    producer = session.createProducer(queue);
                    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); // speed: non-persistent
                    connection.start();

                    startLatch.await(); // sync start

                    for (int i = 0; i < msgsPerThread; i++) {
                        // small dynamic prefix to avoid identical messages if needed
                        System.arraycopy(prefix, 0, payload, 0, Math.min(prefix.length, payload.length));
                        BytesMessage msg = session.createBytesMessage();
                        msg.writeBytes(payload);
                        // optional properties for consumers
                        msg.setIntProperty("thread", threadId);
                        msg.setIntProperty("seq", i);

                        producer.send(msg);
                        totalSent.increment();
                    }
                } catch (Exception e) {
                    System.err.println("Producer error: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    try { if (producer != null) producer.close(); } catch (Exception ignored) {}
                    try { if (session != null) session.close(); } catch (Exception ignored) {}
                    try { if (connection != null) connection.close(); } catch (Exception ignored) {}
                    doneLatch.countDown();
                }
            });
        }

        // start all threads
        startLatch.countDown();
        Instant start = Instant.now();
        doneLatch.await();
        Instant end = Instant.now();
        pool.shutdown();

        long sent = totalSent.longValue();
        Duration elapsed = Duration.between(start, end);
        double seconds = elapsed.toNanos() / 1e9;
        System.out.printf("Sent %d messages in %.3f s => %.2f msg/s (avg)%n", sent, seconds, sent / seconds);
        System.out.printf("Total wall time: %s%n", Duration.between(globalStart, end));
    }
}