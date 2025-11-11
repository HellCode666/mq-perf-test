package com.example;

import com.ibm.mq.MQException;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Hashtable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

/**
 * Fast producer using IBM MQ native API (com.ibm.mq) from the allclient jar.
 * - Each thread opens its own MQQueueManager and MQQueue for isolation.
 * - Uses MQPutMessageOptions with NO_SYNCPOINT for best throughput (no transactions).
 * <p>
 * Configure via environment variables (same as Main):
 * MQ_HOST, MQ_PORT, MQ_CHANNEL, MQ_USER, MQ_PASSWORD
 * Optional: MQ_QMGR, MQ_QUEUE, MQ_THREADS, MQ_MSGS_PER_THREAD, MQ_MSG_SIZE
 */
public class FastProducerAllClient {

    public static void main(String[] args) throws Exception {
        String host = getenv("MQ_HOST", "localhost");
        int port = Integer.parseInt(getenv("MQ_PORT", "1616"));
        String channel = getenv("MQ_CHANNEL", "DIDIO");
        String user = getenv("MQ_USER", "mqexplorer");
        String password = getenv("MQ_PASSWORD", "");

        String qmgr = getenv("MQ_QMGR", "QMgr01");
        String queueName = getenv("MQ_QUEUE", "DIDIO.Q");
        int threads = Integer.parseInt(getenv("MQ_THREADS", "30"));
        int msgsPerThread = Integer.parseInt(getenv("MQ_MSGS_PER_THREAD", "10000"));
        int msgSize = Integer.parseInt(getenv("MQ_MSG_SIZE", "16"));

        System.out.printf("FastProducerAllClient connecting %s:%d channel=%s qmgr=%s queue=%s threads=%d msgs/thread=%d msgSize=%d\n",
                host, port, channel, qmgr, queueName, threads, msgsPerThread, msgSize);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        LongAdder total = new LongAdder();

        Instant startAll = Instant.now();

        final byte[] payload = new byte[msgSize];
        // fill deterministic payload (fast)
        for (int i = 0; i < msgSize; ++i) payload[i] = (byte) ('A' + (i % 26));

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            pool.submit(() -> {
                MQQueueManager qMgr = null;
                MQQueue queue = null;
                try {
                    Hashtable<String, Object> props = new Hashtable<>();
                    props.put(CMQC.HOST_NAME_PROPERTY, host);
                    props.put(CMQC.PORT_PROPERTY, port);
                    props.put(CMQC.CHANNEL_PROPERTY, channel);
                    props.put(CMQC.USER_ID_PROPERTY, user);
                    props.put(CMQC.PASSWORD_PROPERTY, password);
                    props.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES_CLIENT);

                    // create manager per thread
                    qMgr = new MQQueueManager(qmgr, props);

                    int openOptions = CMQC.MQOO_OUTPUT | CMQC.MQOO_FAIL_IF_QUIESCING;
                    queue = qMgr.accessQueue(queueName, openOptions);

                    MQPutMessageOptions pmo = new MQPutMessageOptions();
                    // NO_SYNCPOINT -> fastest (no transaction)
                    pmo.options = CMQC.MQPMO_NO_SYNCPOINT;

                    for (int i = 0; i < msgsPerThread; i++) {
                        MQMessage m = new MQMessage();
                        // write payload as UTF-8 string to be compatible with writeString API
                        m.writeString(new String(payload, StandardCharsets.UTF_8));
                        // lightweight properties
                        m.userId = user; // MQMessage.userId is a String
                        // send
                        queue.put(m, pmo);
                        total.increment();
                    }

                } catch (MQException | java.io.IOException e) {
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    try {
                        if (queue != null) queue.close();
                    } catch (Exception ignored) {
                    }
                    try {
                        if (qMgr != null) qMgr.disconnect();
                    } catch (Exception ignored) {
                    }
                    done.countDown();
                }
            });
        }

        done.await();
        pool.shutdown();

        Instant endAll = Instant.now();
        long sent = total.longValue();
        Duration d = Duration.between(startAll, endAll);
        double s = d.toNanos() / 1e9;
        System.out.printf("Sent %d messages in %.3f s => %.2f msg/s\n", sent, s, sent / s);
    }

    private static String getenv(String name, String def) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? def : v;
    }
}
