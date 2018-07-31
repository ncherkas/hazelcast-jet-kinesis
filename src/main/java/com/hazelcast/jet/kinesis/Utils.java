package com.hazelcast.jet.kinesis;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class Utils {

    private Utils() {
    }

    /**
     * Sleeping interruptibly as oposite to Guava uninterruptibles. Restores back interrupted state in
     * case of {@code InterruptedException}. Re-throws {@code RuntimeException}.
     * @param duration duration
     * @param timeUnit time unit
     */
    public static void sleepInterruptibly(long duration, TimeUnit timeUnit) {
        try {
            checkNotNull(timeUnit).sleep(duration); // checkNotNull from Hazelcast source base !!!
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while sleeping", ex);
        }
    }

}
