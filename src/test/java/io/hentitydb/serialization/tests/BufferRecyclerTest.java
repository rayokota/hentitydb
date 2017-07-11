package io.hentitydb.serialization.tests;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;


import static io.hentitydb.serialization.BufferRecycler.recycler;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BufferRecyclerTest {
    @Test
    public void allocatesNewBuffersWithAGivenCapacity() throws Exception {
        final byte[] bytes = recycler().allocate(100);

        assertTrue("new buffers always have enough capacity",
                bytes.length >= 100);
    }

    @Test
    public void reusesBuffersAfterTheyHaveBeenReleased() throws Exception {
        final byte[] first = recycler().allocate(100);
        first[0] = 29;
        recycler().release(first);

        final byte[] second = recycler().allocate(100);
        
        assertThat("buffers are re-used after being released",
                   second[0],
                   is((byte) 29));
    }

    @Test
    public void prefersLargerBuffers() throws Exception {
        final byte[] small = recycler().allocate(100);
        final byte[] large = recycler().allocate(100000);

        recycler().release(small);
        recycler().release(large);

        final byte[] preferred = recycler().allocate(100);
        assertThat("larger buffers, when released, are used in favor of smaller buffers",
                   preferred.length,
                   is(100000));
    }

    @Test
    public void doesNotShareBuffersBetweenThreads() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final List<Thread> threads = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            final byte id = (byte) i;
            
            threads.add(new Thread("BufferRecyclerTest-"+i) {
                @Override
                public void run() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        fail();
                    }

                    for (int j = 0; j < 100; j++) {
                        final byte[] buf = recycler().allocate(100);
                        buf[0] = id;
                        recycler().release(buf);

                        final byte[] otherBuf = recycler().allocate(100);
                        assertThat(otherBuf[0], is(id));
                        recycler().release(otherBuf);
                    }
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        latch.countDown();

        for (Thread thread : threads) {
            thread.join();
        }
    }
}
