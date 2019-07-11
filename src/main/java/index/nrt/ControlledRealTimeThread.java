package index.nrt;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Utility class that runs a thread to manage periodicc
 * reopens of a {@link ReferenceManager}, with methods to wait for a specific
 * index changes to become visible.  When a given search request needs to see a specific
 * index change, call the {#waitForGeneration} to wait for
 * that change to be visible.  Note that this will only
 * scale well if most searches do not need to wait for a
 * specific index generation.
 * <p>
 * lucene.experimental
 * {@link org.apache.lucene.search.ControlledRealTimeReopenThread}
 */
public class ControlledRealTimeThread extends Thread implements Closeable {

    private final SearcherManager manager;
    private final SearcherLifetimeManager searcherLifetimeManager;
    private final long targetMaxStaleNS;
    private final double targetMaxStaleS;
    private final long targetMinStaleNS;
    private final IndexWriter writer;
    private volatile boolean finish;
    private volatile long waitingGen;
    private volatile long searchingGen;
    private long refreshStartGen;

    private final ReentrantLock reopenLock = new ReentrantLock();
    private final Condition reopenCond = reopenLock.newCondition();

    /**
     * Create ControlledRealTimeReopenThread, to periodically
     * reopen the a {@link ReferenceManager}.
     *
     * @param targetMaxStaleSec Maximum time until a new
     *                          reader must be opened; this sets the upper bound
     *                          on how slowly reopens may occur, when no
     *                          caller is waiting for a specific generation to
     *                          become visible.
     * @param targetMinStaleSec Mininum time until a new
     *                          reader can be opened; this sets the lower bound
     *                          on how quickly reopens may occur, when a caller
     *                          is waiting for a specific generation to
     *                          become visible.
     */
    public ControlledRealTimeThread(IndexWriter writer, SearcherManager manager, SearcherLifetimeManager searcherLifetimeManager,
                             double targetMaxStaleSec, double targetMinStaleSec) {
        if (targetMaxStaleSec < targetMinStaleSec) {
            throw new IllegalArgumentException("targetMaxScaleSec (= " + targetMaxStaleSec + ") < targetMinStaleSec (=" + targetMinStaleSec + ")");
        }
        this.writer = writer;
        this.manager = manager;
        this.searcherLifetimeManager = searcherLifetimeManager;
        this.targetMaxStaleS = targetMaxStaleSec;
        long _targetMaxStaleNS = (long) (1000000000 * targetMaxStaleSec);
        if (_targetMaxStaleNS > 0) this.targetMaxStaleNS = _targetMaxStaleNS;
        else this.targetMaxStaleNS = 1800_000_000_000L;
        this.targetMinStaleNS = (long) (1000000000 * targetMinStaleSec);
        manager.addListener(new ControlledRealTimeThread.HandleRefresh());
    }

    private class HandleRefresh implements ReferenceManager.RefreshListener {
        @Override
        public void beforeRefresh() {
        }

        @Override
        public void afterRefresh(boolean didRefresh) throws IOException {
            refreshDone();
            if (searcherLifetimeManager != null)
                searcherLifetimeManager.prune(new SearcherLifetimeManager.PruneByAge(targetMaxStaleS));
        }
    }

    private synchronized void refreshDone() {
        searchingGen = refreshStartGen;
        notifyAll();
    }

    @Override
    public synchronized void close() {
        //System.out.println("NRT: set finish");

        finish = true;

        // So thread wakes up and notices it should finish:
        reopenLock.lock();
        try {
            reopenCond.signal();
        } finally {
            reopenLock.unlock();
        }

        try {
            join();
        } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
        }

        // Max it out so any waiting search threads will return:
        searchingGen = Long.MAX_VALUE;
        notifyAll();
    }

    /**
     * Waits for the target generation to become visible in
     * the searcher.
     * If the current searcher is older than the
     * target generation, this method will block
     * until the searcher is reopened, by another via
     * {@link ReferenceManager#maybeRefresh} or until the {@link ReferenceManager} is closed.
     *
     * @param targetGen the generation to wait for
     */
    public void waitForGeneration(long targetGen) throws InterruptedException {
        waitForGeneration(targetGen, -1);
    }

    /**
     * Waits for the target generation to become visible in
     * the searcher, up to a maximum specified milli-seconds.
     * If the current searcher is older than the target
     * generation, this method will block until the
     * searcher has been reopened by another thread via
     * {@link ReferenceManager#maybeRefresh}, the given waiting time has elapsed, or until
     * the {@link ReferenceManager} is closed.
     * <p>
     * NOTE: if the waiting time elapses before the requested target generation is
     * available the current {@link SearcherManager} is returned instead.
     *
     * @param targetGen the generation to wait for
     * @param maxMS     maximum milliseconds to wait, or -1 to wait indefinitely
     * @return true if the targetGeneration is now available,
     * or false if maxMS wait time was exceeded
     */
    public synchronized boolean waitForGeneration(long targetGen, int maxMS) throws InterruptedException {
        if (targetGen > searchingGen) {
            // Notify the reopen thread that the waitingGen has
            // changed, so it may wake up and realize it should
            // not sleep for much or any longer before reopening:
            reopenLock.lock();

            // Need to find waitingGen inside lock as it's used to determine
            // stale time
            waitingGen = Math.max(waitingGen, targetGen);

            try {
                reopenCond.signal();
            } finally {
                reopenLock.unlock();
            }

            long startMS = System.nanoTime() / 1000000;

            while (targetGen > searchingGen) {
                if (maxMS < 0) {
                    wait();
                } else {
                    long msLeft = (startMS + maxMS) - System.nanoTime() / 1000000;
                    if (msLeft <= 0) {
                        return false;
                    } else {
                        wait(msLeft);
                    }
                }
            }
        }

        return true;
    }

    @Override
    public void run() {
        // TODO: maybe use private thread ticktock timer, in
        // case clock shift messes up nanoTime?
        long lastReopenStartNS = System.nanoTime();

        //System.out.println("reopen: start");
        while (!finish) {

            // TODO: try to guestimate how long reopen might
            // take based on past data?

            // Loop until we've waiting long enough before the
            // next reopen:
            while (!finish) {

                // Need lock before finding out if has waiting
                reopenLock.lock();
                try {
                    // True if we have someone waiting for reopened searcher:
                    boolean hasWaiting = waitingGen > searchingGen;
                    final long nextReopenStartNS = lastReopenStartNS + (hasWaiting ? targetMinStaleNS : targetMaxStaleNS);

                    final long sleepNS = nextReopenStartNS - System.nanoTime();

                    if (sleepNS > 0) {
                        reopenCond.awaitNanos(sleepNS);
                    } else {
                        break;
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    reopenLock.unlock();
                }
            }

            if (finish) {
                break;
            }

            lastReopenStartNS = System.nanoTime();
            // Save the gen as of when we started the reopen; the
            // listener (HandleRefresh above) copies this to
            // searchingGen once the reopen completes:
            refreshStartGen = writer.getMaxCompletedSequenceNumber();
            try {
                manager.maybeRefreshBlocking();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }

    /**
     * Returns which {@code generation} the current searcher is guaranteed to include.
     */
    @SuppressWarnings("unused")
    public long getSearchingGen() {
        return searchingGen;
    }
}
