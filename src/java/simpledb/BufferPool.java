package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    int UpperBoundNum;  /** The actual upperbound of page number  */
    Map<PageId, Page> pageMap;
    Map<PageId, Long> lastUsedTimeMap;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.UpperBoundNum = numPages;
        pageMap = new HashMap<PageId, Page>();
        lastUsedTimeMap = new HashMap<PageId, Long>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        Page page = pageMap.get(pid);
        if (page == null) {
            if (pageMap.size() >= UpperBoundNum) {
                evictPage();
//                throw new DbException("So many pages for buffer");
            }
            page = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
            pageMap.put(pid, page);
        }
        else {
            lastUsedTimeMap.remove(page.getId());
        }
        lastUsedTimeMap.put(pid, System.currentTimeMillis());

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> newPages = dbFile.insertTuple(tid, t);
        for (Page page : newPages) {
            page.markDirty(true, tid);
            this.pageMap.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        Page modifiedPage = dbFile.deleteTuple(tid, t);
        modifiedPage.markDirty(true, tid);
        pageMap.put(modifiedPage.getId(), modifiedPage);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        for (PageId pageId : pageMap.keySet()) {
            flushPage(pageId);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
        Page page = pageMap.get(pid);
        TransactionId tid = page.isDirty();
        if (tid != null) {
            DbFile dbFile = Database.getCatalog().getDbFile(page.getId().getTableId());
            dbFile.writePage(page);
            page.markDirty(false, tid);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        for (Page page : pageMap.values()) {
            if (tid.equals(page.isDirty())) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
        Long oldestTime = Long.MAX_VALUE;
        PageId LRUPageId = null;
        for (PageId pageId : lastUsedTimeMap.keySet()) {
            Long time = lastUsedTimeMap.get(pageId);
            if (time < oldestTime) {
                oldestTime = time;
                LRUPageId = pageId;
            }
        }

        pageMap.remove(LRUPageId);
        lastUsedTimeMap.remove(LRUPageId);
    }

//    class HeapNode implements Comparable<HeapNode> {
//        private PageId pageId;
//        private Long lastUsedTime;
//
//        public HeapNode(PageId pageId, Long lastUsedTime) {
//            this.pageId = pageId;
//            this.lastUsedTime = lastUsedTime;
//        }
//
//        public PageId getPageId() {
//            return pageId;
//        }
//
//        public Long getLastUsedTime() {
//            return lastUsedTime;
//        }
//
//        /**
//         * Compares this object with the specified object for order.  Returns a
//         * negative integer, zero, or a positive integer as this object is less
//         * than, equal to, or greater than the specified object.
//         * <p/>
//         * <p>The implementor must ensure <tt>sgn(x.compareTo(y)) ==
//         * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>.  (This
//         * implies that <tt>x.compareTo(y)</tt> must throw an exception iff
//         * <tt>y.compareTo(x)</tt> throws an exception.)
//         * <p/>
//         * <p>The implementor must also ensure that the relation is transitive:
//         * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
//         * <tt>x.compareTo(z)&gt;0</tt>.
//         * <p/>
//         * <p>Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt>
//         * implies that <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for
//         * all <tt>z</tt>.
//         * <p/>
//         * <p>It is strongly recommended, but <i>not</i> strictly required that
//         * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>.  Generally speaking, any
//         * class that implements the <tt>Comparable</tt> interface and violates
//         * this condition should clearly indicate this fact.  The recommended
//         * language is "Note: this class has a natural ordering that is
//         * inconsistent with equals."
//         * <p/>
//         * <p>In the foregoing description, the notation
//         * <tt>sgn(</tt><i>expression</i><tt>)</tt> designates the mathematical
//         * <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
//         * <tt>0</tt>, or <tt>1</tt> according to whether the value of
//         * <i>expression</i> is negative, zero or positive.
//         *
//         * @param o the object to be compared.
//         * @return a negative integer, zero, or a positive integer as this object
//         * is less than, equal to, or greater than the specified object.
//         * @throws NullPointerException if the specified object is null
//         * @throws ClassCastException   if the specified object's type prevents it
//         *                              from being compared to this object.
//         */
//        @Override
//        public int compareTo(HeapNode o) {
//            return (int) (this.getLastUsedTime() - o.getLastUsedTime());
//        }
//    }
}
