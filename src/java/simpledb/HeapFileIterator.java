package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Nengbao on 1/16/16.
 */
public class HeapFileIterator implements DbFileIterator {

    HeapFile heapFile;
    TransactionId tid;
    int curPageNo;
    HeapPage heapPage = null;
    Iterator<Tuple> iterator = null;
    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.curPageNo = 0;
    }

    /**
     * Opens the iterator
     *
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(heapFile.getId(), curPageNo), Permissions.READ_ONLY);
        iterator = heapPage.iterator();
    }

    /**
     * @return true if there are more tuples available.
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (iterator == null) {
            return false;
        }

        if (iterator.hasNext()) {
            return true;
        }
        else {
            curPageNo++;
            if (curPageNo >= heapFile.numPages()) {
                return false;
            }
            open();
            return iterator.hasNext();
        }
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        else {
            if (!iterator.hasNext()){
                curPageNo++;
                if (curPageNo >= heapFile.numPages()) {
                    throw new NoSuchElementException();
                }
                open();
            }
            return iterator.next();
        }
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Closes the iterator.
     */
    @Override
    public void close() {
        curPageNo = 0;
        heapPage = null;
        iterator = null;
    }
}
