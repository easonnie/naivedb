package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {


    private File f;
    private TupleDesc td;
    //private int numPage;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
        byte[] pageData = new byte[BufferPool.PAGE_SIZE];
        Page page = null;
        try {
            RandomAccessFile accessor = new RandomAccessFile(f, "r");
            accessor.seek(offset);
            accessor.read(pageData, 0, BufferPool.PAGE_SIZE);
            page = new HeapPage((HeapPageId) pid, pageData);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //System.out.println("File length :" + f.length());
        return (int) Math.ceil(f.length() * 1.0 / BufferPool.PAGE_SIZE * 1.0);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}

class HeapFileIterator implements DbFileIterator {

    private HeapFile f;
    Iterator<Tuple> curIterator;    //curIterator of some page
    private int curPageNum;
    TransactionId tid;
    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.f = f;
        this.tid = tid;
        curPageNum = -1;
        curIterator = null;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        curPageNum = -1;
        curIterator = getNextIterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {

        if (curPageNum == -1) {
            return false;//Not open yet;
        }

        while ((curIterator == null || !curIterator.hasNext()) && curPageNum < f.numPages() - 1) {
            curIterator = getNextIterator();
        }

        if (curIterator == null && curPageNum >= f.numPages() - 1) {
            return false;
        }

        if (curIterator.hasNext()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return curIterator.next();
        } else {
            throw new NoSuchElementException("Can not iterate next tuple, no such element");
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        curPageNum = -1;
        curIterator = null;
    }

    //Get the iterator of the next page;
    private Iterator<Tuple> getNextIterator() throws TransactionAbortedException, DbException {
        //System.out.println("f.numPages:" + f.numPages());
        //System.out.println("curPageNum:" + curPageNum);
        if (curPageNum >= f.numPages() - 1) {
            throw new DbException("Page Num out of bound.");
        }
        HeapPageId curPageId = new HeapPageId(f.getId(), ++curPageNum);
        HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY);
        return curPage.iterator();
    }
}

