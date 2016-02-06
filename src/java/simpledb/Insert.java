package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TupleDesc tupleDesc;
    private final int tableId;
    private DbIterator child;
    private boolean fetched = false;
    private TransactionId tid;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.tableId = tableid;
        this.tid = t;
        this.child = child;
        // tuple desc
        Type[] types = new Type[]{Type.INT_TYPE};
        String[] fields = new String[]{"Number of inserted tuples"};
        tupleDesc = new TupleDesc(types, fields);

        TupleDesc childTupleDesc = child.getTupleDesc();
        DbFile dbFile = Database.getCatalog().getDbFile(tableid);
        TupleDesc tableTupleDesc = dbFile.getTupleDesc();
        if (!childTupleDesc.equals(tableTupleDesc)) {
            throw new DbException("Insert error: the tuple description of child does not match the table to be inserted into.");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, IOException {
        // some code goes here
        if (fetched) {
            return null;
        }

        fetched = true;
        int count = 0;
        while (child.hasNext()) {
            Database.getBufferPool().insertTuple(tid, tableId, child.next());
            count++;
        }

        Tuple tuple = new Tuple(this.tupleDesc);
        tuple.setField(0, new IntField(count));
        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
