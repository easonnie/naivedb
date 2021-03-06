package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    private Tuple curTuple1;
    private Tuple curTuple2;

    private Queue<Tuple> curJoinedTuple;


    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
        curJoinedTuple = new LinkedList<Tuple>();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1.open();
        child2.open();
    }

    public void close() {
        // some code goes here
        child2.close();
        child1.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (curJoinedTuple.size() > 0) {
            return curJoinedTuple.poll();
        }

        while (child1.hasNext()) {
            Tuple tuple1 = child1.next();
            child2.rewind();    //for each tuple1, we need to go through the whole child2.
            while (child2.hasNext()) {
                Tuple tuple2 = child2.next();

                if (p.filter(tuple1, tuple2)) {
                    curJoinedTuple.offer(Tuple.merge(tuple1, tuple2));  //problem may occur when tuple2 is very large.
                }
            }

            if (curJoinedTuple.size() > 0) {
                return curJoinedTuple.poll();
            }
        }
        return null;

//        Tuple[] curTuple = getNextPair();
//
//        while (curTuple != null && !p.filter(curTuple[0], curTuple[1])) {
//            curTuple = getNextPair();
//        }
//
//        if (curTuple == null) {
//            return null;
//        } else {
//            return Tuple.merge(curTuple[0], curTuple[1]);
//        }

    }

//    private Tuple[] getNextPair() throws DbException, TransactionAbortedException {
//        if (child1.hasNext()) {
//            curTuple1 = child1.next();
//        } else {
//            return null;
//        }
//
//        if (child2.hasNext()) {
//            curTuple2 = child2.next();
//        } else {
//            //child2 is go over
//            if (child1.hasNext()) {
//                curTuple1 = child1.next();
//                child2.rewind();
//                if (child2.hasNext()) {
//                    curTuple2 = child2.next();
//                } else {
//                    return null;
//                }
//            } else {
//                return null;
//                //over
//            }
//        }
//
//
//        return new Tuple[] { curTuple1, curTuple2 };
//    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.child1, this.child2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
    }

}
