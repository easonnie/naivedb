package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;


    /**
     * Some member variables.
     *
     */
    private TupleDesc schema;
    private ArrayList<Field> fields;
    private RecordId rid;


    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        schema = td;
        fields = new ArrayList<Field>();
        for (int i = 0; i < td.numFields(); i++) {
            fields.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        //System.out.println(fields.size());
        if (i < 0 || i >= fields.size()) {
            throw new IllegalArgumentException("Field index out of bounds");
        }
        fields.set(i, f);

    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i >= fields.size()) {
            throw new IllegalArgumentException("Field index out of bounds");
        }

        return fields.get(i);

    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        StringBuilder description = new StringBuilder();
        int size = fields.size();
        for (int i = 0; i < size; i++) {
            if (fields.get(i) == null) {
                description.append("null\t");
            } else {
                description.append(fields.get(i).toString() + "\t");
            }
        }
        return description.toString();
        //throw new UnsupportedOperationException("Implement this");
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fields.iterator();
    }


    //A merge util method for join two tuples

    public static Tuple merge(Tuple t1, Tuple t2) {
        TupleDesc mDesc = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
        Tuple mTuple = new Tuple(mDesc);


        for (int i = 0; i < t1.getTupleDesc().numFields(); i++) {
            mTuple.setField(i, t1.getField(i));
        }

        for (int j = 0; j < t2.getTupleDesc().numFields(); j++) {
            int index = j + t1.getTupleDesc().numFields();
            mTuple.setField(index, t2.getField(j));
        }
        return mTuple;
    }

}
