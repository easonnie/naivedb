package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {


    /**
     *  The Array of Type and Field.
     *  Data Structure might be change in the future.
     *
     *  This is the list of each <Type><Field> element.
     */
    private ArrayList<TDItem> fList;

    /**
     *  Get the TDItemlist of the TupleDesc
     */
    private ArrayList<TDItem> getFieldList() {
        return this.fList;
    }

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return fList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        // Populate the fList.

        if (typeAr == null || typeAr.length <= 0) {
            //System.out.println("Input field array is empty");
            throw new IllegalArgumentException("Type array for TupleDesc cannot be empty");
        }

        fList = new ArrayList<TDItem>();

        for (int i = 0; i < typeAr.length; i++) {
            //Deal with the situation that field name may be null or 0 or less than type.length.
            if (fieldAr != null && i < fieldAr.length) {
                fList.add(new TDItem(typeAr[i], fieldAr[i]));
            } else {
                fList.add(new TDItem(typeAr[i], new String("field " + i)));
            }
        }
    }

    /**
     *  A private constructor for merge method.
     * @param fList input field list
     */
    private TupleDesc(ArrayList<TDItem> fList) {
        this.fList = fList;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr){
        // some code goes here
        if (typeAr == null || typeAr.length <= 0) {
            //System.out.println("type array is empty"); //Error
            throw new IllegalArgumentException("Type array for TupleDesc cannot be empty");
        }

        fList = new ArrayList<TDItem>();

        for (int i = 0; i < typeAr.length; i++) {
            fList.add(new TDItem(typeAr[i], new String("field " + i)));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > this.numFields()) {
            throw new NoSuchElementException("Field index: " + i + " out of bound, the total number is " + this.numFields() + ".");
        }

        return fList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > this.numFields()) {
            throw new NoSuchElementException("Field index: " + i + " out of bound, the total number is " + this.numFields() + ".");
        }

        return fList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException("Field name is Null");
        }

        int num = this.numFields();
        for (int i = 0; i < num; i++) {
            TDItem field = fList.get(i);
            if (field.fieldName != null && field.fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No field name: " + name + " is found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < this.numFields(); i++) {
            size += fList.get(i).fieldType.getLen();
        }

        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        ArrayList<TDItem> newList = new ArrayList<TDItem>();
        newList.addAll(td1.getFieldList());
        newList.addAll(td2.getFieldList());

        return new TupleDesc(newList);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        TupleDesc obj = null;
        if (o instanceof TupleDesc) {
            obj = (TupleDesc) o;
        } else {
            return false;
        }

        if (obj.numFields() != this.numFields()) {
            return false;
        }

        if (obj.getSize() != this.getSize()) {
            return false;
        }

        for (int i = 0; i < obj.numFields(); i++) {
            if (!this.getFieldType(i).equals(obj.getFieldType(i))) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
