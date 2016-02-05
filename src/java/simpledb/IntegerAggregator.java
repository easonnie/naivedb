package simpledb;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Aggregator.Op op;

    private boolean isGrouped;

    private HashMap<Field, Integer> count_map;
    private HashMap<Field, Integer> value_map;

    private String aFiledName;
    private String gFiledName;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (gbfield == Aggregator.NO_GROUPING) {
            isGrouped = false;
        } else {
            isGrouped = true;
        }

        aFiledName = "";
        gFiledName = "";

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;

//        if (gbfieldtype == null) {
//            isGrouped = false;
//        } else {
//            isGrouped = true;
//        }

        this.afield = afield;
        this.op = what;

        count_map = new HashMap<Field, Integer>();
        value_map = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField;
        int fieldValue;

        aFiledName = op.toString() + "(" + tup.getTupleDesc().getFieldName(afield) + ")";

        if (isGrouped) {
            groupField = tup.getField(gbfield);
            gFiledName = tup.getTupleDesc().getFieldName(gbfield);
        } else {
            groupField = new IntField(0);
            gFiledName = op.toString();
        }

        fieldValue = ((IntField) tup.getField(afield)).getValue();
        
        if (op == Op.COUNT) {
            if (count_map.containsKey(groupField)) {
                count_map.put(groupField, count_map.get(groupField) + 1);
                value_map.put(groupField, value_map.get(groupField) + 1);
            } else {
                count_map.put(groupField, 1);
                value_map.put(groupField, 1);
            }
        } else if (op == Op.SUM || op == Op.AVG) {
            if (count_map.containsKey(groupField)) {
                count_map.put(groupField, count_map.get(groupField) + 1);
                value_map.put(groupField, value_map.get(groupField) + fieldValue);
            } else {
                value_map.put(groupField, fieldValue);
                count_map.put(groupField, 1);
            }
        } else if (op == Op.MAX) {
            if (count_map.containsKey(groupField)) {
                count_map.put(groupField, count_map.get(groupField) + 1);
                if (value_map.get(groupField) < fieldValue) {
                    value_map.put(groupField, fieldValue);
                }
            } else {
                value_map.put(groupField, fieldValue);
                count_map.put(groupField, 1);
            }
        } else if (op == Op.MIN) {
            if (count_map.containsKey(groupField)) {
                count_map.put(groupField, count_map.get(groupField) + 1);
                if (value_map.get(groupField) > fieldValue) {
                    value_map.put(groupField, fieldValue);
                }
            } else {
                value_map.put(groupField, fieldValue);
                count_map.put(groupField, 1);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        Type[] types;
        String[] names;
        TupleDesc td;

        if (!isGrouped) {
            types = new Type[] { Type.INT_TYPE };
            names = new String[] { aFiledName };
        } else {
            types = new Type[] { gbfieldtype, Type.INT_TYPE };
            names = new String[] { gFiledName, aFiledName };
        }

        td = new TupleDesc(types, names);

        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        if (!isGrouped) {
            for (Field groupField : value_map.keySet()) {
                int fieldValue = value_map.get(groupField);
                if (op == Op.MAX || op == Op.MIN || op == Op.SUM || op == Op.COUNT) {
                    Tuple tuple = new Tuple(td);
                    tuple.setField(0, new IntField(fieldValue));
                    tuples.add(tuple);
                } else if (op == Op.AVG) {
                    int count = count_map.get(groupField);
                    Tuple tuple = new Tuple(td);
                    tuple.setField(0, new IntField(fieldValue / count));
                    tuples.add(tuple);
                }
            }
        } else {
            for (Field groupField : value_map.keySet()) {
                int fieldValue = value_map.get(groupField);
                if (op == Op.MAX || op == Op.MIN || op == Op.SUM || op == Op.COUNT) {
                    Tuple tuple = new Tuple(td);
                    tuple.setField(0, groupField);
                    tuple.setField(1, new IntField(fieldValue));
                    tuples.add(tuple);
                } else if (op == Op.AVG) {
                    int count = count_map.get(groupField);
                    Tuple tuple = new Tuple(td);
                    tuple.setField(0, groupField);
                    tuple.setField(1, new IntField(fieldValue / count));
                    tuples.add(tuple);
                }
            }
        }
        return new TupleIterator(td, tuples);
    }

}
