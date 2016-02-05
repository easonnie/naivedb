package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Aggregator.Op op;

    private boolean isGrouped;

    private HashMap<Field, Integer> count_map;

    private String aFiledName;
    private String gFiledName;


    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {

        if (what != Op.COUNT) {
            throw new IllegalArgumentException("op must be COUNT");
        }
        // some code goes
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
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField;
        String fieldValue;

        aFiledName = op.toString() + "(" + tup.getTupleDesc().getFieldName(afield) + ")";

        if (isGrouped) {
            groupField = tup.getField(gbfield);
            gFiledName = tup.getTupleDesc().getFieldName(gbfield);
        } else {
            groupField = new StringField("0", 2);
            gFiledName = op.toString();
        }

        fieldValue = ((StringField) tup.getField(afield)).getValue();

        if (op == Op.COUNT) {
            if (count_map.containsKey(groupField)) {
                count_map.put(groupField, count_map.get(groupField) + 1);
            } else {
                count_map.put(groupField, 1);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
            for (Field groupField : count_map.keySet()) {
                int fieldValue = count_map.get(groupField);
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(fieldValue));
                tuples.add(tuple);
            }

        } else {
            for (Field groupField : count_map.keySet()) {
                Tuple tuple = new Tuple(td);
                int fieldValue = count_map.get(groupField);
                tuple.setField(0, groupField);
                tuple.setField(1, new IntField(fieldValue));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

}
