package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldNum;
    private Type gbFieldType;
    private Op op;

    private boolean noGrouping;
    Map<Field, Integer> map;

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
     *            aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException
     *             if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbFieldNum = gbfield;
        this.gbFieldType = gbfieldtype;
        this.op = what;
        if (op != Op.COUNT) {
            throw new IllegalArgumentException("String类型的字段只支持count操作,不支持" + op);
        }
        noGrouping = (gbFieldNum == Aggregator.NO_GROUPING);
        map = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param t
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple t) {
        Field gbField = null;
        if (!noGrouping) {
            gbField = t.getField(gbFieldNum);
        }
        map.put(gbField, map.getOrDefault(gbField, 0) + 1);
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
        return new AggregatorIterator(map, gbFieldType, noGrouping);
    }

}
