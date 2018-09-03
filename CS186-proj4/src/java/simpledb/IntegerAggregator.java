package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private static final long serialVersionUID = 1L;

    private int gbFieldNum;
    private Type gbFieldType;
    private int operandNum;
    private Op op;

    private Map<Field, Integer> map;
    private Map<Field, AvgTuple> avgMap;
    private boolean noGrouping;

    private class AvgTuple {
        public long sum;
        public int count;

        public AvgTuple() {
            this.sum = 0;
            this.count = 0;
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbFieldNum
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbFieldNumtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    // The Aggregator is told during construction what operation it should use
    // for aggregation
    // Subsequently, the client code should call
    // Aggregator.mergeTupleIntoGroup() for every tuple in the child iterator.
    // After all tuples have been merged, the client can retrieve a DbIterator
    // of aggregation results
    // Each tuple in the result is a pair of the form (groupValue,
    // aggregateValue), unless the value of the group by field was
    // Aggregator.NO_GROUPING
    public IntegerAggregator(int gbFieldNum, Type gbFieldNumtype, int afield, Op what) {
        this.gbFieldNum = gbFieldNum;
        this.gbFieldType = gbFieldNumtype;
        this.operandNum = afield;
        this.op = what;

        this.noGrouping = (gbFieldNum == Aggregator.NO_GROUPING);
        this.map = new HashMap<Field, Integer>();
        this.avgMap = new HashMap<Field, AvgTuple>();
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
        int fieldValue = ((IntField) t.getField(operandNum)).getValue();

        int value;
        if (op.equals(Op.AVG)) {
            value = operateAvg(gbField, fieldValue);
        } else if (op.equals(Op.COUNT)) {
            value = operateCount(gbField);
        } else {
            value = operateOthers(gbField, fieldValue);
        }
        map.put(gbField, value);
    }

    private int operateAvg(Field field, int fieldValue) {
        // AvgTuple是可变类型，对应每个field只创建一次，更新时只需要修改字段
        if (!avgMap.containsKey(field)) {
            avgMap.put(field, new AvgTuple());
        }
        AvgTuple avg = avgMap.get(field);
        avg.sum += fieldValue;
        avg.count++;
        return (int) (avg.sum / avg.count);
    }

    private int operateCount(Field field) {
        return map.getOrDefault(field, 0) + 1;
    }

    private int operateOthers(Field field, int fieldValue) {
        return getOthersValue(field, fieldValue, fieldValue);
    }

    private int getOthersValue(Field field, int fieldValue, int defaultValue) {
        if (!map.containsKey(field)) {
            return defaultValue;
        } else {
            int mapValue = map.get(field);
            switch (op) {
            case MAX:
                return Integer.max(fieldValue, mapValue);
            case MIN:
                return Integer.min(fieldValue, mapValue);
            case SUM:
                return fieldValue + mapValue;
            default:
                return defaultValue;
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
        return new AggregatorIterator(map, gbFieldType, noGrouping);
    }

}
