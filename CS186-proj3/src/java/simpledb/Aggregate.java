package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator inputIter;
    private int gbFieldNum;
    private int operandNum;
    private Op op;

    private boolean noGrouping;
    private Aggregator aggregator;
    private AggregatorIterator OutputIter;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield,
            Aggregator.Op aop) {
        this.inputIter = child;
        this.gbFieldNum = gfield;
        this.operandNum = afield;
        this.op = aop;

        this.noGrouping = (gbFieldNum == Aggregator.NO_GROUPING);
        TupleDesc desc = inputIter.getTupleDesc();
        Type gbFieldType = noGrouping ? null : desc.getFieldType(gbFieldNum);
        if (desc.getFieldType(operandNum) == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gbFieldNum, gbFieldType,
                    operandNum, op);
        } else {
            aggregator = new StringAggregator(gbFieldNum, gbFieldType,
                    operandNum, op);
        }
        this.OutputIter = (AggregatorIterator) aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if (noGrouping) {
            return Aggregator.NO_GROUPING;
        } else {
            return gbFieldNum;
        }
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     */
    public String groupFieldName() {
        if (noGrouping) {
            return null;
        } else {
            return aggregator.iterator().getTupleDesc().getFieldName(0);
        }
    }

    /**
     * @return the aggregate field
     */
    // TODO: Ϊ�η���ֵ��int?
    public int aggregateField() {
        return this.operandNum;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    // TODO: ����tuple��ʱ��������
    public String aggregateFieldName() {
        if (noGrouping) {
            return aggregator.iterator().getTupleDesc().getFieldName(0);
        } else {
            return aggregator.iterator().getTupleDesc().getFieldName(1);
        }
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        inputIter.open();
        while (inputIter.hasNext()) {
            aggregator.mergeTupleIntoGroup(inputIter.next());
        }
        inputIter.close();
        OutputIter = (AggregatorIterator) aggregator.iterator();
        OutputIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
        if (OutputIter.hasNext()) {
            return OutputIter.next();
        } else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        OutputIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return OutputIter.getTupleDesc();
    }

    public void close() {
        super.close();
        OutputIter.close();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { inputIter };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.inputIter = children[0];
    }

}
