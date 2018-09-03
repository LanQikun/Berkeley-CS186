package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate predicate;
    private DbIterator child1, child2;
    private TupleIterator result;
    private TupleDesc td;

    // 131072是MySql中BlockNestedLoopJoin算法的默认缓冲区大小（以字节为单位）
    // 增大该参数可以减少磁盘IO
    public static int blockMemory = 131072 * 5;

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
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.predicate = p;
        this.child1 = child1;
        this.child2 = child2;
        this.td = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        return this.predicate;
    }

    /**
     * @return the field name of join field1. Should be quantified by alias or
     *         table name.
     */
    public String getJoinField1Name() {
        // 这两个方法不需要添加alias，因为输入的fieldName已经是被SeqScan添加过了的
        int num = predicate.getField1();
        return td.getFieldName(num);
    }

    /**
     * @return the field name of join field2. Should be quantified by alias or
     *         table name.
     *
     */
    public String getJoinField2Name() {
        int num = predicate.getField2();
        return td.getFieldName(num);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open()
            throws DbException, NoSuchElementException, TransactionAbortedException {
        child1.open();
        child2.open();
        super.open();
        result = dbnlSortedJoin();
        result.open();
    }

    /**
     * 性能瓶颈：对两个缓存区Block做Join操作（而不该想着减少IO），因为Block其实较大
     */
    private TupleIterator dbnlSortedJoin()
            throws DbException, TransactionAbortedException {
        LinkedList<Tuple> tuples = new LinkedList<>();
        int blockSize1 = blockMemory / child1.getTupleDesc().getSize();
        int blockSize2 = blockMemory / child2.getTupleDesc().getSize();
        Tuple[] leftCacheBlock = new Tuple[blockSize1];
        Tuple[] rightCacheBlock = new Tuple[blockSize2];
        int index1 = 0;
        int index2 = 0;
        int length1 = child1.getTupleDesc().numFields();
        child1.rewind();

        // 排序后连接block连接，每次将表1的多行与表2连接。
        // 缓冲区每次用完后要清空，以防对未满的block排序时受到影响。
        while (child1.hasNext()) {
            Tuple left = child1.next();
            leftCacheBlock[index1++] = left;
            if (index1 >= leftCacheBlock.length) {
                // 左缓冲区满了以后再处理右表
                child2.rewind();
                while (child2.hasNext()) {
                    Tuple right = child2.next();
                    rightCacheBlock[index2++] = right;
                    if (index2 >= rightCacheBlock.length) {
                        sortedJoin(tuples, leftCacheBlock, rightCacheBlock, length1);
                        Arrays.fill(rightCacheBlock, null);
                        index2 = 0;
                    }
                }
                // 处理右缓冲区剩余的内容
                if (index2 > 0 && index2 < rightCacheBlock.length) {
                    sortedJoin(tuples, leftCacheBlock, rightCacheBlock, length1);
                    Arrays.fill(rightCacheBlock, null);
                    index2 = 0;
                }
                Arrays.fill(leftCacheBlock, null);
                index1 = 0;
            }
        }
        // 处理左缓冲区剩余的内容(对右表进行最后一次join)
        if (index1 > 0 && index1 < leftCacheBlock.length) {
            child2.rewind();
            while (child2.hasNext()) {
                Tuple right = child2.next();
                rightCacheBlock[index2++] = right;
                if (index2 >= rightCacheBlock.length) {
                    sortedJoin(tuples, leftCacheBlock, rightCacheBlock, length1);
                    Arrays.fill(rightCacheBlock, null);
                    index2 = 0;
                }
            }
            if (index2 > 0 && index2 < rightCacheBlock.length) {
                sortedJoin(tuples, leftCacheBlock, rightCacheBlock, length1);
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    private void sortedJoin(LinkedList<Tuple> tuples, Tuple[] lcb, Tuple[] rcb,
            int length1) {
        // 只对非null元素排序
        int nonNullPos1 = lcb.length - 1;
        int nonNullPos2 = rcb.length - 1;
        for (; nonNullPos1 > 0 && lcb[nonNullPos1] == null; nonNullPos1--) {
            continue;
        }
        for (; nonNullPos2 > 0 && rcb[nonNullPos2] == null; nonNullPos2--) {
            continue;
        }
        Tuple[] leftCacheBlock = new Tuple[nonNullPos1 + 1];
        Tuple[] rightCacheBlock = new Tuple[nonNullPos2 + 1];
        System.arraycopy(lcb, 0, leftCacheBlock, 0, leftCacheBlock.length);
        System.arraycopy(rcb, 0, rightCacheBlock, 0, rightCacheBlock.length);

        int index1 = predicate.getField1();
        int index2 = predicate.getField2();
        // 用于每个blcok内部的比较
        JoinPredicate eqPredicate1 = new JoinPredicate(index1, Predicate.Op.EQUALS,
                index1);
        JoinPredicate eqPredicate2 = new JoinPredicate(index2, Predicate.Op.EQUALS,
                index2);
        JoinPredicate ltPredicate1 = new JoinPredicate(index1, Predicate.Op.LESS_THAN,
                index1);
        JoinPredicate ltPredicate2 = new JoinPredicate(index2, Predicate.Op.LESS_THAN,
                index2);
        JoinPredicate gtPredicate1 = new JoinPredicate(index1, Predicate.Op.GREATER_THAN,
                index1);
        JoinPredicate gtPredicate2 = new JoinPredicate(index2, Predicate.Op.GREATER_THAN,
                index2);

        Comparator<Tuple> comparator1 = new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                if (ltPredicate1.filter(o1, o2)) {
                    return -1;
                } else if (gtPredicate1.filter(o1, o2)) {
                    return 1;
                } else
                    return 0;
            }
        };
        Comparator<Tuple> comparator2 = new Comparator<Tuple>() {
            @Override
            public int compare(Tuple o1, Tuple o2) {
                if (ltPredicate2.filter(o1, o2)) {
                    return -1;
                } else if (gtPredicate2.filter(o1, o2)) {
                    return 1;
                } else
                    return 0;
            }
        };
        Arrays.sort(leftCacheBlock, comparator1);
        Arrays.sort(rightCacheBlock, comparator2);
        switch (predicate.getOperator()) {
        // 算法：求两排序数组交集
        case EQUALS:
            int pos1 = 0, pos2 = 0;
            // 用于两个block之间的比较
            JoinPredicate eqPredicate = new JoinPredicate(index1, Predicate.Op.EQUALS,
                    index2);
            JoinPredicate ltPredicate = new JoinPredicate(index1, Predicate.Op.LESS_THAN,
                    index2);
            while (pos1 < leftCacheBlock.length && pos2 < rightCacheBlock.length) {
                Tuple left = leftCacheBlock[pos1];
                Tuple right = rightCacheBlock[pos2];
                // 分别找到两个block连续相等的段，然后连接
                if (eqPredicate.filter(left, right)) {
                    int begin1 = pos1;
                    int begin2 = pos2;
                    for (; pos1 < leftCacheBlock.length
                            && eqPredicate1.filter(left, leftCacheBlock[pos1]); pos1++) {
                        continue;
                    }
                    for (; pos2 < rightCacheBlock.length && eqPredicate2.filter(right,
                            rightCacheBlock[pos2]); pos2++) {
                        continue;
                    }
                    for (int i = begin1; i < pos1; i++) {
                        for (int j = begin2; j < pos2; j++) {
                            tuples.add(
                                    Tuple.merge(leftCacheBlock[i], rightCacheBlock[j]));
                        }
                    }
                } else if (ltPredicate.filter(left, right)) {
                    pos1++;
                } else {
                    pos2++;
                }
            }
            break;
        case LESS_THAN:
        case LESS_THAN_OR_EQ:
            // 从右往左查看
            scan(tuples, leftCacheBlock, rightCacheBlock, false);
            break;
        case GREATER_THAN:
        case GREATER_THAN_OR_EQ:
            // 从左往右查看
            scan(tuples, leftCacheBlock, rightCacheBlock, true);
            break;
        default:
            throw new RuntimeException("JoinPredicate is Illegal");
        }
    }

    private void scan(LinkedList<Tuple> tuples, Tuple[] leftBlock, Tuple[] rightBlock,
            boolean fromLeft) {
        int inc, begin, end;
        if (fromLeft) {
            begin = 0;
            end = rightBlock.length;
            inc = 1;
        } else {
            begin = rightBlock.length - 1;
            end = -1;
            inc = -1;
        }
        for (Tuple left : leftBlock) {
            for (int i = begin; i != end; i += inc) {
                Tuple right = rightBlock[i];
                if (predicate.filter(left, right)) {
                    tuples.add(Tuple.merge(left, right));
                } else
                    break;
            }
        }
    }

    public void close() {
        super.close();
        child1.close();
        child2.close();
        result.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        result.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation(串联，连结) of joining tuples from the left and
     * right relation. Therefore, if an equality predicate is used there will be
     * two copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (result.hasNext()) {
            return result.next();
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child1, this.child2 };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

}