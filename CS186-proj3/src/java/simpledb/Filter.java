package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;
    private DbIterator child;
    //用于缓存，避免rewind后的重复操作
    private TupleIterator result;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.predicate = p;
        this.child = child;
        this.result = null;
    }
    
    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        super.open();
        this.child.open();
        this.result = getResult();
        result.open();
    }

    private TupleIterator getResult() throws NoSuchElementException, DbException, TransactionAbortedException{
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (child.hasNext()) {
            Tuple t = child.next();
            if (predicate.filter(t)) {
                tuples.add(t);
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    public void close() {
        super.close();
        this.child.close();
        this.result = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.result.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext()
            throws NoSuchElementException, TransactionAbortedException, DbException {
        if (result.hasNext()) {
            return result.next();
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }

}
