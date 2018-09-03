package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator iter;
    private TupleDesc desc;
    private int count;
    private boolean canReturn;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.iter = child;
        this.desc = new TupleDesc(new Type[] { Type.INT_TYPE });
        this.count = 0;
        this.canReturn = false;
    }

    public TupleDesc getTupleDesc() {
        return desc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        iter.open();
        while (iter.hasNext()) {
            Tuple tuple = iter.next();
            Database.getBufferPool().deleteTuple(tid, tuple);
            count++;
        }
        canReturn = true;
    }

    public void close() {
        super.close();
        iter.close();
        canReturn = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        canReturn = true;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
        if (canReturn) {
            canReturn = false;
            Tuple t = new Tuple(getTupleDesc());
            t.setField(0, new IntField(count));
            return t;
        } else {
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { iter };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.iter = children[0];
    }

}
