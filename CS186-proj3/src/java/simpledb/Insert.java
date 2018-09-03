package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator iter;
    private int tableId;

    private TupleDesc desc;
    private int count;
    private boolean canReturn;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.iter = child;
        this.tableId = tableid;
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
            try {
                Database.getBufferPool().insertTuple(tid, tableId, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    // insert DOES NOT need check to see if a particular tuple is a duplicate
    // before inserting it.因为默认它是未添加过的
    // 答案要求一次调用就插入所有行
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
        if (canReturn) {
            Field field = new IntField(count);
            canReturn = false;
            return new Tuple(desc, new Field[] { field });
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
