package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private int tableId;
    private String tableAlias;
    private DbFileIterator iterator;
    private boolean isOpen;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.transactionId = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
        this.iterator = Database.getCatalog().getDbFile(tableId)
                .iterator(this.transactionId);
        this.isOpen = false;
    }

    /**
     * @return return the table name of the table the operator scans. This
     *         should be the actual name of the table in the catalog of the
     *         database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * 
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.iterator.open();
        isOpen = true;
    }

    private void checkOpen() throws IllegalStateException {
        if (!isOpen) {
            throw new IllegalStateException();
        }
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc desc = Database.getCatalog().getTupleDesc(tableId);
        int fieldNum = desc.numFields();
        Type[] types = new Type[fieldNum];
        String[] names = new String[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            types[i] = desc.getFieldType(i);
            String prefix = getAlias() == null ? "null." : getAlias() + ".";
            String fieldName = desc.getFieldName(i);
            fieldName = fieldName == null ? "null" : fieldName;
            names[i] = prefix + fieldName;
        }
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        checkOpen();
        return this.iterator.hasNext();
    }

    public Tuple next()
            throws NoSuchElementException, TransactionAbortedException, DbException {
        checkOpen();
        return iterator.next();
    }

    /**
     * Closes the iterator. When the iterator is closed, calling next(),
     * hasNext(), or rewind() should fail by throwing IllegalStateException.
     */
    public void close() {
        // 有这行无法通过测试
        // checkOpen();
        this.iterator.close();
        isOpen = false;
    }

    public void rewind()
            throws DbException, NoSuchElementException, TransactionAbortedException {
        checkOpen();
        this.iterator.rewind();
    }
}
