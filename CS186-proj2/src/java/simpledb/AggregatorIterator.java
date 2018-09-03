package simpledb;

import java.util.Map;
import java.util.NoSuchElementException;

/*
 * 用于Aggregator的迭代器
 */
public class AggregatorIterator implements DbIterator {
    private boolean isOpen;
    private Field[] keys;
    private int cursor;
    private int end;
    private TupleDesc tupleDesc;

    private Map<Field, Integer> map;
    private boolean noGrouping;

    public AggregatorIterator(Map<Field, Integer> map, Type fieldType, boolean noGrouping) {
        this.isOpen = false;
        this.keys = map.keySet().toArray(new Field[map.size()]);
        this.cursor = 0;
        this.end = map.size();
        if (!noGrouping) {
            this.tupleDesc = new TupleDesc(new Type[] { fieldType, Type.INT_TYPE });
        } else {
            this.tupleDesc = new TupleDesc(new Type[] { Type.INT_TYPE });
        }
        this.map = map;
        this.noGrouping = noGrouping;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.isOpen = true;
    }

    private void checkOpen() {
        if (!isOpen) {
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        checkOpen();
        return cursor < end;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        checkOpen();
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Field keyField = keys[cursor];
        Field valueField = new IntField((int) map.get(keyField));
        cursor++;
        Field[] fields;
        if (!noGrouping) {
            fields = new Field[] { keyField, valueField };
        } else {
            fields = new Field[] { valueField };
        }
        return new Tuple(tupleDesc, fields);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        checkOpen();
        cursor = 0;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        checkOpen();
        isOpen = false;
    }

}
