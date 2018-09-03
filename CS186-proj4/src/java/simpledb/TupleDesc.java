package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    // 字段
    private TDItem[] items;
    // 字段个数
    private int numItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr.length == 0) {
            throw new IllegalArgumentException();
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException();
        }

        int length = typeAr.length;
        this.numItems = length;
        this.items = new TDItem[length];
        for (int i = 0; i < length; i++) {
            items[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }

    /*
     * merge时用的构造函数
    */
    public TupleDesc(TDItem[] _items) {
        this.items = _items.clone();
        this.numItems = items.length;
    }

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        Type fieldType;

        /**
         * The name of the field
         */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }

            // correct: 根据定义，name可以是null
            if (o instanceof TDItem) {
                TDItem another = (TDItem) o;
                boolean nameEqual;
                if (this.fieldName != null && another.fieldName != null) {
                    nameEqual = true;
                } else if (this.fieldName == null && another.fieldName == null) {
                    nameEqual = true;
                } else {
                    nameEqual = false;
                }
                boolean typeEqual = this.fieldType.equals(another.fieldType);
                return nameEqual && typeEqual;
            } else {
                return false;
            }
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems that are
     *         included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return new ItemIterator();
    }

    private class ItemIterator implements Iterator<TDItem> {
        private int cursor;
        private final int end;

        public ItemIterator() {
            this.cursor = 0;
            this.end = numItems;
        }

        public boolean hasNext() {
            return this.cursor < this.end;
        }

        public TDItem next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return items[cursor++];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return numItems;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        checkIndex(i);
        return items[i].fieldName;
    }

    private void checkIndex(int i) throws NoSuchElementException {
        if (i < 0 || i >= numItems) {
            throw new NoSuchElementException();
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        checkIndex(i);
        return items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    // correct: name可能为null
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name != null) {
            for (int i = 0; i < numItems; i++) {
                if (items[i].fieldName != null && items[i].fieldName.equals(name)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    // correct: 不包括name的size
    public int getSize() {
        int size = 0;
        for (TDItem item : items) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TDItem[] tdItems1 = td1.items;
        TDItem[] tdItems2 = td2.items;
        int length1 = tdItems1.length;
        int length2 = tdItems2.length;
        TDItem[] resultItems = new TDItem[length1 + length2];
        System.arraycopy(tdItems1, 0, resultItems, 0, length1);
        System.arraycopy(tdItems2, 0, resultItems, length1, length2);
        return new TupleDesc(resultItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof TupleDesc) {
            TupleDesc another = (TupleDesc) o;
            if (another.numFields() == numItems) {
                TDItem[] anotherItems = another.items;
                for (int i = 0; i < numItems; i++) {
                    if (!items[i].equals(anotherItems[i])) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return items.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer result = new StringBuffer();
        for (TDItem tdItem : items) {
            result.append(tdItem.toString() + ", ");
        }
        String s = result.toString();
        return s.substring(0, s.length()-2);
    }
}
