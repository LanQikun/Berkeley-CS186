package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import simpledb.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TupleDesc desc;
    private RecordId id;
    private Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.desc = td;
        this.id = null;
        this.fields = new Field[desc.numFields()];
    }

    public Tuple(TupleDesc tupleDesc, Field[] fields) {
        this.desc = tupleDesc;
        this.fields = fields;
        this.id = null;
    }
    
    public static Tuple merge(Tuple t1, Tuple t2){
        TupleDesc resultTd = TupleDesc.merge(t1.desc, t2.desc);
        Field[] fields1 = t1.fields;
        Field[] fields2 = t2.fields;
        int len1 = t1.fields.length;
        int len2 = t2.fields.length;
        Field[] resultFields = new Field[len1+len2];
        System.arraycopy(fields1, 0, resultFields, 0, len1);
        System.arraycopy(fields2, 0, resultFields, len1, len2);
        return new Tuple(resultTd, resultFields);
    }
    
    public Field[] getFields(){
        return fields;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return desc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return id;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        id = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String result = "";
        for (Field field : fields) {
            result += field + "\t";
        }
        return result.substring(0, result.length() - 1) + '\n';
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        return new FieldIterator();
    }

    /*
     * iteratorµÄ¸¨ÖúÀà
    */
    private class FieldIterator implements Iterator<Field> {
        private int cursor;
        private final int end;

        public FieldIterator() {
            this.cursor = 0;
            this.end = fields.length;
        }

        public boolean hasNext() {
            return this.cursor < this.end;
        }

        public Field next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            return fields[cursor++];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
