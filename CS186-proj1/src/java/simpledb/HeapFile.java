package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    private int pageCount;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.pageCount = (int) (f.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // 假设pageNumber从0开始计数
        if (pid.pageNumber() >= pageCount) {
            throw new IllegalArgumentException("不存在id为" + pid + "的页");
        }

        int size = BufferPool.PAGE_SIZE;
        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            int pos = pid.pageNumber() * size;
            raf.seek(pos);
            byte[] datas = new byte[size];
            raf.read(datas, 0, size);
            return new HeapPage((HeapPageId) pid, datas);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return pageCount;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    // must use the BufferPool.getPage() method to access pages
    private class HeapFileIterator implements DbFileIterator {
        private TransactionId transactionId;
        private int cursor;
        private int last;
        //通过调用page的迭代器进行迭代
        private Iterator<Tuple> currentPageIterator;
        private boolean isOpen;

        public HeapFileIterator(TransactionId tid) {
            this.transactionId = tid;
            this.cursor = 0;
            this.last = pageCount - 1;
            this.isOpen = false;
        }

        //我认为需要构造相应的check，但那无法通过test
        public void open() throws DbException, TransactionAbortedException {
            currentPageIterator = getIterator();
            isOpen = true;
        }

        // 获取第cursor个iterator
        private Iterator<Tuple> getIterator() throws TransactionAbortedException, DbException {
            // 本项目的page number是从0开始的整数
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId,
                    new HeapPageId(getId(), cursor), Permissions.READ_ONLY);
            return page.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return false;
            }
            return currentPageIterator.hasNext() || cursor != last;
        }

        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            if (currentPageIterator.hasNext()) {
                return currentPageIterator.next();
            } else {
                cursor++;
                currentPageIterator = getIterator();
                return currentPageIterator.next();
            }
        }

        public void rewind() throws DbException, TransactionAbortedException {
            cursor = 0;
            currentPageIterator = getIterator();
        }

        public void close() {
            isOpen = false;
        }
    }

}
