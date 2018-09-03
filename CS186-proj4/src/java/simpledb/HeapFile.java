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
     * @throws Exception
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.pageCount = (int) Math.ceil(f.length() / BufferPool.PAGE_SIZE);
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
        // 本项目中的pageNumber是从0开始的整数
        if (pid.pageNumber() >= pageCount) {
            throw new IllegalArgumentException(file.getAbsolutePath());

            // throw new IllegalArgumentException("该页的页号为" + pid.pageNumber()
            // + "，超过了该表的页数上限" + pageCount);
        }

        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            int size = BufferPool.PAGE_SIZE;
            int pos = pid.pageNumber() * size;
            raf.seek(pos);
            byte[] data = new byte[size];
            raf.read(data, 0, size);
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(getFile(), "rw");
        int size = BufferPool.PAGE_SIZE;
        int pos = page.getId().pageNumber() * size;
        raf.seek(pos);
        byte[] data = page.getPageData();
        raf.write(data, 0, size);
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return pageCount;
    }

    // see DbFile.java for javadocs
    // This method will acquire a lock on the affected pages of the file, and
    // may block until the lock can be acquired.
    // access pages using the BufferPool.getPage() method
    // If no such pages exist in the HeapFile, you need to create a new page and
    // append it to the physical file on disk

    // modify:需要使用markDirty
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        ArrayList<Page> pages = new ArrayList<Page>();
        HeapPage page = null;
        boolean found = false;
        for (int i = 0; i < this.numPages(); i++) {
            page = (HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            HeapPageId newPid = new HeapPageId(getId(), numPages());
            page = new HeapPage(newPid, HeapPage.createEmptyPageData());
            writePage(page);
            pageCount++;
            // 必须用BufferPool.getPage将page加入缓冲池，才能进行后续操作
            page = (HeapPage) Database.getBufferPool().getPage(tid, newPid,
                    Permissions.READ_WRITE);
        }
        page.insertTuple(t);
        page.markDirty(true, tid);

        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    // This method will acquire a lock on the affected pages of the
    // file, and may block until the lock can be acquired.

    // throws DbException if the tuple cannot be deleted or is not a member
    // of the file

    public Page deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        if (t.getRecordId() != null) {
            PageId pageId = t.getRecordId().getPageId();
            if (pageId.pageNumber() < pageCount) {
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId,
                        Permissions.READ_WRITE);
                page.deleteTuple(t);
                page.markDirty(true, tid);
                return page;
            }
        }
        throw new DbException(
                "the tuple cannot be deleted or is not a member of the file");
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
        private Iterator<Tuple> currentPageIterator;
        private boolean isOpen;

        public HeapFileIterator(TransactionId tid) {
            this.transactionId = tid;
            this.cursor = 0;
            this.last = pageCount - 1;
            this.isOpen = false;
        }

        // 我认为需要构造相应的checkOpen()，但那无法通过test
        public void open() throws DbException, TransactionAbortedException {
            currentPageIterator = getIterator();
            isOpen = true;
        }

        // 获取第cursor个iterator
        private Iterator<Tuple> getIterator()
                throws TransactionAbortedException, DbException {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId,
                    new HeapPageId(getId(), cursor), Permissions.READ_ONLY);
            return page.iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen || cursor > last) {
                return false;
            }
            if (!currentPageIterator.hasNext()) {
                cursor++;
                if (cursor > last) {
                    return false;
                }
                currentPageIterator = this.getIterator();
                return hasNext();
            }
            return true;
        }

        public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentPageIterator.next();
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
