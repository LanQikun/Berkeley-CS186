package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

/**
 * BufferPool manages the reading and writing of pages into memory from disk.
 * Access methods call into it to retrieve pages, and it fetches pages from the
 * appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches a
 * page, BufferPool checks that the transaction has the appropriate locks to
 * read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /**
     * Default number of pages passed to the constructor. This is used by other
     * classes. BufferPool should use the numPages argument to the constructor
     * instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int maxPagesCount;
    private Cache<PageId, Page> cache;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages
     *            maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxPagesCount = numPages;
        // pageMap = new HashMap<PageId, Page>(maxPagesCount);
        cache = new Cache<>(maxPagesCount);
    }

    /**
     * Retrieve the specified page with the associated permissions. Will acquire
     * a lock and may block if that lock is held by another transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool. If it is
     * present, it should be returned. If it is not present, it should be added
     * to the buffer pool and returned. If there is insufficient space in the
     * buffer pool, an page should be evicted and the new page should be added
     * in its place.
     *
     * @param tid
     *            the ID of the transaction requesting the page
     * @param pid
     *            the ID of the requested page
     * @param perm
     *            the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // TODO: 是否需要判断lock?没有用到tid和perm
        HeapPage page = (HeapPage) cache.get(pid);
        if (page != null) {
            return page;
        }
        HeapFile table = getTable(pid);
        HeapPage newPage = (HeapPage) table.readPage(pid);
        Page removedPage = cache.add(pid, newPage);
        if (removedPage != null) {
            try {
                flushPage(removedPage);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return newPage;

    }

    private HeapFile getTable(PageId pid) {
        return (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
    }

    /**
     * Releases the lock on a page. Calling this is very risky, and may result
     * in wrong behavior. Think hard about who needs to call this and why, and
     * why they can run the risk of calling it.
     *
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param pid
     *            the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid
     *            the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param commit
     *            a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid. Will
     * acquire a write lock on the page the tuple is added to(Lock acquisition
     * is not needed for lab2). May block if the lock cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid
     *            the transaction adding the tuple
     * @param tableId
     *            the table to add the tuple to
     * @param t
     *            the tuple to add
     */
    // Marks any pages that were dirtied
    // updates cached versions of any pages that have been dirtied
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(tableId);
        // 更新pageMap(已经在低层的函数中mark dirty)
        ArrayList<Page> pages = table.insertTuple(tid, t);
        for (Page page : pages) {
            cache.add(page.getId(), page);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool. Will acquire a write
     * lock on the page the tuple is removed from. May block if the lock cannot
     * be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit. Does not need to update cached versions of any pages
     * that have been dirtied, as it is not possible that a new page was created
     * during the deletion (note difference from addTuple).
     *
     * @param tid
     *            the transaction adding the tuple.
     * @param t
     *            the tuple to add
     */
    // Remove the specified tuple from the buffer pool
    // Marks any pages that were dirtied
    // 这里说只从缓冲池中删除，但table中也应该删除
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        HeapFile table = getTable(pid);
        // 已经在低层的函数中mark dirty
        table.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk. NB: Be careful using this routine -- it
     * writes dirty data to disk so will break simpledb if running in NO STEAL
     * mode.
     */
    @Deprecated
    public synchronized void flushAllPages() throws IOException {
        Iterator<Page> it = cache.iterator();
        while (it.hasNext()) {
            flushPage(it.next());
        }
    }

    /**
     * Remove the specific page id from the buffer pool. Needed by the recovery
     * manager to ensure that the buffer pool doesn't keep a rolled back page in
     * its cache.
     * 
     * @throws IOException
     */
    public synchronized void discardPage(PageId pid) {

    }

    /**
     * Flushes a certain page to disk
     * 
     * @param pid
     *            an ID indicating the page to flush
     */
    // and mark it as not dirty
    private synchronized void flushPage(Page page) throws IOException {
        HeapFile table = getTable(page.getId());
        HeapPage dirtyPage = (HeapPage) page;
        table.writePage(dirtyPage);
        dirtyPage.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     * 
     * @throws IOException
     */
    // 已经在LruCache中实现
    @Deprecated
    private synchronized void evictPage() throws DbException, IOException {
    }

}
