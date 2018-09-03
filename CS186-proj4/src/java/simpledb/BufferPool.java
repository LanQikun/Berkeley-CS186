package simpledb;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
    private final long WAIT_TIME = 500;

    private int maxPagesCount;
    private LruMap<PageId, Page> cache;
    // 有三种情况需要释放锁：事务完成，替换页面，flushPage
    private LockManager lockManager;
    private Map<TransactionId, Set<Page>> tidToPages;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages
     *            maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.maxPagesCount = numPages;
        this.cache = new LruMap<PageId, Page>(maxPagesCount);
        this.tidToPages = new HashMap<TransactionId, Set<Page>>();
        this.lockManager = new LockManager();
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
     * 除了transactionComplete，只有这个方法会修改cache和事务表
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
        HeapPage page = (HeapPage) cache.get(pid);
        if (page == null) {
            if (cache.isFull()) {
                // 移除页面
                Page pageToRemove = cache.getCur();
                while (pageToRemove.isDirty() != null) {
                    pageToRemove = cache.getAnother();
                    if (pageToRemove == null) {
                        throw new DbException("all pages in the buffer pool are dirty");
                    }
                }
                cache.removeCur();
                lockManager.unlockAll(pageToRemove.getId());
            }
            // 加入新页面
            HeapFile table = getTable(pid);
            page = (HeapPage) table.readPage(pid);
            cache.put(pid, page);
        }
        blockedLock(tid, pid, perm);
        // 每次获取（而不是修改）page就加到事务表中，因为在事务结束时需要释放所有锁（而不只是X锁）
        updateTransaction(tid, page);
        return page;
    }

    private void blockedLock(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException {
        if (!lockManager.lock(tid, pid, perm)) {
            if(lockManager.mayDeadlock(tid, pid)){
                throw new TransactionAbortedException();
            }

            do {
                try {
                    Thread.sleep(WAIT_TIME);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } while (!lockManager.lock(tid, pid, perm));
        }
    }

    private void updateTransaction(TransactionId tid, Page page) {
        if (!tidToPages.containsKey(tid)) {
            tidToPages.put(tid, new HashSet<Page>());
        }
        tidToPages.get(tid).add(page);
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
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        lockManager.unlockAll(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid
     *            the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    // public boolean holdsLock(TransactionId tid, PageId pid) {
    // return this.lockManager.hasLock(tid, pid);
    // }

    /**
     * Commit or abort a given transaction; release all locks associated to the
     * transaction.
     *
     * @param tid
     *            the ID of the transaction requesting the unlock
     * @param commit
     *            a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // 当多个线程发现死锁时，可能导致对同一事务调用多次transactionComplete，
        // 所以要先判断tid是否在事务表中
        if (!tidToPages.containsKey(tid)) {
            return;
        }
        for (Page page : this.tidToPages.get(tid)) {
            PageId pid = page.getId();
            this.lockManager.unlockAll(tid, pid);
            TransactionId t = page.isDirty();
            if (t != null && tid.equals(t)) {
                if (commit) {
                    flushPage(page);
                } else {
                    // 将page恢复到tid未出现过的时候
                    HeapFile table = getTable(pid);
                    page = (HeapPage) table.readPage(pid);
                    // 发现问题：修改cache中的page时，没有修改tidToPage
                    cache.put(pid, page);
                }
            }
        }
        this.tidToPages.remove(tid);
        this.lockManager.removeTransaction(tid);
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
        table.insertTuple(tid, t);
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
        Iterator<Page> iter = cache.iterator();
        while (iter.hasNext()) {
            Page page = iter.next();
            this.lockManager.unlockAll(page.getId());
            if (page.isDirty() != null) {
                flushPage(page);
            }
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
     * Flushes a certain page to disk and mark it as not dirty
     * 
     * @param pid
     *            an ID indicating the page to flush
     */
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
        for (Page page : this.tidToPages.get(tid)) {
            PageId pid = page.getId();
            TransactionId t = page.isDirty();
            if (t != null && tid.equals(t)) {
                this.lockManager.unlockAll(pid);
                flushPage(page);
            }
        }
    }

    /**
     * Discards a page from the buffer pool. Flushes the page to disk to ensure
     * dirty pages are updated on disk.
     * 已经在LRU算法中实现
     * 
     * @throws IOException
     */
    private synchronized void evictPage() throws DbException, IOException {
    }

}
