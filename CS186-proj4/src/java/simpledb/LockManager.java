package simpledb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private class Lock {
        TransactionId tid;
        Permissions perm;

        public Lock(TransactionId tid, Permissions perm) {
            this.tid = tid;
            this.perm = perm;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Lock Lock = (Lock) o;
            return tid.equals(Lock.tid) && perm.equals(Lock.perm);
        }

        @Override
        public int hashCode() {
            return tid.hashCode() * 31 + perm.hashCode();
        }
    }

    private Map<PageId, Set<Lock>> lockMap;
    // ��Դ����ͼ
    private Map<TransactionId, PageId> waitingMap;

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<PageId, Set<Lock>>();
        this.waitingMap = new ConcurrentHashMap<TransactionId, PageId>();
    }

    // ==========================���������� begin==================================

    /**
     * ��ȡpid��Ӧ��locks����������ڣ��ʹ���һ��
     */
    private Set<Lock> getOrPutLocks(PageId pid) {
        if (!lockMap.containsKey(pid)) {
            Set<Lock> locks = new HashSet<Lock>();
            lockMap.put(pid, locks);
        }
        return lockMap.get(pid);
    }

    /**
     * @return �����Ƿ�ɹ�
     */
    public synchronized boolean lock(TransactionId tid, PageId pid, Permissions perm) {
        boolean success = false;
        if (perm.equals(Permissions.READ_ONLY)) {
            success = grantSLock(tid, pid);
        } else {
            success = grantXLock(tid, pid);
        }
        if (!success) {
            waitingMap.put(tid, pid);
        } else {
            waitingMap.remove(tid);
        }
        return success;
    }

    private synchronized boolean grantSLock(TransactionId tid, PageId pid) {
        Set<Lock> locks = getOrPutLocks(pid);
        // ���X��
        for (Lock lock : locks) {
            if (lock.perm == Permissions.READ_WRITE && !lock.tid.equals(tid)) {
                // �����������X��
                return false;
            }
        }
        Lock thisSLock = new Lock(tid, Permissions.READ_ONLY);
        // ���S��
        if (!locks.contains(thisSLock)) {
            locks.add(thisSLock);
        }
        return true;
    }

    private synchronized boolean grantXLock(TransactionId tid, PageId pid) {
        Set<Lock> locks = getOrPutLocks(pid);
        boolean update = false;
        for (Lock lock : locks) {
            if (lock.tid.equals(tid)) {
                if (lock.perm == Permissions.READ_WRITE) {
                    // �б������X��
                    return true;
                } else {
                    update = true;
                }
            } else {
                // �������������
                return false;
            }
        }
        if (update) {
            // �б������S��������ΪX��
            locks.remove(new Lock(tid, Permissions.READ_ONLY));
        }
        locks.add(new Lock(tid, Permissions.READ_WRITE));
        return true;
    }

    public synchronized void unlock(TransactionId tid, PageId pid, Permissions perm) {
        Set<Lock> locks = lockMap.get(pid);
        Lock releaseLock = new Lock(tid, perm);
        locks.remove(releaseLock);
        if (locks.isEmpty()) {
            lockMap.remove(pid);
        }
    }

    // public boolean hasLock(PageId pid) {
    // return lockMap.containsKey(pid) && lockMap.get(pid).size() > 0;
    // }

    // public boolean hasLock(TransactionId tid, PageId pid) {
    // if (lockMap.containsKey(pid)) {
    // for (Lock lock : lockMap.get(pid)) {
    // if (lock.tid.equals(tid)) {
    // return true;
    // }
    // }
    // }
    // return false;
    // }

    public synchronized void unlockAll(PageId pid) {
        if (lockMap.containsKey(pid)) {
            Set<Lock> locks = lockMap.get(pid);
            locks.clear();
            lockMap.remove(pid);
        }
    }

    public synchronized void unlockAll(TransactionId tid, PageId pid) {
        if (this.lockMap.containsKey(pid)) {
            Set<Lock> locks = lockMap.get(pid);
            Lock thisSLock = new Lock(tid, Permissions.READ_ONLY);
            Lock thisXLock = new Lock(tid, Permissions.READ_WRITE);
            locks.remove(thisSLock);
            locks.remove(thisXLock);
        }
    }

    // ==========================���������� end==================================

    // ==========================������� begin==================================

    // ��֪��Ϊ�γ���
    // public synchronized boolean mayDeadlock(TransactionId proposer, PageId
    // pid,
    // Permissions perm) {
    // if (perm == Permissions.READ_ONLY) {
    // for (Lock lock : lockMap.get(pid)) {
    // TransactionId cur = lock.tid;
    // if (!cur.equals(proposer) && lock.perm == Permissions.READ_WRITE) {
    // return isWaited(proposer, cur);
    // }
    // }
    // } else {
    // Set<TransactionId> used = new HashSet<TransactionId>();
    // for (Lock lock : lockMap.get(pid)) {
    // TransactionId cur = lock.tid;
    // if (!cur.equals(proposer) && !used.contains(cur)) {
    // if (isWaited(proposer, cur)) {
    // return true;
    // } else {
    // used.add(cur);
    // }
    // }
    // }
    // }
    // return false;
    // }
    //
    // /**
    // * @return proposer�Ƿ�ȴ�holder
    // */
    // private synchronized boolean isWaited(TransactionId holder, TransactionId
    // proposer) {
    // if (holder.equals(proposer)) {
    // System.out.println(222);
    // return true;
    // }
    // if (!waitingMap.containsKey(proposer)) {
    // System.out.println(waitingMap.size());
    // System.out.println(333);
    // return false;
    // }
    //
    // WaitingState state = waitingMap.get(proposer);
    // PageId pid = state.pid;
    // Permissions perm = state.perm;
    // if (perm == Permissions.READ_ONLY) {
    // for (Lock lock : lockMap.get(pid)) {
    // TransactionId cur = lock.tid;
    // if (!cur.equals(proposer) && lock.perm == Permissions.READ_WRITE) {
    // System.out.println(9999);
    // return isWaited(holder, cur);
    // }
    // }
    // } else {
    // Set<TransactionId> used = new HashSet<TransactionId>();
    // for (Lock lock : lockMap.get(pid)) {
    // TransactionId cur = lock.tid;
    // if (!cur.equals(proposer) && !used.contains(cur)) {
    // if (isWaited(holder, cur)) {
    // System.out.println("holder: " + holder);
    // System.out.println("cur: " + cur);
    // System.out.println(0000);
    // return true;
    // } else {
    // used.add(cur);
    // }
    // }
    // }
    // }
    // return false;
    // }

    // ���A����p��Դʧ�ܣ��ͼ��p�ĳ������Ƿ��ڣ�ֱ�ӻ��ӣ��ȴ�A���е���Դ
    public synchronized boolean mayDeadlock(TransactionId tid, PageId pid) {// T1Ϊtid��P3Ϊpid
        if (!lockMap.containsKey(pid)) {
            return false;
        }
        List<PageId> allResource = getAllResource(tid);
        for (Lock lock : lockMap.get(pid)) {
            TransactionId holder = lock.tid;
            if (!holder.equals(tid)) {
                if (isWaiting(holder, allResource, tid)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * �ж�tid�Ƿ�ֱ�ӻ��ӣ��ȴ�pids�е���Դ
     */
    private synchronized boolean isWaiting(TransactionId tid, List<PageId> allResource,
            TransactionId toRemove) {
        if (!waitingMap.containsKey(tid)) {
            return false;
        }
        PageId waitingPid = waitingMap.get(tid);
        for (PageId resource : allResource) {
            if (resource.equals(waitingPid)) {
                return true;
            }
        }
        for (Lock lock : lockMap.get(waitingPid)) {
            TransactionId holder = lock.tid;
            if (!holder.equals(toRemove)) {
                if (isWaiting(holder, allResource, toRemove))
                    return true;
            }
        }
        return false;
    }

    private List<PageId> getAllResource(TransactionId tid) {
        List<PageId> result = new ArrayList<>();
        for (Map.Entry<PageId, Set<Lock>> entry : lockMap.entrySet()) {
            for (Lock lock : entry.getValue()) {
                if (lock.tid.equals(tid)) {
                    result.add(entry.getKey());
                }
            }
        }
        return result;
    }

    public void removeTransaction(TransactionId tid) {
        this.waitingMap.remove(tid);
    }

};