package one.tm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

class TransactionImpl implements Transaction {
    private static final Log log = LogFactory.getLog(TransactionImpl.class);
    private static final AtomicLong GLOBAL_ID_GENERATOR = new AtomicLong();

    private long globalId;
    private long startTime;
    private long timeoutMs;
    private int status;
    private int branches;
    private Map<XAResource, XidImpl> resources;
    private List<Synchronization> synchronizations;

    TransactionImpl(int timeout) {
        this.globalId = GLOBAL_ID_GENERATOR.incrementAndGet();
        this.startTime = System.currentTimeMillis();
        this.timeoutMs = timeout * 1000L;
        this.status = Status.STATUS_ACTIVE;
        this.resources = new HashMap<XAResource, XidImpl>();
        this.synchronizations = new ArrayList<Synchronization>();

        if (log.isDebugEnabled()) {
            log.debug("begin: " + toString());
        }
    }

    @Override
    public int hashCode() {
        return (int) globalId;
    }

    @Override
    public String toString() {
        return "TransactionImpl{id=" + globalId + ",start=" + startTime + ",status=" + status + '}';
    }

    @Override
    public void commit() throws RollbackException, IllegalStateException, SystemException {
        switch (status) {
            case Status.STATUS_ACTIVE:
                if (System.currentTimeMillis() - startTime > timeoutMs) {
                    doRollback();
                    throw new RollbackException("Transaction is timed out");
                }
                doCommit();
                break;
            case Status.STATUS_MARKED_ROLLBACK:
                doRollback();
                throw new RollbackException("Transaction is marked for rollback");
            default:
                throw new IllegalStateException("Transaction is not active");
        }
    }

    @Override
    public boolean delistResource(XAResource res, int flag) throws IllegalStateException, SystemException {
        if (log.isDebugEnabled()) {
            log.debug("delist: " + res + " from " + toString());
        }

        switch (status) {
            case Status.STATUS_ACTIVE:
            case Status.STATUS_MARKED_ROLLBACK:
                XidImpl xid = resources.remove(res);
                if (xid != null) {
                    releaseResource(res, xid, flag);
                    return true;
                }
                return false;
            default:
                throw new IllegalStateException("Transaction is not active");
        }
    }

    @Override
    public boolean enlistResource(XAResource res) throws RollbackException, IllegalStateException, SystemException {
        if (log.isDebugEnabled()) {
            log.debug("enlist: " + res + " onto " + toString());
        }

        switch (status) {
            case Status.STATUS_ACTIVE:
                if (System.currentTimeMillis() - startTime > timeoutMs) {
                    throw new RollbackException("Transaction is timed out");
                }
                if (!resources.containsKey(res)) {
                    XidImpl xid = new XidImpl(globalId, ++branches);
                    try {
                        res.start(xid, XAResource.TMNOFLAGS);
                        resources.put(res, xid);
                        return true;
                    } catch (XAException e) {
                        throw SystemException(e);
                    }
                }
                return false;
            case Status.STATUS_MARKED_ROLLBACK:
                throw new RollbackException("Transaction is marked for rollback");
            default:
                throw new IllegalStateException("Transaction is not active");
        }
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void registerSynchronization(Synchronization sync) throws RollbackException, IllegalStateException {
        switch (status) {
            case Status.STATUS_ACTIVE:
                synchronizations.add(sync);
                break;
            case Status.STATUS_MARKED_ROLLBACK:
                throw new RollbackException("Transaction is marked for rollback");
            default:
                throw new IllegalStateException("Transaction is not active");
        }
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException {
        if (status == Status.STATUS_COMMITTED) {
            throw new IllegalStateException("Transaction is already committed");
        }
        doRollback();
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException {
        if (status > Status.STATUS_MARKED_ROLLBACK) {
            throw new IllegalStateException("Transaction is not active");
        }
        status = Status.STATUS_MARKED_ROLLBACK;
    }

    private void doCommit() throws SystemException {
        if (log.isDebugEnabled()) {
            log.debug("commit: " + toString());
        }

        beforeCompletion();
        status = Status.STATUS_COMMITTING;

        try {
            for (Map.Entry<XAResource, XidImpl> entry : resources.entrySet()) {
                XAResource res = entry.getKey();
                XidImpl xid = entry.getValue();
                if (xid.status == Status.STATUS_ACTIVE) {
                    res.commit(xid, true);
                    releaseResource(res, xid, XAResource.TMSUCCESS);
                    xid.status = Status.STATUS_COMMITTED;
                }
            }

            status = Status.STATUS_COMMITTED;
            afterCompletion();
        } catch (XAException e) {
            throw SystemException(e);
        } finally {
            if (status != Status.STATUS_COMMITTED) {
                doRollback();
            }
        }
    }

    private void doRollback() throws SystemException {
        if (log.isDebugEnabled()) {
            log.debug("rollback: " + toString());
        }

        XAException xaException = null;
        status = Status.STATUS_ROLLING_BACK;

        try {
            for (Map.Entry<XAResource, XidImpl> entry : resources.entrySet()) {
                XAResource res = entry.getKey();
                XidImpl xid = entry.getValue();
                if (xid.status == Status.STATUS_ACTIVE) {
                    try {
                        res.rollback(xid);
                    } catch (XAException e) {
                        xaException = e;
                    } finally {
                        releaseResource(res, xid, XAResource.TMFAIL);
                        xid.status = Status.STATUS_ROLLEDBACK;
                    }
                }
            }
        } finally {
            status = Status.STATUS_ROLLEDBACK;
            afterCompletion();
        }

        if (xaException != null) {
            throw SystemException(xaException);
        }
    }

    private void beforeCompletion() {
        if (!synchronizations.isEmpty()) {
            for (Synchronization sync : synchronizations) {
                sync.beforeCompletion();
            }
        }
    }

    private void afterCompletion() {
        if (!synchronizations.isEmpty()) {
            for (Synchronization sync : synchronizations) {
                sync.afterCompletion(status);
            }
        }
    }

    private void releaseResource(XAResource res, XidImpl xid, int flag) {
        try {
            res.end(xid, flag);
        } catch (XAException e) {
            log.warn("Cannot release " + res + " due to " + e);
        }
    }

    private static SystemException SystemException(XAException e) {
        SystemException wrapper = new SystemException(e.errorCode);
        wrapper.initCause(e);
        return wrapper;
    }
}
