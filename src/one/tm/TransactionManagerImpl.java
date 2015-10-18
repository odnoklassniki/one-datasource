package one.tm;

import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import java.io.Serializable;

public class TransactionManagerImpl implements TransactionManager, UserTransaction, Serializable {
    public static final TransactionManagerImpl INSTANCE = new TransactionManagerImpl();

    private static final ThreadLocal<TransactionImpl> TRANSACTIONS = new ThreadLocal<TransactionImpl>();
    private static final ThreadLocal<Integer> TIMEOUTS = new ThreadLocal<Integer>();
    private static final int DEFAULT_TIMEOUT = 3600;

    private TransactionManagerImpl() {
    }

    @Override
    public TransactionImpl getTransaction() {
        return TRANSACTIONS.get();
    }

    @Override
    public int getStatus() {
        TransactionImpl tx = getTransaction();
        return tx == null ? Status.STATUS_NO_TRANSACTION : tx.getStatus();
    }

    @Override
    public void begin() throws NotSupportedException {
        if (getTransaction() != null) {
            throw new NotSupportedException("Nested transactions not allowed");
        }
        Integer timeout = TIMEOUTS.get();
        TransactionImpl tx = new TransactionImpl(timeout != null ? timeout : DEFAULT_TIMEOUT);
        TRANSACTIONS.set(tx);
    }

    @Override
    public void commit() throws RollbackException, IllegalStateException, SystemException {
        TransactionImpl tx = getTransaction();
        if (tx == null) {
            throw new IllegalStateException("No associated transaction");
        }

        try {
            tx.commit();
        } finally {
            TRANSACTIONS.remove();
        }
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException {
        TransactionImpl tx = getTransaction();
        if (tx == null) {
            throw new IllegalStateException("No associated transaction");
        }

        try {
            tx.rollback();
        } finally {
            TRANSACTIONS.remove();
        }
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException {
        TransactionImpl tx = getTransaction();
        if (tx == null) {
            throw new IllegalStateException("No associated transaction");
        }
        tx.setRollbackOnly();
    }

    @Override
    public void setTransactionTimeout(int seconds) throws SystemException {
        if (seconds > 0) {
            TIMEOUTS.set(seconds);
        } else if (seconds == 0) {
            TIMEOUTS.remove();
        } else {
            throw new SystemException("Negative timeout value");
        }
    }

    @Override
    public TransactionImpl suspend() {
        TransactionImpl tx = getTransaction();
        TRANSACTIONS.remove();
        return tx;
    }

    @Override
    public void resume(Transaction tx) throws InvalidTransactionException, IllegalStateException {
        if (getTransaction() != null) {
            throw new IllegalStateException("Transaction is already associated");
        }
        if (!(tx instanceof TransactionImpl)) {
            throw new InvalidTransactionException("Foreign transaction: " + tx);
        }
        TRANSACTIONS.set((TransactionImpl) tx);
    }

    @Override
    public String toString() {
        TransactionImpl tx = getTransaction();
        return "TransactionManagerImpl{" + tx + '}';
    }
}
