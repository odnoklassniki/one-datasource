package one.tm;

import javax.transaction.Status;
import javax.transaction.xa.Xid;

class XidImpl implements Xid {
    private static final int FORMAT_ID = 0x4f646b6c; // Odkl

    private final long globalTransactionId;
    private final int branchId;
    int status;

    XidImpl(long globalTransactionId, int branchId) {
        this.globalTransactionId = globalTransactionId;
        this.branchId = branchId;
        this.status = Status.STATUS_ACTIVE;
    }

    @Override
    public int getFormatId() {
        return FORMAT_ID;
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return new byte[]{
                (byte) (globalTransactionId >>> 56),
                (byte) (globalTransactionId >>> 48),
                (byte) (globalTransactionId >>> 40),
                (byte) (globalTransactionId >>> 32),
                (byte) (globalTransactionId >>> 24),
                (byte) (globalTransactionId >>> 16),
                (byte) (globalTransactionId >>> 8),
                (byte) globalTransactionId
        };
    }

    @Override
    public byte[] getBranchQualifier() {
        return new byte[]{
                (byte) (branchId >>> 24),
                (byte) (branchId >>> 16),
                (byte) (branchId >>> 8),
                (byte) branchId
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof XidImpl)) return false;

        XidImpl other = (XidImpl) o;
        return other.globalTransactionId == globalTransactionId && other.branchId == branchId;
    }

    @Override
    public int hashCode() {
        return (int) (globalTransactionId ^ (globalTransactionId >>> 32)) * 31 + branchId;
    }

    @Override
    public String toString() {
        return "XidImpl{" + globalTransactionId + ':' + branchId + '}';
    }
}
