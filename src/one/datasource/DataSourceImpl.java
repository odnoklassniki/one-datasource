package one.datasource;

import one.tm.TransactionManagerImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class DataSourceImpl implements DataSource, DataSourceMXBean {
    private static final Log log = LogFactory.getLog(DataSourceImpl.class);

    private final LinkedList<ConnectionWrapper> pool = new LinkedList<ConnectionWrapper>();
    private final Map<Transaction, ConnectionWrapper> inTransaction = new ConcurrentHashMap<Transaction, ConnectionWrapper>();

    private int loginTimeout = 10;
    private PrintWriter logWriter;

    private final String name;
    private final Driver driver;
    private final String url;
    private final String user;
    private final String password;
    private final long keepAlive;
    private final long borrowTimeout;
    private final int lockTimeout;
    private final int poolSize;
    private final TransactionManager transactionManager;

    private long checkIdleConnectionsTime;
    private int createdCount;
    private int waitingThreads;
    private boolean closed;

    public DataSourceImpl(String name, Properties props) {
        try {
            this.name = name;
            this.driver = (Driver) Class.forName(props.getProperty("driver", "com.inet.tds.TdsDriver")).newInstance();
            this.url = props.getProperty("url");
            this.user = props.getProperty("user");
            this.password = props.getProperty("password");
            this.keepAlive = Integer.parseInt(props.getProperty("keep-alive", "1800")) * 1000L;
            this.borrowTimeout = Integer.parseInt(props.getProperty("borrow-timeout", "3")) * 1000L;
            this.lockTimeout = Integer.parseInt(props.getProperty("lock-timeout", "-1"));
            this.poolSize = Integer.parseInt(props.getProperty("pool-size", "10"));
            this.transactionManager = getTransactionManager();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid DataSource descriptor for " + name, e);
        }
    }

    public void close() {
        synchronized (pool) {
            for (ConnectionWrapper connection : pool) {
                connection.closeUnderlyingConnection();
            }

            createdCount = 0;
            closed = true;
            pool.clear();
            pool.notifyAll();
        }
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "DataSourceImpl{" + name + '}';
    }

    @Override
    public Connection getConnection() throws SQLException {
        Transaction tx = getTransaction();
        if (tx == null) {
            return borrowConnection();
        }

        ConnectionWrapper connection = inTransaction.get(tx);
        if (connection == null) {
            connection = borrowConnection();
            registerInTransaction(connection, tx);
        } else if (log.isDebugEnabled()) {
            log.debug("reuse: " + toString() + " in " + tx);
        }
        return connection;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
        this.logWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() {
        return loginTimeout;
    }

    public Logger getParentLogger() {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Impossible to unwrap");
    }

    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    private ConnectionWrapper borrowConnection() throws SQLException {
        long accessTime = System.currentTimeMillis();
        long waited = 0;

        if (accessTime > checkIdleConnectionsTime) {
            checkIdleConnectionsTime = accessTime + keepAlive / 10;
            closeIdleConnections(accessTime - keepAlive);
        }

        reuse:
        synchronized (pool) {
            while (!closed) {
                // First try to get an idle object from the queue
                ConnectionWrapper connection = pool.pollFirst();
                if (connection != null) {
                    connection.lastAccessTime = accessTime;
                    return connection;
                }

                // If capacity permits, create a new object out of the lock
                if (createdCount < poolSize) {
                    createdCount++;
                    break reuse;
                }

                // Lastly wait until an existing connection becomes free
                waitForFreeConnection(borrowTimeout - waited);
                waited = System.currentTimeMillis() - accessTime;
            }
            throw new SQLException("DataSource is closed");
        }

        ConnectionWrapper connection = null;
        try {
            return connection = new ConnectionWrapper(getRawConnection(), this, accessTime);
        } finally {
            if (connection == null) decreaseCount();
        }
    }

    private Connection getRawConnection() throws SQLException {
        Properties props = new Properties();
        if (user != null) props.put("user", user);
        if (password != null) props.put("password", password);

        Connection connection = driver.connect(url, props);
        if (connection == null) {
            throw new SQLException("Unsupported connection string: " + url);
        }

        if (lockTimeout >= 0) {
            executeRawSQL(connection, "SET LOCK_TIMEOUT " + lockTimeout);
        }

        return connection;
    }

    private void executeRawSQL(Connection connection, String sql) {
        try {
            Statement stmt = connection.createStatement();
            try {
                stmt.executeUpdate(sql);
            } finally {
                stmt.close();
            }
        } catch (Throwable e) {
            log.error("Cannot execute " + sql + " on " + toString(), e);
        }
    }

    private void closeIdleConnections(long closeTime) {
        ArrayList<ConnectionWrapper> idleConnections = new ArrayList<ConnectionWrapper>();

        synchronized (pool) {
            for (Iterator<ConnectionWrapper> iterator = pool.iterator(); iterator.hasNext(); ) {
                ConnectionWrapper connection = iterator.next();
                if (connection.lastAccessTime < closeTime) {
                    idleConnections.add(connection);
                    iterator.remove();
                    decreaseCount();
                }
            }
        }

        if (!idleConnections.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Closing " + idleConnections.size() + " idle connections on " + toString());
            }
            for (ConnectionWrapper connection : idleConnections) {
                connection.closeUnderlyingConnection();
            }
        }
    }

    private void waitForFreeConnection(long waitTime) throws SQLException {
        if (waitTime <= 0) {
            throw new SQLException("DataSource timed out waiting for a free connection");
        }

        waitingThreads++;
        try {
            pool.wait(waitTime);
        } catch (InterruptedException e) {
            throw new SQLException("Interrupted while waiting for a free connection");
        }
        waitingThreads--;
    }

    private void decreaseCount() {
        synchronized (pool) {
            if (!closed) {
                createdCount--;
                if (waitingThreads > 0) pool.notify();
            }
        }
    }

    private TransactionManager getTransactionManager() throws NamingException {
        TransactionManager tm = null;
        try {
            tm = (TransactionManager) new InitialContext().lookup("java:/TransactionManager");
        } catch (NamingException e) {
            //
        }
        return tm != null ? tm : TransactionManagerImpl.INSTANCE;
    }

    private Transaction getTransaction() throws SQLException {
        try {
            return transactionManager.getTransaction();
        } catch (SystemException e) {
            throw new SQLException(e);
        }
    }

    void registerInTransaction(ConnectionWrapper connection, Transaction tx) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("register: " + toString() + " in " + tx);
        }

        try {
            connection.setAutoCommit(false);
            tx.enlistResource(new ConnectionXAResource(connection));
        } catch (SQLException e) {
            releaseConnection(connection);
            throw e;
        } catch (Throwable e) {
            releaseConnection(connection);
            throw new SQLException(e);
        }

        connection.tx = tx;
        inTransaction.put(tx, connection);
    }

    void unregisterFromTransaction(ConnectionWrapper connection) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("unregister: " + toString() + " from " + connection.tx);
        }

        inTransaction.remove(connection.tx);
        connection.tx = null;

        try {
            connection.setAutoCommit(true);
        } finally {
            releaseConnection(connection);
        }
    }

    void releaseConnection(ConnectionWrapper connection) {
        if (connection.invalidate) {
            decreaseCount();
        } else {
            synchronized (pool) {
                if (!closed) {
                    pool.addFirst(connection);
                    if (waitingThreads > 0) pool.notify();
                    return;
                }
            }
        }

        connection.closeUnderlyingConnection();
    }

    @Override
    public String getUrl() {
        return url;
    }

    @Override
    public int getOpenConnections() {
        return createdCount;
    }

    @Override
    public int getIdleConnections() {
        synchronized (pool) {
            return pool.size();
        }
    }

    @Override
    public int getTransactions() {
        return inTransaction.size();
    }

    @Override
    public int getMaxConnections() {
        return poolSize;
    }

    @Override
    public long getBorrowTimeout() {
        return borrowTimeout;
    }

    @Override
    public long getLockTimeout() {
        return lockTimeout;
    }
}
