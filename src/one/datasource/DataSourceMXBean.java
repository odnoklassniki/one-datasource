package one.datasource;

public interface DataSourceMXBean {
    String getUrl();
    int getOpenConnections();
    int getIdleConnections();
    int getTransactions();
    int getMaxConnections();
    long getBorrowTimeout();
    long getLockTimeout();
}
