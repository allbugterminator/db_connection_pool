# db_connection_pool

## UnixODBC配置指南
[https://mp.weixin.qq.com/s/rlRyOIjnlXlsvhugMpRy7g](https://mp.weixin.qq.com/s/rlRyOIjnlXlsvhugMpRy7g "详解UnixODBC：在Linux中配置MySQL数据源与C++连接实例")

## 文件说明
1. connection_pool_test.cpp是连接池测试代码；
2. odbc_connection_pool.cpp和odbc_connection_pool.h是线程池核心实现；
3. odbc_wrapper.h是对odbc的封装；
4. odbc_mysql.cpp和test_odbc.cpp就是两个测试代码；