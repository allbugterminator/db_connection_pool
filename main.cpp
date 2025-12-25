#include "odbc_wrapper.h"

int main() {
    odbc::ConnectionConfig dbConfig;
    dbConfig.dsn = "MyMariaDB";
    dbConfig.username = "sdba";
    dbConfig.password = "123456";
    dbConfig.database = "testdb";
    dbConfig.host = "127.0.0.1";
    dbConfig.port = 3306;

    odbc::Connection db(dbConfig);
    
    // 创建表
    db.execute("CREATE TABLE IF NOT EXISTS users ("
                "id INT PRIMARY KEY, "
                "name VARCHAR(50), "
                "email VARCHAR(100))");
    
    // 插入数据
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')");
    
    // 查询数据
    db.execute("SELECT id, name FROM users");
    
    return 0;
}