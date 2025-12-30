#include "odbc_connection_pool.h"

// 3. 使用连接池
void query_user_data(std::shared_ptr<odbc::ConnectionPool> pool, int user_id) {
    try {
        // 获取连接（RAII自动管理）
        auto conn = pool->get_connection();
        
        // 执行查询
        auto result = conn->query(
            "SELECT name, email FROM users WHERE id = " + std::to_string(user_id));
        
        for (const auto& row : result) {
            std::cout << "Name: " << row.get_as<std::string>("name")
                      << ", Email: " << row.get_as<std::string>("email") << std::endl;
        }
        
        // 连接自动归还到池中
    } catch (const std::exception& e) {
        std::cerr << "Database error: " << e.what() << std::endl;
    }
}

// 4. 多线程使用示例
void concurrent_operations(std::shared_ptr<odbc::ConnectionPool> pool) {
    std::vector<std::thread> threads;
    
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([i, &pool]() {
            try {
                auto conn = pool->get_connection();
                std::string infoStr = "INFO";
                std::string messageStr = "Thread " + std::to_string(i) + " message";
                auto stmt = conn->prepare("INSERT INTO logs(level, message) VALUES(?, ?)");
                stmt->bind_param(1, infoStr);
                stmt->bind_param(2, messageStr);
                stmt->execute(); // 确保执行时，bind_param绑定的参数未被释放
            } catch (const std::exception& e) {
                std::cerr << "Thread " << i << " error: " << e.what() << std::endl;
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
}

int main() {
    try {
        // 1. 配置连接池
        odbc::ConnectionPoolConfig pool_config;
        pool_config.min_connections = 5;
        pool_config.max_connections = 20;
        pool_config.connection_timeout = 30;
        pool_config.connection_config.databaseType = odbc::DatabaseType::MARIADB;
        pool_config.connection_config.driver = "MariaDB";
        pool_config.connection_config.host = "127.0.0.1";
        pool_config.connection_config.port = 3306;
        pool_config.connection_config.username = "testuser";
        pool_config.connection_config.password = "123456";
        pool_config.connection_config.database = "testdb";
        pool_config.connection_config.charset = "utf8";

        // 2. 创建连接池
        auto pool = std::make_shared<odbc::ConnectionPool>(pool_config);
        
        auto conn = pool->get_connection();
        // 创建表
        conn->execute("CREATE TABLE IF NOT EXISTS logs ("
                    "id INT PRIMARY KEY AUTO_INCREMENT, "
                    "level VARCHAR(50), "
                    "message VARCHAR(100))");
        
        concurrent_operations(pool);
        
        // 查询数据
        conn->execute("SELECT id, level, message FROM logs");
        std::cout << "main end" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "failed to execute sql, err: " << e.what() << std::endl;
    }
    
    return 0;
}