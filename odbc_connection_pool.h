#ifndef __ODBC_CONNECTION_POOL_H__
#define __ODBC_CONNECTION_POOL_H__

#include "odbc_wrapper.h"
#include <memory>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>
#include <unordered_set>
#include <functional>

namespace odbc {

/**
 * @brief 连接池配置结构体
 */
struct ConnectionPoolConfig {
    size_t min_connections = 5;           ///< 最小连接数
    size_t max_connections = 20;          ///< 最大连接数
    size_t max_idle_time = 300;           ///< 最大空闲时间(秒)
    size_t connection_timeout = 30;       ///< 连接超时时间(秒)
    size_t validation_interval = 60;     ///< 健康检查间隔(秒)
    bool test_on_borrow = true;           ///< 借用时测试连接
    bool test_on_return = false;          ///< 归还时测试连接
    
    // 连接字符串或配置
    ConnectionConfig connection_config;
};

// RAII连接句柄
class PoolConnectionHandle {
public:
    using Ptr = std::unique_ptr<PoolConnectionHandle>;
    using ReleaseFunc = std::function<void(Connection::Ptr)>;

    PoolConnectionHandle(Connection::Ptr conn, ReleaseFunc release_func)
        : conn_(std::move(conn)) {
        release_func_ = std::move(release_func);
        std::cout << "======PoolConnectionHandle, conn_: " << (conn_ == nullptr) << ", release_func_:" << (release_func_ == nullptr) << std::endl;
    }
    
    ~PoolConnectionHandle() {
        try
        {
            std::cout << "~PoolConnectionHandle, conn_: " << (conn_ == nullptr) << ", release_func_:" << (release_func_ == nullptr) << std::endl;
            if (conn_) {
                release_func_(std::move(conn_));
            }
        }
        catch(const std::exception& e)
        {
            std::cerr << "~PoolConnectionHandle, err: " << e.what() << std::endl;;
        }
        
    }
    
    // 禁止拷贝
    PoolConnectionHandle(const PoolConnectionHandle&) = delete;
    PoolConnectionHandle& operator=(const PoolConnectionHandle&) = delete;

    // 允许移动
    PoolConnectionHandle(PoolConnectionHandle&& other) noexcept
        : conn_(std::move(other.conn_))
        , release_func_(std::move(other.release_func_)) {
        other.release_func_ = nullptr;
    }
    
    PoolConnectionHandle& operator=(PoolConnectionHandle&& other) noexcept {
        if (this != &other) {
            conn_ = std::move(other.conn_);
            release_func_ = std::move(other.release_func_);
            other.release_func_ = nullptr;
        }
        return *this;
    }

    bool is_connected() {
        if (!conn_) {
            throw std::runtime_error("Connection handle is invalid");
        }
        return conn_->is_connected();
    }

    size_t execute(const std::string& sql) {
        if (!conn_) {
            throw std::runtime_error("Connection handle is invalid");
        }
        return conn_->execute(sql);
    }

    ResultSet query(const std::string& sql) {
        if (!conn_) {
            throw std::runtime_error("Connection handle is invalid");
        }
        return conn_->query(sql);
    }
    
    explicit operator bool() const { return conn_ != nullptr; }
    
private:
    Connection::Ptr conn_;
    ReleaseFunc release_func_;
};

/**
 * @brief 连接池主类
 */
class ConnectionPool : public std::enable_shared_from_this<ConnectionPool> {
public:
    using Ptr = std::shared_ptr<ConnectionPool>;
    explicit ConnectionPool(const ConnectionPoolConfig& config);
    ~ConnectionPool();
    
    // 禁止拷贝
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;
    
    /**
     * @brief 获取连接（支持超时）
     */
    PoolConnectionHandle::Ptr get_connection(
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));
    
    /**
     * @brief 获取连接池状态
     */
    struct PoolStatus {
        size_t total_connections;
        size_t idle_connections;
        size_t active_connections;
        size_t waiting_requests;
    };
    
    PoolStatus get_status() const;
    
    /**
     * @brief 关闭连接池
     */
    void shutdown();
    /**
     * @brief 将连接归还到池中
     */
    void return_connection(std::unique_ptr<Connection> conn);
    /**
     * @brief 是否已关闭
     */
    bool is_shutdown() {
        return shutdown_;
    }
    
private:
    /**
     * @brief 创建新连接
     */
    std::unique_ptr<Connection> create_connection();
    
    /**
     * @brief 健康检查线程函数
     */
    void health_check_task();
    
    /**
     * @brief 从池中获取空闲连接（内部使用）
     */
    PoolConnectionHandle::Ptr borrow_from_pool();
    
    // 连接池配置
    ConnectionPoolConfig config_;
    
    // 连接存储
    std::queue<Connection::Ptr> idle_connections_;
    std::unordered_set<Connection::Ptr> active_connections_;
    
    // 同步原语
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    
    // 状态变量
    std::atomic<size_t> total_connections_{0};
    std::atomic<size_t> waiting_requests_{0};
    std::atomic<bool> shutdown_{false};
    
    // 后台线程
    std::thread cleanup_thread_;
    std::thread health_check_thread_;
    
    // 友元声明，允许PooledConnection访问return_connection
    // friend class Connection;
};

/**
 * @brief 连接池智能指针的删除器
 */
class ConnectionDeleter {
public:
    explicit ConnectionDeleter(ConnectionPool* pool) : pool_(pool) {}
    
    void operator()(Connection* conn) {
        if (pool_ && !pool_->is_shutdown()) {
            pool_->return_connection(std::unique_ptr<Connection>(conn));
        } else {
            delete conn;
        }
    }
    
private:
    ConnectionPool* pool_;
};

using ConnectionPtr = std::unique_ptr<Connection, ConnectionDeleter>;

} // namespace odbc

#endif // __ODBC_CONNECTION_POOL_H__
