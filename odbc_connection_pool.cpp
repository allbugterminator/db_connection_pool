#include "odbc_connection_pool.h"
#include <algorithm>
#include <iostream>

namespace odbc {

ConnectionPool::ConnectionPool(const ConnectionPoolConfig& config)
    : config_(config) {
    
    // 初始化最小连接数
    try {
        for (size_t i = 0; i < config_.min_connections; ++i) {
            auto conn = std::make_unique<Connection>(config_.connection_config);
            idle_connections_.push(std::move(conn));
            total_connections_++;
        }

        // std::cout << "ConnectionPool init, idl_connections size: " << total_connections_ << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Warning: Failed to create initial connections: " 
                  << e.what() << std::endl;
    }
    
    // 启动后台线程
    health_check_thread_ = std::thread(&ConnectionPool::health_check_task, this);
}

ConnectionPool::~ConnectionPool() {
    shutdown();
}

void ConnectionPool::shutdown() {
    if (shutdown_.exchange(true)) {
        return; // 已经关闭
    }
    
    // 通知所有等待的线程
    condition_.notify_all();
    
    // 等待后台线程结束
    // if (cleanup_thread_.joinable()) {
    //     cleanup_thread_.join();
    // }
    if (health_check_thread_.joinable()) {
        health_check_thread_.join();
    }
    
    // 关闭所有连接
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 关闭空闲连接
    while (!idle_connections_.empty()) {
        idle_connections_.pop();
    }
    
    // 注意：活跃连接会在其析构时自动关闭
    active_connections_ = 0;
    total_connections_ = 0;
}

PoolConnectionHandle::Ptr ConnectionPool::get_connection(std::chrono::milliseconds timeout) {
    
    if (shutdown_) {
        throw std::runtime_error("Connection pool is shutdown");
    }
    
    waiting_requests_++;
    
    auto start_time = std::chrono::steady_clock::now();
    
    while (!shutdown_) {
        // 尝试从池中获取连接
        auto connHandle = borrow_from_pool();
        if (connHandle) {
            waiting_requests_--;
            // std::cout << "success to get conn handle" << std::endl;
            
            // 检查连接是否有效
            if (config_.test_on_borrow && !connHandle->is_connected()) {
                // 连接无效，尝试创建新连接
                try {
                    // 连接无效，尝试创建新连接
                    auto new_conn = create_connection();
                    if (new_conn) {
                        auto weak_pool = weak_from_this();
                        auto release_func = [weak_pool](Connection::Ptr released_conn) {
                            if (auto pool = weak_pool.lock()) {
                                pool->return_connection(std::move(released_conn));
                            } else {
                                std::cout << "Connection pool already released." << std::endl;
                            }
                        };
                        return std::make_unique<PoolConnectionHandle>(
                            std::move(new_conn), std::move(release_func));
                    }
                    // 创建失败，继续尝试获取
                    continue;
                } catch (const std::exception& e) {
                    std::cerr << "Connection test failed: " << e.what() << std::endl;
                    continue;
                }
            }
            
            return connHandle;
        }
        
        // 检查是否超时
        auto elapsed = std::chrono::steady_clock::now() - start_time;
        if (elapsed >= timeout) {
            std::cout << "Timeout waiting for database connection" << std::endl;
            waiting_requests_--;
            throw std::runtime_error("Timeout waiting for database connection");
        }
        
        // 等待一段时间再重试
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    waiting_requests_--;
    throw std::runtime_error("Connection pool is shutdown");
}

PoolConnectionHandle::Ptr ConnectionPool::borrow_from_pool() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // std::cout << "start borrow_from_pool idl_connections size: " << idle_connections_.size() << std::endl;
    if (!idle_connections_.empty()) {
        // 从空闲队列获取连接
        auto conn = std::move(idle_connections_.front());
        idle_connections_.pop();
        
        if (conn && conn->is_connected()) {
            // ✅ 修复：将连接插入active_connections_
            active_connections_++;
            
            auto weak_pool = weak_from_this();
            auto release_func = [weak_pool](Connection::Ptr released_conn) {
                if (auto pool = weak_pool.lock()) {
                    pool->return_connection(std::move(released_conn));
                } else {
                    std::cout << "Connection pool already released." << std::endl;
                }
            };
            
            return std::make_unique<PoolConnectionHandle>(
                std::move(conn), std::move(release_func));
        } else {
            // 连接无效，减少计数
            std::cout << "Connection invalid, reducing count." << std::endl;
            if (conn) {
                active_connections_--;
            }
            total_connections_--;
        }
    }

    // 尝试创建新连接
    if (total_connections_ < config_.max_connections) {
        try {
            auto conn = create_connection();
            if (conn && conn->is_connected()) {
                active_connections_++;
                total_connections_++;
                
                auto weak_pool = weak_from_this();
                auto release_func = [weak_pool](Connection::Ptr released_conn) {
                    if (auto pool = weak_pool.lock()) {
                        pool->return_connection(std::move(released_conn));
                    } else {
                        std::cout << "Connection pool already released." << std::endl;
                    }
                };
                
                return std::make_unique<PoolConnectionHandle>(
                    std::move(conn), std::move(release_func));
            }
        } catch (const std::exception&) {
            // 创建失败，返回空指针
        }
    }
    
    return nullptr;
}

void ConnectionPool::return_connection(std::unique_ptr<Connection> conn) {
    if (!conn) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(mutex_);
    
    active_connections_--;
    if (shutdown_) {
        // 如果连接池已关闭，直接释放连接
        total_connections_--;
        return;
    }
    
    // 检查连接是否仍然有效
    if (config_.test_on_return && !conn->is_connected()) {
        total_connections_--;
        return; // 连接无效，直接销毁
    }
    
    // 检查连接是否空闲超时
    // if (conn->is_idle_timeout(config_.max_idle_time)) {
    //     total_connections_--;
    //     return; // 连接超时，直接销毁
    // }
    
    // 将连接放回空闲队列
    // conn->update_last_used();

    // std::cout << "resturn_connection idle_connections_ size: " << idle_connections_.size() << std::endl;
    idle_connections_.push(std::move(conn));
    
    // 通知等待的线程
    condition_.notify_one();
}

std::unique_ptr<Connection> ConnectionPool::create_connection() {
    try {
        auto conn = std::make_unique<Connection>(config_.connection_config);
        return conn;
    } catch (const std::exception& e) {
        std::cerr << "Failed to create connection: " << e.what() << std::endl;
        throw;
    }
}

ConnectionPool::PoolStatus ConnectionPool::get_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    return PoolStatus{
        .total_connections = total_connections_,
        .idle_connections = idle_connections_.size(),
        .active_connections = active_connections_,
        .waiting_requests = waiting_requests_
    };
}

void ConnectionPool::health_check_task() {
    while (!shutdown_) {
        std::this_thread::sleep_for(
            std::chrono::seconds(config_.validation_interval));
        
        if (shutdown_) break;
        
        std::lock_guard<std::mutex> lock(mutex_);
        
        // 检查空闲连接的健康状态
        std::queue<std::unique_ptr<Connection>> healthy_connections;
        size_t invalid_count = 0;
        
        while (!idle_connections_.empty()) {
            auto conn = std::move(idle_connections_.front());
            idle_connections_.pop();
            
            if (conn->is_connected()) {
                healthy_connections.push(std::move(conn));
            } else {
                invalid_count++;
                total_connections_--;
            }
        }
        
        idle_connections_ = std::move(healthy_connections);
        
        if (invalid_count > 0) {
            std::cout << "Health check: Removed " << invalid_count 
                      << " invalid connections" << std::endl;
        }
    }
}

} // namespace odbc