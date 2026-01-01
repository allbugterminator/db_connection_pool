#include "odbc_connection_pool.h"
#include "odbc_wrapper.h"
#include <chrono>
#include <vector>
#include <thread>
#include <atomic>
#include <iomanip>
#include <sys/resource.h>

struct TestConfig {
    int total_queries;           // 总查询次数
    int max_threads;             // 最大线程数
    int connection_pool_size;     // 连接池大小
    bool use_connection_pool;    // 是否使用连接池
    std::string test_name;       // 测试名称

    // 连接字符串或配置
    odbc::ConnectionConfig connection_config;
};

class PerformanceMetrics {
public:
    std::atomic<long long> total_time_ms{0};
    std::atomic<int> success_count{0};
    std::atomic<int> error_count{0};
    std::chrono::steady_clock::time_point start_time;
    
    void start() {
        start_time = std::chrono::steady_clock::now();
    }
    
    void end() {
        auto end_time = std::chrono::steady_clock::now();
        total_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
    }
    
    void print_results(const TestConfig& config) {
        double qps = (success_count * 1000.0) / total_time_ms;
        std::cout << "\n=== " << config.test_name << " 测试结果 ===" << std::endl;
        std::cout << "总查询次数: " << config.total_queries << std::endl;
        std::cout << "成功次数: " << success_count << std::endl;
        std::cout << "失败次数: " << error_count << std::endl;
        std::cout << "总耗时: " << total_time_ms << " ms" << std::endl;
        std::cout << "QPS: " << std::fixed << std::setprecision(2) << qps << " 查询/秒" << std::endl;
        std::cout << "平均延迟: " << std::fixed << std::setprecision(2) 
                  << (total_time_ms * 1000.0 / success_count) << " μs" << std::endl;
    }
};

// 直接连接测试
class DirectConnectionTest {
public:
    static void run_test(const TestConfig& config, PerformanceMetrics& metrics) {
        std::vector<std::thread> threads;
        int queries_per_thread = config.total_queries / config.max_threads;
        
        metrics.start();
        
        for (int i = 0; i < config.max_threads; ++i) {
            threads.emplace_back([&, queries_per_thread]() {
                for (int j = 0; j < queries_per_thread; ++j) {
                    try {
                        auto start = std::chrono::steady_clock::now();
                        
                        // 每次创建新连接
                        odbc::Connection conn;
                        conn.connect(config.connection_config);
                        
                        // 执行简单查询
                        auto result = conn.query("SELECT 1 as test_value");
                        if (!result.empty()) {
                            metrics.success_count++;
                        } else {
                            metrics.error_count++;
                        }
                        
                        conn.disconnect();
                        
                    } catch (const std::exception& e) {
                        metrics.error_count++;
                    }
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        metrics.end();
    }
};

// 连接池测试
class ConnectionPoolTest {
public:
    static void run_test(const TestConfig& config, PerformanceMetrics& metrics) {
        // 配置连接池
        odbc::ConnectionPoolConfig pool_config;
        pool_config.min_connections = config.connection_pool_size;
        pool_config.max_connections = config.connection_pool_size;
        pool_config.connection_timeout = 30;
        pool_config.connection_config = config.connection_config;
        
        auto pool = std::make_shared<odbc::ConnectionPool>(pool_config);
        
        std::vector<std::thread> threads;
        int queries_per_thread = config.total_queries / config.max_threads;

        metrics.start();
        
        for (int i = 0; i < config.max_threads; ++i) {
            threads.emplace_back([&, queries_per_thread]() {
                for (int j = 0; j < queries_per_thread; ++j) {
                    try {
                        auto start = std::chrono::steady_clock::now();
                        
                        // 从连接池获取连接
                        auto conn = pool->get_connection();
                        
                        // 执行相同的查询
                        auto result = conn->query("SELECT 1 as test_value");
                        if (!result.empty()) {
                            metrics.success_count++;
                        } else {
                            metrics.error_count++;
                        }
                        
                        // 连接自动归还到池中
                    } catch (const std::exception& e) {
                        metrics.error_count++;
                    }
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        metrics.end();
        pool->shutdown();
    }
};

void print_comparison(const PerformanceMetrics& direct, const PerformanceMetrics& pool) {
    double direct_qps = (direct.success_count * 1000.0) / direct.total_time_ms;
    double pool_qps = (pool.success_count * 1000.0) / pool.total_time_ms;
    double improvement = ((pool_qps - direct_qps) / direct_qps) * 100;
    
    std::cout << "\n=== 性能对比分析 ===" << std::endl;
    std::cout << "连接池性能提升: " << std::fixed << std::setprecision(1) 
              << improvement << "%" << std::endl;
    std::cout << "吞吐量提升: " << std::fixed << std::setprecision(0)
              << (pool_qps - direct_qps) << " QPS" << std::endl;
    std::cout << "延迟降低: " << std::fixed << std::setprecision(1)
              << ((direct.total_time_ms - pool.total_time_ms) * 100.0 / direct.total_time_ms)
              << "%" << std::endl;
    
    // 输出性能对比表格
    std::cout << "\n性能指标对比:" << std::endl;
    std::cout << "┌──────────────────┬────────────┬────────────┬──────────┐" << std::endl;
    std::cout << "│ 指标             │ 直接连接   │ 连接池     │ 提升     │" << std::endl;
    std::cout << "├──────────────────┼────────────┼────────────┼──────────┤" << std::endl;
    std::cout << "│ 总耗时(ms)       │ " << std::setw(10) << direct.total_time_ms 
              << " │ " << std::setw(10) << pool.total_time_ms 
              << " │ " << std::setw(8) << std::fixed << std::setprecision(1)
              << (100.0 * (direct.total_time_ms - pool.total_time_ms) / direct.total_time_ms) 
              << "% │" << std::endl;
    std::cout << "│ 成功率           │ " << std::setw(10) << direct.success_count 
              << " │ " << std::setw(10) << pool.success_count 
              << " │ " << std::setw(8) << "N/A" << " │" << std::endl;
    std::cout << "│ QPS              │ " << std::setw(10) << std::fixed << std::setprecision(0) << direct_qps
              << " │ " << std::setw(10) << pool_qps
              << " │ " << std::setw(8) << std::fixed << std::setprecision(1) << improvement 
              << "% │" << std::endl;
    std::cout << "└──────────────────┴────────────┴────────────┴──────────┘" << std::endl;
}

enum class LoadTestType {
    LightLoadTest,
    MediumLoadTest,
    HeavyLoadTest,
};

// 负载测试
void load_test(LoadTestType loadType) {
    TestConfig config;
    config.connection_config.databaseType = odbc::DatabaseType::MARIADB;
    config.connection_config.driver = "MariaDB";
    config.connection_config.host = "127.0.0.1";
    config.connection_config.port = 3306;
    config.connection_config.username = "testuser";
    config.connection_config.password = "123456";
    config.connection_config.database = "testdb";
    config.connection_config.charset = "utf8";
    
    switch (loadType)
    {
    case LoadTestType::LightLoadTest:
        {
            config.total_queries = 1000;
            config.max_threads = 4;
            config.connection_pool_size = 10;
            config.test_name = "轻负载测试(1000次查询)";
        }
        break;

    case LoadTestType::MediumLoadTest:
        {
            config.total_queries = 5000;
            config.max_threads = 8;
            config.connection_pool_size = 15;
            config.test_name = "中等负载测试(5000次查询)";
        }
        break;

    case LoadTestType::HeavyLoadTest:
        {
            config.total_queries = 10000;
            config.max_threads = 16;
            config.connection_pool_size = 20;
            config.test_name = "高负载压力测试(10000次查询)";
        }
        break;
    
    default:
        break;
    }
    
    // 测试直接连接
    PerformanceMetrics direct_metrics;
    config.use_connection_pool = false;
    config.test_name += " - 直接连接";
    DirectConnectionTest::run_test(config, direct_metrics);
    direct_metrics.print_results(config);
    
    // 测试连接池
    PerformanceMetrics pool_metrics;
    config.use_connection_pool = true;
    config.test_name += " - 连接池";

    std::cout << "=========start to test pool=======" << std::endl;
    ConnectionPoolTest::run_test(config, pool_metrics);
    std::cout << "=========end to test pool=======" << std::endl;
    pool_metrics.print_results(config);
    
    // 性能对比
    print_comparison(direct_metrics, pool_metrics);
}

class ResourceMonitor {
public:
    static void print_memory_usage() {
        struct rusage usage;
        getrusage(RUSAGE_SELF, &usage);
        
        std::cout << "内存使用情况:" << std::endl;
        std::cout << "最大常驻集大小: " << usage.ru_maxrss / 1024 << " MB" << std::endl;
        std::cout << "页错误次数: " << usage.ru_majflt << std::endl;
    }
    
    static void start_monitor() {
        std::thread monitor_thread([]() {
            while (true) {
                std::this_thread::sleep_for(std::chrono::seconds(5));
                print_memory_usage();
            }
        });
        monitor_thread.detach();
    }
};

int main() {
    std::cout << "开始ODBC连接池全方位压力测试..." << std::endl;
    
    try {
        // 启动资源监控
        ResourceMonitor::start_monitor();
        
        // 执行不同负载测试
        load_test(LoadTestType::LightLoadTest);
        load_test(LoadTestType::MediumLoadTest);
        load_test(LoadTestType::HeavyLoadTest);
        
        std::cout << "\n=== 所有测试完成 ===" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "测试过程中发生错误: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
