#ifndef __ODBC_WRAPPER_H__
#define __ODBC_WRAPPER_H__

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <memory>
#include <type_traits>
#include <chrono>
#include <stdexcept>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <thread>
#include <unordered_map>

#include <boost/any.hpp>

namespace odbc {

// ODBC异常类
class OdbcException : public std::runtime_error {
public:
    OdbcException(const std::string& message, SQLSMALLINT handle_type, SQLHANDLE handle)
        : std::runtime_error(build_message(message, handle_type, handle)) {}
    
private:
    static std::string build_message(const std::string& message, 
                                    SQLSMALLINT handle_type, 
                                    SQLHANDLE handle) {
        SQLCHAR sql_state[6];
        SQLCHAR error_msg[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER native_error = 0;
        SQLSMALLINT msg_len = 0;
        SQLRETURN ret;
        
        std::ostringstream oss;
        oss << message;
        
        int rec_num = 1;
        while ((ret = SQLGetDiagRec(handle_type, handle, rec_num, 
                                   sql_state, &native_error,
                                   error_msg, sizeof(error_msg), &msg_len)) 
               == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
            oss << "\n  SQL State: " << sql_state 
                << ", Native Error: " << native_error
                << ", Message: " << error_msg;
            rec_num++;
        }
        
        return oss.str();
    }
};

// RAII句柄包装器
template<SQLSMALLINT HandleType>
class OdbcHandle {
public:
    explicit OdbcHandle(SQLHANDLE parent_handle = SQL_NULL_HANDLE) 
        : handle_(SQL_NULL_HANDLE) {
        
        SQLRETURN ret = SQLAllocHandle(HandleType, parent_handle, &handle_);
        if (!SQL_SUCCEEDED(ret) || handle_ == SQL_NULL_HANDLE) {
            throw OdbcException("Failed to allocate ODBC handle", 
                               HandleType, parent_handle);
        }
    }
    
    ~OdbcHandle() {
        if (handle_ != SQL_NULL_HANDLE) {
            SQLFreeHandle(HandleType, handle_);
        }
    }
    
    // 禁止拷贝
    OdbcHandle(const OdbcHandle&) = delete;
    OdbcHandle& operator=(const OdbcHandle&) = delete;
    
    // 允许移动
    OdbcHandle(OdbcHandle&& other) noexcept 
        : handle_(other.handle_) {
        other.handle_ = SQL_NULL_HANDLE;
    }
    
    OdbcHandle& operator=(OdbcHandle&& other) noexcept {
        if (this != &other) {
            if (handle_ != SQL_NULL_HANDLE) {
                SQLFreeHandle(HandleType, handle_);
            }
            handle_ = other.handle_;
            other.handle_ = SQL_NULL_HANDLE;
        }
        return *this;
    }
    
    SQLHANDLE get() const noexcept { return handle_; }
    operator SQLHANDLE() const noexcept { return handle_; }
    
    // 检查返回码
    void check(SQLRETURN ret, const std::string& operation) const {
        if (!SQL_SUCCEEDED(ret)) {
            throw OdbcException("ODBC operation failed: " + operation, 
                               HandleType, handle_);
        }
    }
    
private:
    SQLHANDLE handle_;
};

// 类型别名
using EnvironmentHandle = OdbcHandle<SQL_HANDLE_ENV>;
using ConnectionHandle = OdbcHandle<SQL_HANDLE_DBC>;
using StatementHandle = OdbcHandle<SQL_HANDLE_STMT>;

// 值类型包装
class Value {
public:
    enum class Type {
        Null,
        Integer,
        Long,
        Double,
        String,
        Timestamp,
        Boolean
    };
    
    Value() : type_(Type::Null) {}
    
    explicit Value(SQLINTEGER val) 
        : type_(Type::Integer), int_val_(val) {}
    
    explicit Value(SQLBIGINT val) 
        : type_(Type::Long), long_val_(val) {}
    
    explicit Value(double val) 
        : type_(Type::Double), double_val_(val) {}
    
    explicit Value(const std::string& val) 
        : type_(Type::String), str_val_(val) {}
    
    explicit Value(const char* val) 
        : type_(Type::String), str_val_(val) {}
    
    explicit Value(bool val) 
        : type_(Type::Boolean), bool_val_(val) {}
    
    explicit Value(const std::chrono::system_clock::time_point& tp)
        : type_(Type::Timestamp), timestamp_val_(tp) {}
    
    // 转换为目标类型
    template<typename T>
    T as() const;
    
    Type type() const { return type_; }
    bool is_null() const { return type_ == Type::Null; }
    
private:
    Type type_;
    union {
        SQLINTEGER int_val_;
        SQLBIGINT long_val_;
        double double_val_;
        bool bool_val_;
        std::chrono::system_clock::time_point timestamp_val_;
    };
    std::string str_val_;
};

// 结果集行
class Row {
public:
    Row() = default;
    
    void add_column(const std::string& name, const Value& value) {
        columns_.push_back(value);
        column_names_.push_back(name);
    }
    
    Value get(size_t index) const {
        if (index >= columns_.size()) {
            throw std::out_of_range("Column index out of range");
        }
        return columns_[index];
    }
    
    Value get(const std::string& name) const {
        for (size_t i = 0; i < column_names_.size(); ++i) {
            if (column_names_[i] == name) {
                return columns_[i];
            }
        }
        throw std::runtime_error("Column not found: " + name);
    }
    
    size_t size() const { return columns_.size(); }
    bool empty() const { return columns_.empty(); }
    
    // 转换为目标类型
    template<typename T>
    T get_as(size_t index) const {
        return get(index).as<T>();
    }
    
    template<typename T>
    T get_as(const std::string& name) const {
        return get(name).as<T>();
    }
    
private:
    std::vector<Value> columns_;
    std::vector<std::string> column_names_;
};

// 结果集
class ResultSet {
public:
    class Iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = Row;
        using difference_type = std::ptrdiff_t;
        using pointer = Row*;
        using reference = Row&;
        
        Iterator(ResultSet* rs, size_t index) 
            : result_set_(rs), index_(index) {}
        
        reference operator*() { return (*result_set_)[index_]; }
        pointer operator->() { return &(*result_set_)[index_]; }
        
        Iterator& operator++() {
            ++index_;
            return *this;
        }
        
        Iterator operator++(int) {
            Iterator tmp = *this;
            ++index_;
            return tmp;
        }
        
        bool operator==(const Iterator& other) const {
            return result_set_ == other.result_set_ && index_ == other.index_;
        }
        
        bool operator!=(const Iterator& other) const {
            return !(*this == other);
        }
        
    private:
        ResultSet* result_set_;
        size_t index_;
    };
    
    ResultSet() = default;
    
    void add_row(Row&& row) {
        rows_.push_back(std::move(row));
    }
    
    size_t size() const { return rows_.size(); }
    bool empty() const { return rows_.empty(); }
    
    Row& operator[](size_t index) {
        if (index >= rows_.size()) {
            throw std::out_of_range("Row index out of range");
        }
        return rows_[index];
    }
    
    const Row& operator[](size_t index) const {
        if (index >= rows_.size()) {
            throw std::out_of_range("Row index out of range");
        }
        return rows_[index];
    }
    
    Iterator begin() { return Iterator(this, 0); }
    Iterator end() { return Iterator(this, rows_.size()); }
    
    // 获取第一行第一列的值
    template<typename T>
    T scalar() const {
        if (rows_.empty() || rows_[0].empty()) {
            throw std::runtime_error("No data in result set");
        }
        return rows_[0].get_as<T>(0);
    }
    
private:
    std::vector<Row> rows_;
};

/**
 * @brief 数据库类型枚举
 * 
 * 当前仅支持主流关系型数据库
 */
enum class DatabaseType {
    // ==================== 关系型数据库 ====================
    UNKNOWN = 0,           ///< 未知数据库类型
    
    // MySQL系列
    MYSQL = 100,           ///< MySQL数据库
    MARIADB = 101,         ///< MariaDB数据库
    PERCONA = 102,         ///< Percona Server
    
    // PostgreSQL系列
    POSTGRESQL = 200,      ///< PostgreSQL数据库
    GREENPLUM = 201,       ///< Greenplum数据库
    COCKROACHDB = 202,     ///< CockroachDB
    
    // SQL Server系列
    SQL_SERVER = 300,      ///< Microsoft SQL Server
    AZURE_SQL = 301,       ///< Azure SQL Database
    SYBASE = 302,          ///< Sybase ASE
    
    // Oracle系列
    ORACLE = 400,          ///< Oracle Database
    
    // SQLite系列
    SQLITE = 500,          ///< SQLite数据库
    
    // IBM系列
    DB2 = 600,             ///< IBM DB2
    INFORMIX = 601,        ///< IBM Informix
    
    // 其他关系型数据库
    CLICKHOUSE = 700,      ///< ClickHouse
    VERTICA = 701,         ///< Vertica
    SNOWFLAKE = 702,       ///< Snowflake
};

// 连接配置
struct ConnectionConfig {
    std::string driver;
    std::string username;
    std::string password;
    std::string database;
    std::string host;
    std::string charset;
    unsigned int port = 0;
    unsigned int timeout = 30;  // 连接超时(秒)
    bool auto_commit = true;
    bool ssl = false;
    DatabaseType databaseType = DatabaseType::UNKNOWN;
    
    // 构建连接字符串
    std::string to_connection_string() const {
        std::ostringstream oss;
        if (databaseType == DatabaseType::UNKNOWN) {
            throw std::runtime_error("database type is unknown.");
        }
        
        if (!driver.empty()) {
            oss << "DRIVER={" << driver << "};";
        }
        if (!host.empty()) {
            oss << "SERVER=" << host << ";";
        }
        if (port > 0) {
            oss << "PORT=" << port << ";";
        }
        if (!database.empty()) {
            oss << "DATABASE=" << database << ";";
        }
        if (!username.empty()) {
            oss << "UID=" << username << ";";
        }
        if (!password.empty()) {
            oss << "PWD=" << password << ";";
        }

        // 添加可选参数
        if (!charset.empty()) {
            oss << "CHARSET=" << charset << ";";
        }
        if (timeout > 0) {
            oss << "ConnectionTimeout=" << timeout << ";";
        }
        if (ssl) {
            oss << "SSL Mode=REQUIRED;";
        }
        
        // 数据库特定选项
        if (databaseType == DatabaseType::MYSQL || databaseType == DatabaseType::MARIADB) {
            oss << "OPTION=3;";  // 启用多语句和长缓冲区
        } else if (databaseType == DatabaseType::POSTGRESQL) {
            oss << "sslmode=require;";
        }
        
        return oss.str();
    }
};

// 主连接类
class Connection {
public:
    Connection() = default;
    
    explicit Connection(const ConnectionConfig& config) {
        connect(config);
    }
    
    ~Connection() {
        if (connected_) {
            try {
                disconnect();
            } catch (...) {
                // 忽略析构时的异常
            }
        }
    }
    
    // 禁止拷贝
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    
    // 允许移动
    Connection(Connection&& other) noexcept
        : env_handle_(std::move(other.env_handle_))
        , conn_handle_(std::move(other.conn_handle_))
        , connected_(other.connected_)
        , auto_commit_(other.auto_commit_) {
        other.connected_ = false;
    }
    
    Connection& operator=(Connection&& other) noexcept {
        if (this != &other) {
            if (connected_) {
                disconnect();
            }
            env_handle_ = std::move(other.env_handle_);
            conn_handle_ = std::move(other.conn_handle_);
            connected_ = other.connected_;
            auto_commit_ = other.auto_commit_;
            other.connected_ = false;
        }
        return *this;
    }
    
    // 连接到数据库
    void connect(const ConnectionConfig& config) {
        try {
            // 1. 创建环境句柄
            env_handle_ = std::make_unique<EnvironmentHandle>();
            
            // 2. 设置ODBC版本
            env_handle_->check(
                SQLSetEnvAttr(env_handle_->get(), SQL_ATTR_ODBC_VERSION, 
                            (void*)SQL_OV_ODBC3, 0),
                "Set ODBC version"
            );
            
            // 3. 创建连接句柄
            conn_handle_ = std::make_unique<ConnectionHandle>(env_handle_->get());
            
            // 4. 设置连接超时
            SQLSetConnectAttr(conn_handle_->get(), SQL_LOGIN_TIMEOUT, 
                            (SQLPOINTER)(long)config.timeout, 0);
            
            // 5. 建立连接
            std::string conn_str = config.to_connection_string();
            conn_str = "DRIVER={MariaDB};"
                          "SERVER=127.0.0.1;"
                          "DATABASE=testdb;"
                          "UID=sdba;"
                          "PWD=123456;"
                          "PORT=3306;"
                          "CHARSET=utf8;"
                          "OPTION=3;";
            SQLCHAR outstr[1024];
            SQLSMALLINT outstrlen;
            SQLRETURN ret = SQLDriverConnect(
                conn_handle_->get(),
                nullptr,
                (SQLCHAR*)conn_str.c_str(),
                SQL_NTS,
                outstr, 
                sizeof(outstr), 
                &outstrlen,
                SQL_DRIVER_COMPLETE
            );

            if (!SQL_SUCCEEDED(ret)) {
                throw OdbcException("Failed to connect to database", 
                                   SQL_HANDLE_DBC, conn_handle_->get());
            }
            connected_ = true;
            auto_commit_ = config.auto_commit;
            // 6. 设置自动提交
            set_auto_commit(config.auto_commit);
            
            
            std::cout << "Connected to database successfully" << std::endl;
            
        } catch (const std::exception& e) {
            connected_ = false;
            throw;
        }
    }
    
    // 断开连接
    void disconnect() {
        if (!connected_) return;
        
        try {
            if (conn_handle_) {
                SQLDisconnect(conn_handle_->get());
            }
            connected_ = false;
            std::cout << "Disconnected from database" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error during disconnect: " << e.what() << std::endl;
            throw;
        }
    }
    
    // 执行SQL语句（无结果）
    size_t execute(const std::string& sql) {
        if (!connected_) {
            throw std::runtime_error("Not connected to database");
        }
        
        StatementHandle stmt(conn_handle_->get());
        
        stmt.check(
            SQLExecDirect(stmt.get(), (SQLCHAR*)sql.c_str(), SQL_NTS),
            "Execute SQL: " + sql
        );
        
        // 获取影响的行数
        SQLLEN row_count = 0;
        SQLRowCount(stmt.get(), &row_count);
        
        return static_cast<size_t>(row_count);
    }
    
    // 执行查询
    ResultSet query(const std::string& sql) {
        if (!connected_) {
            throw std::runtime_error("Not connected to database");
        }
        
        StatementHandle stmt(conn_handle_->get());
        
        stmt.check(
            SQLExecDirect(stmt.get(), (SQLCHAR*)sql.c_str(), SQL_NTS),
            "Execute query: " + sql
        );
        
        return fetch_results(stmt);
    }
    
    // 预备语句执行
    class PreparedStatement {
    public:
        PreparedStatement(Connection* conn, const std::string& sql)
            : conn_(conn)
            , stmt_(conn->conn_handle_->get())
            , param_count_(0) {
            
            // 准备语句
            stmt_.check(
                SQLPrepare(stmt_.get(), (SQLCHAR*)sql.c_str(), SQL_NTS),
                "Prepare statement: " + sql
            );
            
            // 获取参数个数
            SQLNumParams(stmt_.get(), &param_count_);
            param_values_.resize(param_count_);
            param_lengths_.resize(param_count_, SQL_NTS);
        }
        
        // 绑定参数
        template<typename T>
        void bind_param(SQLUSMALLINT index, const T& value) {
            if (index < 1 || index > param_count_) {
                throw std::out_of_range("Parameter index out of range");
            }
            bind_impl(index, value);
        }
        
        // 执行
        size_t execute() {
            stmt_.check(
                SQLExecute(stmt_.get()),
                "Execute prepared statement"
            );
            
            SQLLEN row_count = 0;
            SQLRowCount(stmt_.get(), &row_count);
            return static_cast<size_t>(row_count);
        }
        
        // 执行查询
        ResultSet execute_query() {
            execute();
            return conn_->fetch_results(stmt_);
        }
        
    private:
        template<typename T>
        void bind_impl(SQLUSMALLINT index, const T& value,
                      typename std::enable_if<std::is_integral<T>::value>::type* = 0) {
            param_values_[index - 1] = static_cast<SQLINTEGER>(value);
            stmt_.check(
                SQLBindParameter(stmt_.get(), index, SQL_PARAM_INPUT,
                               SQL_C_SLONG, SQL_INTEGER, 0, 0,
                               &param_values_[index - 1], 0, nullptr),
                "Bind integer parameter"
            );
        }
        
        void bind_impl(SQLUSMALLINT index, const std::string& value) {
            param_values_[index - 1] = value;
            param_lengths_[index - 1] = value.length();
            stmt_.check(
                SQLBindParameter(stmt_.get(), index, SQL_PARAM_INPUT,
                               SQL_C_CHAR, SQL_VARCHAR, value.length(), 0,
                               (SQLPOINTER)value.c_str(), value.length(), 
                               &param_lengths_[index - 1]),
                "Bind string parameter"
            );
        }
        
        void bind_impl(SQLUSMALLINT index, double value) {
            param_values_[index - 1] = value;
            stmt_.check(
                SQLBindParameter(stmt_.get(), index, SQL_PARAM_INPUT,
                               SQL_C_DOUBLE, SQL_DOUBLE, 0, 0,
                               &param_values_[index - 1], 0, nullptr),
                "Bind double parameter"
            );
        }
        
        Connection* conn_;
        StatementHandle stmt_;
        SQLSMALLINT param_count_;
        std::vector<boost::any> param_values_;
        std::vector<SQLLEN> param_lengths_;
    };
    
    std::unique_ptr<PreparedStatement> prepare(const std::string& sql) {
        return std::make_unique<PreparedStatement>(this, sql);
    }
    
    // 开始事务
    void begin_transaction() {
        if (!connected_) {
            throw std::runtime_error("Not connected to database");
        }
        
        if (auto_commit_) {
            set_auto_commit(false);
        }
    }
    
    // 提交事务
    void commit() {
        if (!connected_) {
            throw std::runtime_error("Not connected to database");
        }
        
        SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, conn_handle_->get(), SQL_COMMIT);
        if (!SQL_SUCCEEDED(ret)) {
            throw OdbcException("Failed to commit transaction", 
                               SQL_HANDLE_DBC, conn_handle_->get());
        }
        
        if (!auto_commit_) {
            set_auto_commit(true);
        }
    }
    
    // 回滚事务
    void rollback() {
        if (!connected_) {
            throw std::runtime_error("Not connected to database");
        }
        
        SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, conn_handle_->get(), SQL_ROLLBACK);
        if (!SQL_SUCCEEDED(ret)) {
            throw OdbcException("Failed to rollback transaction", 
                               SQL_HANDLE_DBC, conn_handle_->get());
        }
        
        if (!auto_commit_) {
            set_auto_commit(true);
        }
    }
    
    // 设置自动提交
    void set_auto_commit(bool enable) {
        if (!connected_) {
            throw std::runtime_error("Not connected to database");
        }
        
        SQLUINTEGER mode = enable ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;
        SQLRETURN ret = SQLSetConnectAttr(conn_handle_->get(), 
                                         SQL_ATTR_AUTOCOMMIT, 
                                         (SQLPOINTER)mode, 
                                         0);
        if (!SQL_SUCCEEDED(ret)) {
            throw OdbcException("Failed to set autocommit mode", 
                               SQL_HANDLE_DBC, conn_handle_->get());
        }
        auto_commit_ = enable;
    }
    
    bool is_connected() const { return connected_; }
    bool is_auto_commit() const { return auto_commit_; }
    
    // 获取数据库元数据
    std::vector<std::string> get_tables() {
        if (!connected_) {
            throw std::runtime_error("Not connected to database");
        }
        
        StatementHandle stmt(conn_handle_->get());
        std::vector<std::string> tables;
        
        // 获取表信息
        stmt.check(
            SQLTables(stmt.get(), nullptr, 0, nullptr, 0, 
                     nullptr, 0, (SQLCHAR*)"TABLE", SQL_NTS),
            "Get tables"
        );
        
        SQLCHAR table_name[256];
        SQLLEN name_len = 0;
        
        while (SQLFetch(stmt.get()) == SQL_SUCCESS) {
            SQLGetData(stmt.get(), 3, SQL_C_CHAR, table_name, 
                      sizeof(table_name), &name_len);
            if (name_len > 0) {
                tables.push_back(reinterpret_cast<char*>(table_name));
            }
        }
        
        return tables;
    }
    
    // 测试连接
    bool ping() {
        if (!connected_) return false;
        
        try {
            execute("SELECT 1");
            return true;
        } catch (...) {
            return false;
        }
    }
    
private:
    // 获取结果集
    ResultSet fetch_results(StatementHandle& stmt) {
        ResultSet result_set;
        
        // 获取列数
        SQLSMALLINT column_count = 0;
        SQLNumResultCols(stmt.get(), &column_count);
        
        if (column_count == 0) {
            return result_set;  // 无结果集
        }
        
        // 获取列信息
        std::vector<std::string> column_names;
        std::vector<SQLSMALLINT> column_types;
        
        for (SQLSMALLINT i = 1; i <= column_count; ++i) {
            SQLCHAR column_name[256];
            SQLSMALLINT name_len = 0;
            SQLSMALLINT data_type = 0;
            SQLULEN column_size = 0;
            SQLSMALLINT decimal_digits = 0;
            SQLSMALLINT nullable = 0;
            
            SQLDescribeCol(stmt.get(), i, column_name, sizeof(column_name),
                          &name_len, &data_type, &column_size, 
                          &decimal_digits, &nullable);
            
            column_names.push_back(reinterpret_cast<char*>(column_name));
            column_types.push_back(data_type);
        }
        
        // 获取数据
        while (SQLFetch(stmt.get()) == SQL_SUCCESS) {
            Row row;
            
            for (SQLSMALLINT i = 1; i <= column_count; ++i) {
                // 检查是否为空
                SQLLEN indicator = 0;
                SQLGetData(stmt.get(), i, SQL_C_DEFAULT, nullptr, 0, &indicator);
                
                if (indicator == SQL_NULL_DATA) {
                    row.add_column(column_names[i-1], Value());
                    continue;
                }
                
                // 根据列类型获取数据
                switch (column_types[i-1]) {
                    case SQL_INTEGER:
                    case SQL_SMALLINT:
                    case SQL_TINYINT: {
                        SQLINTEGER int_val = 0;
                        SQLGetData(stmt.get(), i, SQL_C_SLONG, &int_val, 0, nullptr);
                        row.add_column(column_names[i-1], Value(int_val));
                        break;
                    }
                    case SQL_BIGINT: {
                        SQLBIGINT long_val = 0;
                        SQLGetData(stmt.get(), i, SQL_C_SBIGINT, &long_val, 0, nullptr);
                        row.add_column(column_names[i-1], Value(long_val));
                        break;
                    }
                    case SQL_DOUBLE:
                    case SQL_FLOAT:
                    case SQL_REAL: {
                        double double_val = 0.0;
                        SQLGetData(stmt.get(), i, SQL_C_DOUBLE, &double_val, 0, nullptr);
                        row.add_column(column_names[i-1], Value(double_val));
                        break;
                    }
                    case SQL_DECIMAL:
                    case SQL_NUMERIC: {
                        SQL_NUMERIC_STRUCT numeric_val;
                        SQLGetData(stmt.get(), i, SQL_C_NUMERIC, &numeric_val, 0, nullptr);
                        // 转换为double
                        double double_val = 0.0;
                        SQL_NUMERIC_STRUCT* num = &numeric_val;
                        double_val = 0.0;
                        for (int i = 0; i < SQL_MAX_NUMERIC_LEN; i++) {
                            double_val = double_val * 256 + num->val[i];
                        }
                        for (int i = 0; i < num->scale; i++) {
                            double_val /= 10.0;
                        }
                        if (num->sign == 0) {  // 负数
                            double_val = -double_val;
                        }
                        row.add_column(column_names[i-1], Value(double_val));
                        break;
                    }
                    case SQL_CHAR:
                    case SQL_VARCHAR:
                    case SQL_LONGVARCHAR: {
                        // 先获取长度
                        SQLLEN data_len = 0;
                        SQLGetData(stmt.get(), i, SQL_C_CHAR, nullptr, 0, &data_len);
                        
                        if (data_len > 0) {
                            std::vector<char> buffer(data_len + 1);
                            SQLGetData(stmt.get(), i, SQL_C_CHAR, buffer.data(), 
                                      buffer.size(), nullptr);
                            row.add_column(column_names[i-1], 
                                         Value(std::string(buffer.data())));
                        } else {
                            row.add_column(column_names[i-1], Value(""));
                        }
                        break;
                    }
                    case SQL_DATE:
                    case SQL_TYPE_DATE: {
                        DATE_STRUCT date_val;
                        SQLGetData(stmt.get(), i, SQL_C_DATE, &date_val, 0, nullptr);
                        // 转换为字符串
                        std::ostringstream oss;
                        oss << std::setfill('0')
                            << std::setw(4) << date_val.year << "-"
                            << std::setw(2) << date_val.month << "-"
                            << std::setw(2) << date_val.day;
                        row.add_column(column_names[i-1], Value(oss.str()));
                        break;
                    }
                    case SQL_TIMESTAMP:
                    case SQL_TYPE_TIMESTAMP: {
                        TIMESTAMP_STRUCT ts_val;
                        SQLGetData(stmt.get(), i, SQL_C_TIMESTAMP, &ts_val, 0, nullptr);
                        // 转换为字符串
                        std::ostringstream oss;
                        oss << std::setfill('0')
                            << std::setw(4) << ts_val.year << "-"
                            << std::setw(2) << ts_val.month << "-"
                            << std::setw(2) << ts_val.day << " "
                            << std::setw(2) << ts_val.hour << ":"
                            << std::setw(2) << ts_val.minute << ":"
                            << std::setw(2) << ts_val.second;
                        if (ts_val.fraction > 0) {
                            oss << "." << std::setw(9) << ts_val.fraction;
                        }
                        row.add_column(column_names[i-1], Value(oss.str()));
                        break;
                    }
                    case SQL_BIT: {
                        unsigned char bool_val = 0;
                        SQLGetData(stmt.get(), i, SQL_C_BIT, &bool_val, 0, nullptr);
                        row.add_column(column_names[i-1], Value(bool_val != 0));
                        break;
                    }
                    default: {
                        // 默认按字符串处理
                        char buffer[4096];
                        SQLLEN len = 0;
                        SQLGetData(stmt.get(), i, SQL_C_CHAR, buffer, 
                                  sizeof(buffer), &len);
                        if (len > 0) {
                            row.add_column(column_names[i-1], 
                                         Value(std::string(buffer, len)));
                        } else {
                            row.add_column(column_names[i-1], Value(""));
                        }
                        break;
                    }
                }
            }
            
            result_set.add_row(std::move(row));
        }
        
        return result_set;
    }
    
private:
    std::unique_ptr<EnvironmentHandle> env_handle_;
    std::unique_ptr<ConnectionHandle> conn_handle_;
    bool connected_ = false;
    bool auto_commit_ = true;
};

// Value类型的转换实现
template<>
inline int Value::as<int>() const {
    switch (type_) {
        case Type::Integer: return static_cast<int>(int_val_);
        case Type::Long: return static_cast<int>(long_val_);
        case Type::Double: return static_cast<int>(double_val_);
        case Type::Boolean: return bool_val_ ? 1 : 0;
        case Type::String: return std::stoi(str_val_);
        case Type::Null: throw std::runtime_error("Cannot convert NULL to int");
        default: throw std::runtime_error("Invalid type conversion to int");
    }
}

template<>
inline long long Value::as<long long>() const {
    switch (type_) {
        case Type::Integer: return static_cast<long long>(int_val_);
        case Type::Long: return long_val_;
        case Type::Double: return static_cast<long long>(double_val_);
        case Type::Boolean: return bool_val_ ? 1 : 0;
        case Type::String: return std::stoll(str_val_);
        case Type::Null: throw std::runtime_error("Cannot convert NULL to long long");
        default: throw std::runtime_error("Invalid type conversion to long long");
    }
}

template<>
inline double Value::as<double>() const {
    switch (type_) {
        case Type::Integer: return static_cast<double>(int_val_);
        case Type::Long: return static_cast<double>(long_val_);
        case Type::Double: return double_val_;
        case Type::Boolean: return bool_val_ ? 1.0 : 0.0;
        case Type::String: return std::stod(str_val_);
        case Type::Null: throw std::runtime_error("Cannot convert NULL to double");
        default: throw std::runtime_error("Invalid type conversion to double");
    }
}

template<>
inline std::string Value::as<std::string>() const {
    switch (type_) {
        case Type::Integer: return std::to_string(int_val_);
        case Type::Long: return std::to_string(long_val_);
        case Type::Double: return std::to_string(double_val_);
        case Type::Boolean: return bool_val_ ? "true" : "false";
        case Type::String: return str_val_;
        case Type::Timestamp: {
            // 将时间戳转换为字符串
            auto tt = std::chrono::system_clock::to_time_t(timestamp_val_);
            std::tm* tm = std::localtime(&tt);
            std::ostringstream oss;
            oss << std::put_time(tm, "%Y-%m-%d %H:%M:%S");
            return oss.str();
        }
        case Type::Null: return "NULL";
        default: throw std::runtime_error("Invalid type conversion to string");
    }
}

template<>
inline bool Value::as<bool>() const {
    switch (type_) {
        case Type::Integer: return int_val_ != 0;
        case Type::Long: return long_val_ != 0;
        case Type::Double: return double_val_ != 0.0;
        case Type::Boolean: return bool_val_;
        case Type::String: {
            std::string lower = str_val_;
            std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            return lower == "true" || lower == "1" || lower == "yes" || lower == "on";
        }
        case Type::Null: return false;
        default: throw std::runtime_error("Invalid type conversion to bool");
    }
}

template<>
inline std::chrono::system_clock::time_point Value::as<std::chrono::system_clock::time_point>() const {
    if (type_ != Type::Timestamp) {
        throw std::runtime_error("Cannot convert non-timestamp to time_point");
    }
    return timestamp_val_;
}

} // namespace odbc

#endif // __ODBC_WRAPPER_H__