// odbc_mysql.cpp
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <iostream>
#include <string>
#include <vector>

class ODBCDatabase {
private:
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    
    void checkError(SQLRETURN ret, const std::string& operation) {
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            SQLCHAR sqlState[6], msg[SQL_MAX_MESSAGE_LENGTH];
            SQLINTEGER nativeError;
            SQLSMALLINT msgLen;
            
            SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlState, &nativeError, 
                         msg, sizeof(msg), &msgLen);
            std::cerr << operation << " failed: " << msg 
                      << " (SQL State: " << sqlState << ")" << std::endl;
        }
    }
    
public:
    ODBCDatabase() : env(nullptr), dbc(nullptr), stmt(nullptr) {}
    
    ~ODBCDatabase() {
        disconnect();
    }
    
    bool connect(const std::string& dsn, 
                 const std::string& user = "", 
                 const std::string& pass = "") {
        // 分配环境句柄
        if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env) != SQL_SUCCESS)
            return false;
            
        std::cout << "===========000============" << dsn << std::endl;
        // 设置ODBC版本
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
        std::cout << "===========111============" << dsn << std::endl;
        
        // 分配连接句柄
        if (SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc) != SQL_SUCCESS)
            return false;

        std::cout << "===========222============" << dsn << std::endl;
        
        // 建立连接
        SQLRETURN ret = SQLConnect(dbc, 
                                  (SQLCHAR*)dsn.c_str(), SQL_NTS,
                                  (SQLCHAR*)user.c_str(), SQL_NTS,
                                  (SQLCHAR*)pass.c_str(), SQL_NTS);
        
        std::cout << "===========333============" << dsn << std::endl;
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            checkError(ret, "Connection");
            return false;
        }
        
        std::cout << "===========444============" << dsn << std::endl;

        // 分配语句句柄
        if (SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt) != SQL_SUCCESS)
            return false;
            
        std::cout << "Connected to ODBC data source: " << dsn << std::endl;
        return true;
    }
    
    bool execute(const std::string& query) {
        SQLRETURN ret = SQLExecDirect(stmt, (SQLCHAR*)query.c_str(), SQL_NTS);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            checkError(ret, "Execute query: " + query);
            return false;
        }
        return true;
    }
    
    void printResults() {
        SQLCHAR name[256];
        SQLINTEGER id;
        SQLLEN nameLen, idLen;
        
        // 绑定结果列
        SQLBindCol(stmt, 1, SQL_C_LONG, &id, 0, &idLen);
        SQLBindCol(stmt, 2, SQL_C_CHAR, name, sizeof(name), &nameLen);
        
        // 获取每一行
        while (SQLFetch(stmt) == SQL_SUCCESS) {
            if (nameLen == SQL_NULL_DATA) {
                std::cout << "ID: " << id << ", Name: NULL" << std::endl;
            } else {
                std::cout << "ID: " << id << ", Name: " << name << std::endl;
            }
        }
        
        SQLCloseCursor(stmt);
    }
    
    void disconnect() {
        if (stmt) {
            SQLFreeHandle(SQL_HANDLE_STMT, stmt);
            stmt = nullptr;
        }
        if (dbc) {
            SQLDisconnect(dbc);
            SQLFreeHandle(SQL_HANDLE_DBC, dbc);
            dbc = nullptr;
        }
        if (env) {
            SQLFreeHandle(SQL_HANDLE_ENV, env);
            env = nullptr;
        }
    }
};

int main() {
    ODBCDatabase db;
    
    // 连接到ODBC数据源
    if (db.connect("MyMariaDB", "sdba", "123456")) {
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
        db.printResults();
        
        db.disconnect();
    }
    
    return 0;
}