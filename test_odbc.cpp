#include <sql.h>
#include <sqlext.h>
#include <iostream>
#include <string>

int main() {
    SQLHENV env;
    SQLHDBC dbc;
    SQLRETURN ret;
    
    // 分配环境句柄
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    
    // 分配连接句柄
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    
    // 连接字符串
    std::string conn_str = "DRIVER={MariaDB};"
                          "SERVER=127.0.0.1;"
                          "DATABASE=testdb;"
                          "UID=sdba;"
                          "PWD=123456;"
                          "PORT=3306;"
                          "CHARSET=utf8;"
                          "OPTION=3;";
    
    // 尝试连接
    SQLCHAR outstr[1024];
    SQLSMALLINT outstrlen;
    ret = SQLDriverConnect(dbc, NULL, (SQLCHAR*)conn_str.c_str(), SQL_NTS, 
                          outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);
    
    if (SQL_SUCCEEDED(ret)) {
        std::cout << "Connected successfully!" << std::endl;
        SQLDisconnect(dbc);
    } else {
        SQLCHAR sqlstate[6], message[1024];
        SQLINTEGER native_error;
        SQLSMALLINT msglen;
        SQLError(SQL_NULL_HENV, SQL_NULL_HDBC, SQL_NULL_HSTMT, 
                sqlstate, &native_error, message, sizeof(message), &msglen);
        std::cerr << "Failed to connect: " << message 
                  << " SQL State: " << sqlstate 
                  << " Native Error: " << native_error << std::endl;
    }
    
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
    return 0;
}