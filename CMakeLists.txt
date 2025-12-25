cmake_minimum_required(VERSION 3.10)
project(SociPoolCpp)

# 设置C++11标准
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")

# 查找SOCI
find_package(SOCI REQUIRED)
find_package(SOCI_MYSQL)
find_package(SOCI_SQLITE3)

# 包含目录
include_directories(${SOCI_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# 可执行文件
add_executable(soci_pool_demo
    example_cpp.cpp
)

# 链接库
target_link_libraries(soci_pool_demo
    SOCI::soci_core
    ${SOCI_MYSQL_LIBRARIES}
    ${SOCI_SQLITE3_LIBRARIES}
    pthread
)

# 编译为库
add_library(soci_pool_cpp STATIC
    # 如果有对应的cpp文件
)

target_link_libraries(soci_pool_cpp
    SOCI::soci_core
    pthread
)