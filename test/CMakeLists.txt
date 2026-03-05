cmake_minimum_required(VERSION 3.25)
project(Koro_test)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(Koro_test scheduler_test.cpp)

target_link_libraries(Koro_test PRIVATE koro_lib elog_lib)

target_compile_definitions(Koro_test PRIVATE 
    LYNX_WEB_SRC_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
)