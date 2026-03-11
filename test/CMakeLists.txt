cmake_minimum_required(VERSION 3.25)
project(test)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(server hook_test2.cpp)
# add_executable(client client.cpp)

target_link_libraries(server PRIVATE koro elog)
# target_link_libraries(client PRIVATE koro elog)

target_compile_definitions(server PRIVATE 
    LYNX_WEB_SRC_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
)
# target_compile_definitions(client PRIVATE 
#     LYNX_WEB_SRC_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
# )