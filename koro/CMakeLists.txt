# set(LIB_SOURCES)

file(GLOB LIB_SOURCES CMAKE_CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_library(koro_lib STATIC ${LIB_SOURCES})

target_link_libraries(koro_lib PUBLIC ${MYSQL_CONNECTOR_CPP})

target_include_directories(koro_lib 
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..
)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set_target_properties(koro_lib PROPERTIES DEBUG_POSTFIX "_d")
endif()