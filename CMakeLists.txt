cmake_minimum_required(VERSION 3.25)
project(Koro)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Choose the build type" FORCE)
endif()

if(NOT LOG_LEVEL)
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(LOG_LEVEL "Debug" CACHE STRING "elog level")
    elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
        set(LOG_LEVEL "INFO" CACHE STRING "elog level")
    else()
        set(LOG_LEVEL "INFO" CACHE STRING "elog level")
    endif()
endif()

set_property(CACHE LOG_LEVEL PROPERTY STRINGS
    TRACE DEBUG INFO WARN ERROR FATAL
)

message(STATUS "Current logger level: ${LOG_LEVEL}")
add_definitions(-DELOG_LEVEL_SETTING=LYNX_${LOG_LEVEL})
message(STATUS "cmake build mode: ${CMAKE_BUILD_TYPE}")

add_subdirectory(elog)
add_subdirectory(koro)
add_subdirectory(test)

