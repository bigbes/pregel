cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(pregel C)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

# Find Tarantool and Lua dependecies
set(TARANTOOL_FIND_REQUIRED ON)
find_package(Tarantool)
include_directories(${TARANTOOL_INCLUDE_DIRS})

# Set CFLAGS
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -Wextra")

add_subdirectory(pregel)

# enable_testing()
#
# set (LUA_PATH "LUA_PATH=${PROJECT_SOURCE_DIR}/?.lua\\;${PROJECT_SOURCE_DIR}/?/init.lua\\;\\;")
# set (LUA_SOURCE_DIR "LUA_SOURCE_DIR=${PROJECT_SOURCE_DIR}")
#
# add_test(pregel ${CMAKE_SOURCE_DIR}/test/pregel.test.lua)
#
# set_tests_properties(pregel PROPERTIES ENVIRONMENT "${LUA_PATH};${LUA_SOURCE_DIR}")
#
# # Add `make check`
# add_custom_target(check
#     WORKING_DIRECTORY ${PROJECT_BUILD_DIR}
#     COMMAND ctest -V)
