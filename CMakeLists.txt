cmake_minimum_required(VERSION 3.15..3.29)

project(
  sql-compiler
  VERSION 0.0.1
  LANGUAGES CXX
)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_BINARY_DIR}")

option(SANITIZE_ADDRESS "Enable AddressSanitizer" OFF)
option(SANITIZE_UNDEFINED "Enable UndefinedBehaviorSanitizer" OFF)
option(SANITIZE_THREADS "Enable ThreadSanitizer" OFF)

add_compile_options(-Wno-deprecated-declarations)

if (SANITIZE_ADDRESS)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
endif()

if (SANITIZE_UNDEFINED)
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
endif()

if (SANITIZE_THREADS)
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
endif()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

set(FETCHCONTENT_QUIET OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(src/stewkk/sql)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  include(CTest)
  if(BUILD_TESTING)
    add_subdirectory(test)
  endif()
endif()
