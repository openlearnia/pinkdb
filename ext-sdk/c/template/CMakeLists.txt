# CMake build for the pinkdb extension skeleton (alternative to the Makefile).
#
#   cmake -S . -B build && cmake --build build
#   # -> build/my_extension.dylib  (.so on Linux)
#
# The `PREFIX ""` property drops CMake's default `lib` prefix so the artifact
# is `my_extension.dylib`, matching the Makefile's output.
cmake_minimum_required(VERSION 3.16)
project(my_extension C)

add_library(my_extension SHARED my_extension.c)

target_include_directories(my_extension PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

set_target_properties(my_extension PROPERTIES
    PREFIX ""
    C_STANDARD 99
    C_STANDARD_REQUIRED ON
)

# The ABI header and the skeleton must build warning-free.
target_compile_options(my_extension PRIVATE -Wall -Wextra)
