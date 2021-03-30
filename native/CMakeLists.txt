cmake_minimum_required(VERSION 3.4...3.18)
project(dask_native)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS -D_GLIBCXX_USE_CXX11_ABI=1)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ../../dask_accelerated)

find_package(Arrow 3.0.0 CONFIG REQUIRED)

add_subdirectory(re2)

add_subdirectory(pybind11)
pybind11_add_module(dask_native src/NativeFilter.cpp)

target_link_libraries(dask_native PRIVATE arrow_shared re2)

target_compile_definitions(dask_native PRIVATE VERSION_INFO=1)
