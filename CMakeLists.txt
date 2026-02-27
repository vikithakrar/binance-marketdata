cmake_minimum_required(VERSION 3.20)
project(binance_marketdata_connector LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Boost 1.74 REQUIRED COMPONENTS system json)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_library(binance_connector
    src/connector.cpp
)

target_include_directories(binance_connector
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(binance_connector
    PUBLIC
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
        Boost::json
        Threads::Threads
)

add_executable(binance_connector_demo src/main.cpp)
target_link_libraries(binance_connector_demo PRIVATE binance_connector)
