cmake_minimum_required(VERSION 3.16)
project(fastdds_csv_app)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(fastcdr REQUIRED)
find_package(fastdds REQUIRED)
find_package(CURL REQUIRED)
find_package(fastdds_statistics_backend REQUIRED)

# ──────────────────────────────────────────────────────────────────────────────
# IDL files
# ──────────────────────────────────────────────────────────────────────────────

set(IDL_AIR_SAMPLE ${CMAKE_CURRENT_SOURCE_DIR}/idl/AirSample.idl)
set(IDL_AIR_STATS  ${CMAKE_CURRENT_SOURCE_DIR}/idl/AirQualityStats.idl)
set(GENERATED_DIR  ${CMAKE_CURRENT_BINARY_DIR}/generated)

file(MAKE_DIRECTORY ${GENERATED_DIR})

# ── Generate AirSample ────────────────────────────────────────────────────────
add_custom_command(
    OUTPUT
        ${GENERATED_DIR}/AirSample.hpp
        ${GENERATED_DIR}/AirSamplePubSubTypes.cxx
        ${GENERATED_DIR}/AirSamplePubSubTypes.hpp
        ${GENERATED_DIR}/AirSampleTypeObjectSupport.cxx
        ${GENERATED_DIR}/AirSampleTypeObjectSupport.hpp
    COMMAND fastddsgen -replace -d ${GENERATED_DIR} ${IDL_AIR_SAMPLE}
    DEPENDS ${IDL_AIR_SAMPLE}
    COMMENT "Generating Fast DDS types from AirSample.idl"
)

add_custom_target(generate_airsample_types ALL
    DEPENDS
        ${GENERATED_DIR}/AirSample.hpp
        ${GENERATED_DIR}/AirSamplePubSubTypes.cxx
        ${GENERATED_DIR}/AirSamplePubSubTypes.hpp
        ${GENERATED_DIR}/AirSampleTypeObjectSupport.cxx
        ${GENERATED_DIR}/AirSampleTypeObjectSupport.hpp
)

# ── Generate AirQualityStats ──────────────────────────────────────────────────
add_custom_command(
    OUTPUT
        ${GENERATED_DIR}/AirQualityStats.hpp
        ${GENERATED_DIR}/AirQualityStatsPubSubTypes.cxx
        ${GENERATED_DIR}/AirQualityStatsPubSubTypes.hpp
        ${GENERATED_DIR}/AirQualityStatsTypeObjectSupport.cxx
        ${GENERATED_DIR}/AirQualityStatsTypeObjectSupport.hpp
    COMMAND fastddsgen -replace -d ${GENERATED_DIR} ${IDL_AIR_STATS}
    DEPENDS ${IDL_AIR_STATS}
    COMMENT "Generating Fast DDS types from AirQualityStats.idl"
)

add_custom_target(generate_airstats_types ALL
    DEPENDS
        ${GENERATED_DIR}/AirQualityStats.hpp
        ${GENERATED_DIR}/AirQualityStatsPubSubTypes.cxx
        ${GENERATED_DIR}/AirQualityStatsPubSubTypes.hpp
        ${GENERATED_DIR}/AirQualityStatsTypeObjectSupport.cxx
        ${GENERATED_DIR}/AirQualityStatsTypeObjectSupport.hpp
)

# ──────────────────────────────────────────────────────────────────────────────
# Executables
# ──────────────────────────────────────────────────────────────────────────────

add_executable(csv_publisher
    src/csv_publisher.cpp
    ${GENERATED_DIR}/AirSamplePubSubTypes.cxx
    ${GENERATED_DIR}/AirSampleTypeObjectSupport.cxx
)

add_executable(csv_subscriber
    src/csv_subscriber.cpp
    ${GENERATED_DIR}/AirSamplePubSubTypes.cxx
    ${GENERATED_DIR}/AirSampleTypeObjectSupport.cxx
)

add_executable(csv_intermediate_store
    src/csv_intermediate_store.cpp
    ${GENERATED_DIR}/AirSamplePubSubTypes.cxx
    ${GENERATED_DIR}/AirSampleTypeObjectSupport.cxx
)

add_executable(csv_analysis_aggregator
    src/csv_analysis_aggregator.cpp
    ${GENERATED_DIR}/AirSamplePubSubTypes.cxx
    ${GENERATED_DIR}/AirSampleTypeObjectSupport.cxx
    ${GENERATED_DIR}/AirQualityStatsPubSubTypes.cxx
    ${GENERATED_DIR}/AirQualityStatsTypeObjectSupport.cxx
)

add_executable(csv_final_agg_subscriber
    src/csv_final_agg_subscriber.cpp
    ${GENERATED_DIR}/AirSamplePubSubTypes.cxx
    ${GENERATED_DIR}/AirSampleTypeObjectSupport.cxx
    ${GENERATED_DIR}/AirQualityStatsPubSubTypes.cxx
    ${GENERATED_DIR}/AirQualityStatsTypeObjectSupport.cxx
)

add_executable(monitor
    src/monitor.cpp
)

# ──────────────────────────────────────────────────────────────────────────────
# Dependencies
# ──────────────────────────────────────────────────────────────────────────────

add_dependencies(csv_publisher generate_airsample_types)
add_dependencies(csv_subscriber generate_airsample_types)
add_dependencies(csv_intermediate_store generate_airsample_types)

add_dependencies(csv_analysis_aggregator
    generate_airsample_types
    generate_airstats_types
)

add_dependencies(csv_final_agg_subscriber
    generate_airsample_types
    generate_airstats_types
)

# monitor no depende de IDL

# ──────────────────────────────────────────────────────────────────────────────
# Includes
# ──────────────────────────────────────────────────────────────────────────────

target_include_directories(csv_publisher PRIVATE
    ${GENERATED_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_include_directories(csv_subscriber PRIVATE
    ${GENERATED_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_include_directories(csv_intermediate_store PRIVATE
    ${GENERATED_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_include_directories(csv_analysis_aggregator PRIVATE
    ${GENERATED_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_include_directories(csv_final_agg_subscriber PRIVATE
    ${GENERATED_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_include_directories(monitor PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# ──────────────────────────────────────────────────────────────────────────────
# Link
# ──────────────────────────────────────────────────────────────────────────────

target_link_libraries(csv_publisher PRIVATE fastdds fastcdr)
target_link_libraries(csv_subscriber PRIVATE fastdds fastcdr)
target_link_libraries(csv_intermediate_store PRIVATE fastdds fastcdr)
target_link_libraries(csv_analysis_aggregator PRIVATE fastdds fastcdr)
target_link_libraries(csv_final_agg_subscriber PRIVATE
    fastdds
    fastcdr
    CURL::libcurl
)

target_link_libraries(monitor PRIVATE
    fastdds
    fastcdr
    fastdds_statistics_backend
)