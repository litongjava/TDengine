aux_source_directory(src MGMT_ARBITRATOR)
add_library(mgmt_arbitrator STATIC ${MGMT_ARBITRATOR})
target_include_directories(
    mgmt_arbitrator
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    mgmt_arbitrator node_util
)
