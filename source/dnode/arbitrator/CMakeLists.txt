aux_source_directory(src ARBITRATOR_SRC)
add_library(arbitrator STATIC ${ARBITRATOR_SRC})
target_include_directories(
    arbitrator
    PUBLIC "${TD_SOURCE_DIR}/include/dnode/arbitrator"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    arbitrator
    PRIVATE transport
    PRIVATE os
    PRIVATE common
    PRIVATE util
    PRIVATE qworker
    PRIVATE qcom
    PRIVATE executor
    PRIVATE index
)
