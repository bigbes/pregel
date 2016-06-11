# Build
if (APPLE)
    set(CMAKE_SHARED_LINKER_FLAGS
        "${CMAKE_SHARED_LINKER_FLAGS} -undefined suppress -flat_namespace")
endif(APPLE)

add_library(avro-legacy SHARED legacy.c)
target_link_libraries(avro-legacy avro)
set_target_properties(avro-legacy
        PROPERTIES
            PREFIX ""
            SUFFIX ".so"
            OUTPUT_NAME "legacy"
)

install(TARGETS avro-legacy LIBRARY
    DESTINATION ${TARANTOOL_INSTALL_LIBDIR}/pregel/avro/)
install(FILES constants.lua DESTINATION ${TARANTOOL_INSTALL_LUADIR}/pregel/avro)
install(FILES init.lua      DESTINATION ${TARANTOOL_INSTALL_LUADIR}/pregel/avro)
install(FILES c.lua         DESTINATION ${TARANTOOL_INSTALL_LUADIR}/pregel/avro)
