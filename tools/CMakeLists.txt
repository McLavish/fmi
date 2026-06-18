if(FMI_ENABLE_CRIU)
    add_executable(fmi_criu_supervisor criu_supervisor.cpp)
    set_target_properties(fmi_criu_supervisor PROPERTIES OUTPUT_NAME "fmi-criu-supervisor")
    target_link_libraries(fmi_criu_supervisor FMI)

    add_executable(fmi_migration_supervisor migration_supervisor.cpp)
    set_target_properties(fmi_migration_supervisor PROPERTIES OUTPUT_NAME "fmi-migration-supervisor")
    target_link_libraries(fmi_migration_supervisor FMI)
endif()
