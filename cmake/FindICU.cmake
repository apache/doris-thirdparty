if(NOT TARGET icu)
    message(STATUS "ICU target not found, checking for local sources...")

    set(ICU_ARCHIVE_URL "https://github.com/unicode-org/icu/archive/refs/tags/release-75-1.tar.gz")
    set(ICU_ARCHIVE "${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/release-75-1.tar.gz")
    set(ICU_EXTRACT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/3rdparty")
    set(ICU_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/icu/icu4c/source")
    set(ICU_INSTALL_DIR "${ICU_SOURCE_DIR}/install")
    set(ICU_INCLUDE_DIR "${ICU_INSTALL_DIR}/include")
    set(ICU_LIBRARY_DIR "${ICU_INSTALL_DIR}/lib")
    set(ICU_UC_LIB "${ICU_LIBRARY_DIR}/libicuuc.a")
    set(ICU_I18N_LIB "${ICU_LIBRARY_DIR}/libicui18n.a")
    set(ICU_DATA_LIB "${ICU_LIBRARY_DIR}/libicudata.a")

    message(STATUS "ICU source directory not found, downloading and building ICU...")

    if(NOT EXISTS "${ICU_UC_LIB}" OR NOT EXISTS "${ICU_I18N_LIB}" OR NOT EXISTS "${ICU_DATA_LIB}")
        message(STATUS "ICU libraries not found, downloading and building ICU...")

        # Download ICU archive
        if (NOT EXISTS "${ICU_ARCHIVE}")
            message(STATUS "Downloading ICU archive from GitHub...")
            execute_process(
                COMMAND wget -q "${ICU_ARCHIVE_URL}" -O "${ICU_ARCHIVE}"
                RESULT_VARIABLE download_result
            )

            # Check if the download was successful
            if(NOT download_result EQUAL 0)
                message(FATAL_ERROR "Failed to download ICU archive.")
            else()
                message(STATUS "Download complete: ${ICU_ARCHIVE}")
            endif()
        else()
            message(STATUS "ICU archive already exists: ${ICU_ARCHIVE}")
        endif()

        # Extract ICU archive
        message(STATUS "Extracting ICU archive to ${ICU_EXTRACT_DIR}...")
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E tar xzf "${ICU_ARCHIVE}"
            WORKING_DIRECTORY "${ICU_EXTRACT_DIR}"
            RESULT_VARIABLE extract_result
        )

        # Check if the extraction was successful
        if(NOT extract_result EQUAL 0)
            message(FATAL_ERROR "Failed to extract ICU archive: ${ICU_ARCHIVE}")
        else()
            message(STATUS "Extraction complete.")
        endif()

        # Rename the extracted directory
        set(ICU_EXTRACTED_DIR "${ICU_EXTRACT_DIR}/icu-release-75-1")
        set(ICU_RENAMED_DIR "${ICU_EXTRACT_DIR}/icu")

        # Check if the target directory already exists
        if (EXISTS "${ICU_RENAMED_DIR}")
            message(STATUS "Removing existing directory: ${ICU_RENAMED_DIR}...")
            file(REMOVE_RECURSE "${ICU_RENAMED_DIR}")
        endif()

        if (EXISTS "${ICU_EXTRACTED_DIR}")
            message(STATUS "Renaming extracted directory from ${ICU_EXTRACTED_DIR} to ${ICU_RENAMED_DIR}...")
            execute_process(
                COMMAND mv "${ICU_EXTRACTED_DIR}" "${ICU_RENAMED_DIR}"
                RESULT_VARIABLE rename_result
            )

            # Check if the renaming was successful
            if(NOT rename_result EQUAL 0)
                message(FATAL_ERROR "Failed to rename directory: ${ICU_EXTRACTED_DIR}")
            else()
                message(STATUS "Directory renamed to: ${ICU_RENAMED_DIR}")
            endif()
        else()
            message(FATAL_ERROR "Extracted directory not found: ${ICU_EXTRACTED_DIR}")
        endif()

        # Configure ICU
        message(STATUS "Configuring ICU...")
        execute_process(
            COMMAND 
                ./configure
                --prefix=${ICU_INSTALL_DIR}
                --disable-shared
                --enable-static
                --disable-samples
                --disable-tests
            WORKING_DIRECTORY "${ICU_SOURCE_DIR}"
            RESULT_VARIABLE configure_result
            OUTPUT_QUIET
            ERROR_QUIET
        )

        # Check if the configuration was successful
        if(NOT configure_result EQUAL 0)
            message(FATAL_ERROR "Failed to configure ICU.")
        else()
            message(STATUS "ICU configuration complete.")
        endif()

        # Build ICU
        message(STATUS "Building ICU...")
        execute_process(
            COMMAND make -j${CMAKE_BUILD_PARALLEL_LEVEL}
            WORKING_DIRECTORY "${ICU_SOURCE_DIR}"
            RESULT_VARIABLE build_result
            OUTPUT_QUIET
            ERROR_QUIET
        )

        # Check if the build was successful
        if(NOT build_result EQUAL 0)
            message(FATAL_ERROR "Failed to build ICU.")
        else()
            message(STATUS "ICU build complete.")
        endif()

        # Install ICU
        message(STATUS "Installing ICU to ${ICU_INSTALL_DIR}...")
        execute_process(
            COMMAND make install
            WORKING_DIRECTORY "${ICU_SOURCE_DIR}"
            RESULT_VARIABLE install_result
            OUTPUT_QUIET
            ERROR_QUIET
        )

        # Check if the install was successful
        if(NOT install_result EQUAL 0)
            message(FATAL_ERROR "Failed to install ICU.")
        else()
            message(STATUS "ICU installation complete.")
        endif()
    else()
        message(STATUS "Found pre-built ICU libraries at ${ICU_LIBRARY_DIR}")
    endif()

    # These lists of sources were generated from build log of the original ICU build system (configure + make).

    add_library(_icuuc STATIC IMPORTED)
    set_target_properties(_icuuc PROPERTIES
        IMPORTED_LOCATION "${ICU_LIBRARY_DIR}/libicuuc.a"
        INTERFACE_INCLUDE_DIRECTORIES "${ICU_INCLUDE_DIR}"
    )

    add_library(_icui18n STATIC IMPORTED)
    set_target_properties(_icui18n PROPERTIES
        IMPORTED_LOCATION "${ICU_LIBRARY_DIR}/libicui18n.a"
    )

    add_library(_icudata STATIC IMPORTED)
    set_target_properties(_icudata PROPERTIES
        IMPORTED_LOCATION "${ICU_LIBRARY_DIR}/libicudata.a"
    )

    add_library(icu INTERFACE IMPORTED)
    target_link_libraries(icu INTERFACE _icui18n _icuuc _icudata)

    message(STATUS "ICU manually built and targets defined")
else()
  message(STATUS "ICU target already exists")
endif()