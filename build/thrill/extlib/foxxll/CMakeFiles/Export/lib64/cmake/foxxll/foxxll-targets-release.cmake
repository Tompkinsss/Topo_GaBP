#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "foxxll" for configuration "Release"
set_property(TARGET foxxll APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(foxxll PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
  IMPORTED_LINK_INTERFACE_LIBRARIES_RELEASE "tlx;-lpthread"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib64/libfoxxll.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS foxxll )
list(APPEND _IMPORT_CHECK_FILES_FOR_foxxll "${_IMPORT_PREFIX}/lib64/libfoxxll.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
