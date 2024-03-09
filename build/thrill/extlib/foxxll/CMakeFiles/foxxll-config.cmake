# CMake config file for FOXXLL
#
# It defines the following variables
#  FOXXLL_CXX_FLAGS    - C++ flags for FOXXLL
#  FOXXLL_INCLUDE_DIRS - include directories for FOXXLL
#  FOXXLL_LIBRARIES    - libraries to link against

# compute paths from current cmake file path
get_filename_component(FOXXLL_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
set(FOXXLL_INCLUDE_DIRS "${FOXXLL_CMAKE_DIR}/../../../")

# additional compiler flags. these often include -fopenmp!
set(FOXXLL_CXX_FLAGS "")

# load our library dependencies (contains definitions for IMPORTED targets)
include("${FOXXLL_CMAKE_DIR}/foxxll-targets.cmake")

# these are IMPORTED targets created by foxxll-targets.cmake, link these with
# your program.
set(FOXXLL_LIBRARIES "foxxll;tlx;-lpthread")
