# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 2.8

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list

# Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The program to use to edit the cache.
CMAKE_EDIT_COMMAND = /usr/bin/ccmake

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/nscc-gz/lzy/GaBP

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/nscc-gz/lzy/GaBP/build

# Utility rule file for NightlySubmit.

# Include the progress variables for this target.
include thrill/CMakeFiles/NightlySubmit.dir/progress.make

thrill/CMakeFiles/NightlySubmit:
	cd /home/nscc-gz/lzy/GaBP/build/thrill && /usr/bin/ctest -D NightlySubmit

NightlySubmit: thrill/CMakeFiles/NightlySubmit
NightlySubmit: thrill/CMakeFiles/NightlySubmit.dir/build.make
.PHONY : NightlySubmit

# Rule to build all files generated by this target.
thrill/CMakeFiles/NightlySubmit.dir/build: NightlySubmit
.PHONY : thrill/CMakeFiles/NightlySubmit.dir/build

thrill/CMakeFiles/NightlySubmit.dir/clean:
	cd /home/nscc-gz/lzy/GaBP/build/thrill && $(CMAKE_COMMAND) -P CMakeFiles/NightlySubmit.dir/cmake_clean.cmake
.PHONY : thrill/CMakeFiles/NightlySubmit.dir/clean

thrill/CMakeFiles/NightlySubmit.dir/depend:
	cd /home/nscc-gz/lzy/GaBP/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/nscc-gz/lzy/GaBP /home/nscc-gz/lzy/GaBP/thrill /home/nscc-gz/lzy/GaBP/build /home/nscc-gz/lzy/GaBP/build/thrill /home/nscc-gz/lzy/GaBP/build/thrill/CMakeFiles/NightlySubmit.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : thrill/CMakeFiles/NightlySubmit.dir/depend

