/***************************************************************************
 *  foxxll/config.hpp.in
 *
 *  Template file processed by cmake to set all define switches for this build
 *  according to the cmake build options.
 *
 *  Part of FOXXLL. See http://foxxll.org
 *
 *  Copyright (C) 2012-2013 Timo Bingmann <tb@panthema.net>
 *  Copyright (C) 2018      Manuel Penschuck <foxxll@manuel.jetzt>
 *
 *  Distributed under the Boost Software License, Version 1.0.
 *  (See accompanying file LICENSE_1_0.txt or copy at
 *  http://www.boost.org/LICENSE_1_0.txt)
 **************************************************************************/

#ifndef FOXXLL_CONFIG_HEADER
#define FOXXLL_CONFIG_HEADER

// the FOXXLL library version variables
#define FOXXLL_VERSION_MAJOR 1
#define FOXXLL_VERSION_MINOR 4
#define FOXXLL_VERSION_PATCH 99
#define FOXXLL_VERSION_STRING "1.4.99"
#define FOXXLL_VERSION_PHASE "prerelease/Release"

// if this is a git repository, add the refspec and commit sha
#define FOXXLL_VERSION_GIT_REFSPEC "1.4.1-454-ga4a8aee"
#define FOXXLL_VERSION_GIT_SHA1 "a4a8aeee64743f845c5851e8b089965ea1c219d7"

/* #undef FOXXLL_DIRECT_IO_OFF */
// default: 0/1 (platform dependent)
// cmake:   detection of platform and flag
// effect:  disables use of O_DIRECT flag on unsupported platforms

#define FOXXLL_HAVE_MMAP_FILE 1
// default: 0/1 (platform dependent)
// used in: io/mmap_file.h/cpp
// effect:  enables/disables memory mapped file implementation

#define FOXXLL_HAVE_LINUXAIO_FILE 1
// default: 0/1 (platform dependent)
// used in: io/linuxaio_file.h/cpp
// effect:  enables/disables Linux AIO file implementation

/* #undef FOXXLL_WINDOWS */
// default: off
// cmake:   detection of ms windows platform
// effect:  enables windows-specific api calls (mingw or msvc)

/* #undef FOXXLL_MSVC */
// default: off
// cmake:   detection of ms visual c++ via CMake (contains version number)
// effect:  enables msvc-specific headers and macros

/* #undef FOXXLL_WITH_VALGRIND */
// default: off
// cmake:   option USE_VALGRIND=ON
// effect:  run all tests with valgrind and pre-initialize some memory buffers

#endif // !FOXXLL_CONFIG_HEADER
