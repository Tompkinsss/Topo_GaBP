#pragma once
#ifndef THRILL_VFS_MPI_FILE_HEADER
#define THRILL_VFS_MPI_FILE_HEADER

#include <thrill/vfs/file_io.hpp>
#include <string>

namespace thrill{
namespace vfs{

void MPIGlob(const std::string& path, const GlobType& gtype, FileList& filelist);

ReadStreamPtr MPIOpenReadStream(const std::string& path, const common::Range& range = common::Range());

WriteStreamPtr MPIOpenWriteStream(const std::string& path);

}
}

#endif
