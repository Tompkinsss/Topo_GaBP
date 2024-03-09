/*******************************************************************************
 * thrill/vfs/file_io.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/file_io.hpp>

#include <thrill/common/string.hpp>
#include <thrill/vfs/bzip2_filter.hpp>
#include <thrill/vfs/gzip_filter.hpp>
#include <thrill/vfs/hdfs3_file.hpp>
#include <thrill/vfs/s3_file.hpp>
#include <thrill/vfs/sys_file.hpp>
#include <thrill/vfs/mpi_file.hpp>
#include <tlx/die.hpp>
#include <tlx/string/ends_with.hpp>
#include <tlx/string/ssprintf.hpp>
#include <tlx/string/starts_with.hpp>

#include <algorithm>
#include <string>
#include <vector>
#include <iostream>
namespace thrill {
namespace vfs {

/******************************************************************************/

void Initialize() {
    S3Initialize();
    Hdfs3Initialize();
}

void Deinitialize() {
    S3Deinitialize();
    Hdfs3Deinitialize();
}

/******************************************************************************/

bool IsCompressed(const std::string& path) {
    return tlx::ends_with(path, ".gz") ||
           tlx::ends_with(path, ".bz2") ||
           tlx::ends_with(path, ".xz") ||
           tlx::ends_with(path, ".lzo") ||
           tlx::ends_with(path, ".lz4");
}

bool IsRemoteUri(const std::string& path) {
    return tlx::starts_with(path, "s3://") ||
           tlx::starts_with(path, "hdfs://");
}

std::ostream& operator << (std::ostream& os, const Type& t) {
    switch (t) {
    case Type::File:
        return os << "File";
    case Type::Directory:
        return os << "Directory";
    default:
        return os << "Invalid";
    }
}

std::string FillFilePattern(const std::string& pathbase,
                            size_t worker, size_t file_part) {

    static constexpr bool debug = false;

    using size_type = std::string::size_type;

    std::string out_path = pathbase;

    // detect and save extension
    std::string extension;
    {
        size_type slash_end = out_path.rfind('/');
        size_type dot_end = out_path.rfind('.');
        if (dot_end != std::string::npos &&
            // dot is after slash
            (slash_end == std::string::npos || slash_end < dot_end)) {
            extension = out_path.substr(dot_end);
            out_path.erase(dot_end);
        }
    }
    {
        // replace @
        size_type at_end = out_path.rfind('@');
        size_type at_begin = out_path.find_last_not_of('@', at_end);

        size_type at_length =
            at_end != std::string::npos && at_end > at_begin
            ? at_end - at_begin : 4;

        sLOG << "at_length" << at_length;
        out_path.replace(at_begin + 1, at_length,
                         tlx::ssnprintf(at_length + 2, "%0*zu",
                                        static_cast<int>(at_length),
                                        worker));
    }
    {
        // replace hash signs
        size_type hash_end = out_path.rfind('#');
        size_type hash_begin = out_path.find_last_not_of('#', hash_end);

        size_type hash_length =
            hash_end != std::string::npos && hash_end > hash_begin
            ? hash_end - hash_begin : 10;

        sLOG << "hash_length" << hash_length;
        out_path.replace(hash_begin + 1, hash_length,
                         tlx::ssnprintf(hash_length + 2, "%0*zu",
                                        static_cast<int>(hash_length),
                                        file_part));
    }
    out_path += extension;
    return out_path;
}

/******************************************************************************/

FileList Glob(const std::vector<std::string>& globlist, const GlobType& gtype) {
    FileList filelist;

    // run through globs and collect files. The sub-Glob() methods must only
    // fill in the fields "path" and "size" of FileInfo, overall stats are
    // calculated afterwards.
    for (const std::string& path : globlist)
    {
        if (tlx::starts_with(path, "file://")) {
            // remove the file:// prefix
		std::cout << "Glob path: " << path << std::endl;
            SysGlob(path.substr(7), gtype, filelist);
        }
        else if (tlx::starts_with(path, "s3://")) {
            S3Glob(path, gtype, filelist);
        }
        else if (tlx::starts_with(path, "hdfs://")) {
            Hdfs3Glob(path, gtype, filelist);
        }
	else if(tlx::starts_with(path,"mpi://")){
		MPIGlob(path.substr(6),gtype,filelist);
	}
        else {
            SysGlob(path, gtype, filelist);
        }
    }

    // calculate exclusive prefix sum and overall stats

    filelist.contains_compressed = false;
    filelist.contains_remote_uri = false;
    filelist.total_size = 0;
    uint64_t size_ex_psum = 0;

    for (FileInfo& fi : filelist)
    {
        uint64_t size_next = size_ex_psum + fi.size;
        fi.size_ex_psum = size_ex_psum;
        size_ex_psum = size_next;

        filelist.contains_compressed |= fi.IsCompressed();
        filelist.contains_remote_uri |= fi.IsRemoteUri();
        filelist.total_size += fi.size;
    }

    return filelist;
}

FileList Glob(const std::string& glob, const GlobType& gtype) {
    return Glob(std::vector<std::string>{ glob }, gtype);
}

/******************************************************************************/

ReadStream::~ReadStream() { }

ReadStreamPtr OpenReadStream(
    const std::string& path, const common::Range& range) {

    ReadStreamPtr p;
    if (tlx::starts_with(path, "file://")) {
        p = SysOpenReadStream(path.substr(7), range);
    }else if(tlx::starts_with(path,"mpi://")){
	    p = MPIOpenReadStream(path.substr(6),range);
    }
    else if (tlx::starts_with(path, "s3://")) {
        p = S3OpenReadStream(path, range);
    }
    else if (tlx::starts_with(path, "hdfs://")) {
        p = Hdfs3OpenReadStream(path, range);
    }
    else {
        if(strcmp(getenv("THRILL_IO"),"mpi")==0){
            p = MPIOpenReadStream(path,range);
	}else{
            p = SysOpenReadStream(path, range);
	}
    }

    if (tlx::ends_with(path, ".gz")) {
        p = MakeGZipReadFilter(p);
        die_unless(range.begin == 0 || "Cannot seek in compressed streams.");
    }
    else if (tlx::ends_with(path, ".bz2")) {
        p = MakeBZip2ReadFilter(p);
        die_unless(range.begin == 0 || "Cannot seek in compressed streams.");
    }

    return p;
}

WriteStream::~WriteStream() { }

WriteStreamPtr OpenWriteStream(const std::string& path) {

    WriteStreamPtr p;
    if (tlx::starts_with(path, "file://")) {
        p = SysOpenWriteStream(path.substr(7));

    }else if(tlx::starts_with(path,"mpi://")){
	    p = MPIOpenWriteStream(path.substr(6));
    }
    else if (tlx::starts_with(path, "s3://")) {
        p = S3OpenWriteStream(path);
    }
    else if (tlx::starts_with(path, "hdfs://")) {
        p = Hdfs3OpenWriteStream(path);
    }
    else {
	if(strcmp(getenv("THRILL_IO"),"mpi")==0){
            p = MPIOpenWriteStream(path);
	}else{
            p = SysOpenWriteStream(path);
       	
        }
    }

    if (tlx::ends_with(path, ".gz")) {
        p = MakeGZipWriteFilter(p);
    }
    else if (tlx::ends_with(path, ".bz2")) {
        p = MakeBZip2WriteFilter(p);
    }

    return p;
}

} // namespace vfs
} // namespace thrill

/******************************************************************************/
