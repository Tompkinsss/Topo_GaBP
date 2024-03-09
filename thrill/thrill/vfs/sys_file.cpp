/*******************************************************************************
 * thrill/vfs/sys_file.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/sys_file.hpp>

#include <thrill/common/porting.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/vfs/simple_glob.hpp>

#include <tlx/die.hpp>
#include <tlx/string/ends_with.hpp>

#include <fcntl.h>
#include <sys/stat.h>

#if !defined(_MSC_VER)

#include <dirent.h>
#include <glob.h>
#include <sys/wait.h>
#include <unistd.h>

#if !defined(O_BINARY)
#define O_BINARY 0
#endif

#else

#include <io.h>
#include <windows.h>

#define S_ISREG(m)       (((m) & _S_IFMT) == _S_IFREG)

#endif

#include <algorithm>
#include <string>
#include <vector>
#include <iostream>
namespace thrill {
namespace vfs {

/******************************************************************************/

static void SysGlobWalkRecursive(const std::string& path, FileList& filelist) {
#if defined(_MSC_VER)

    WIN32_FIND_DATA ff;
    HANDLE h = FindFirstFile((path + "\\*").c_str(), &ff);

    if (h == INVALID_HANDLE_VALUE) {
        throw common::ErrnoException(
                  "FindFirstFile failed:" + std::to_string(GetLastError()));
    }

    std::vector<FileInfo> tmp_list;

    do {
        if (ff.cFileName[0] != '.')
        {
            FileInfo fi;
            if (ff.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
                fi.type = Type::Directory;
            }
            else {
                fi.type = Type::File;
            }
            fi.path = path + "\\" + ff.cFileName;
            fi.size = (static_cast<uint64_t>(ff.nFileSizeHigh) * (MAXDWORD + 1))
                      + static_cast<uint64_t>(ff.nFileSizeLow);
            tmp_list.emplace_back(fi);
        }
    } while (FindNextFile(h, &ff) != 0);

    DWORD e = GetLastError();
    if (e != ERROR_NO_MORE_FILES) {
        throw common::ErrnoException(
                  "FindFirstFile failed:" + std::to_string(GetLastError()));
    }

    std::sort(tmp_list.begin(), tmp_list.end());

    for (const FileInfo& fi : tmp_list) {
        if (fi.type == Type::Directory) {
            SysGlobWalkRecursive(fi.path, filelist);
        }
        else {
            filelist.emplace_back(fi);
        }
    }

#else
    // read entries
    DIR* dir = opendir(path.c_str());
    if (dir == nullptr)
        throw common::ErrnoException("Could not read directory " + path);

    struct dirent* de;
    struct stat st;

    std::vector<std::string> list;

    while ((de = common::ts_readdir(dir)) != nullptr) {
        // skip ".", "..", and also hidden files (don't create them).
        if (de->d_name[0] == '.') continue;

        list.emplace_back(path + "/" + de->d_name);
    }

    closedir(dir);

    // sort file names
    std::sort(list.begin(), list.end());

    for (const std::string& entry : list) {
        if (stat(entry.c_str(), &st) != 0)
            throw common::ErrnoException("Could not lstat() " + entry);

        if (S_ISDIR(st.st_mode)) {
            // descend into directories
            SysGlobWalkRecursive(entry, filelist);
        }
        else if (S_ISREG(st.st_mode)) {
            FileInfo fi;
            fi.type = Type::File;
            fi.path = entry;
            fi.size = static_cast<uint64_t>(st.st_size);
            filelist.emplace_back(fi);
        }
    }
#endif
}

void SysGlob(const std::string& path, const GlobType& gtype,
             FileList& filelist) {

    std::vector<std::string> list;

    // collect file names
#if defined(_MSC_VER)
    glob_local::CSimpleGlob sglob;
    sglob.Add(path.c_str());
    for (int n = 0; n < sglob.FileCount(); ++n) {
        list.emplace_back(sglob.File(n));
    }
#else
    glob_t glob_result;
    glob(path.c_str(), GLOB_TILDE, nullptr, &glob_result);

    for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
        list.push_back(glob_result.gl_pathv[i]);
    }
    globfree(&glob_result);
#endif

    // sort file names
    std::sort(list.begin(), list.end());

    // stat files to collect size information
    struct stat filestat;
    for (const std::string& file : list)
    {
        if (::stat(file.c_str(), &filestat) != 0) {
            die("ERROR: could not stat() path " + file);
        }

        if (S_ISREG(filestat.st_mode)) {
            if (gtype == GlobType::All || gtype == GlobType::File) {
                FileInfo fi;
                fi.type = Type::File;
                fi.path = file;
                fi.size = static_cast<uint64_t>(filestat.st_size);
                filelist.emplace_back(fi);
            }
        }
        else {
            // directory entries or others
            if (gtype == GlobType::All || gtype == GlobType::Directory) {
                FileInfo fi;
                fi.type = Type::Directory;
                fi.path = file;
                fi.size = 0;
                filelist.emplace_back(fi);
            }
            else if (gtype == GlobType::File) {
                SysGlobWalkRecursive(file, filelist);
            }
        }
    }
}

/******************************************************************************/

/*!
 * Represents a POSIX system file via its file descriptor.
 */
class SysFile final : public virtual ReadStream, public virtual WriteStream
{
    static constexpr bool debug = false;

public:
    //! default constructor
    SysFile() : fd_(-1) { }

    //! constructor: use OpenForRead or OpenForWrite.
    explicit SysFile(int fd, int pid = 0) noexcept
        : fd_(fd), pid_(pid) { }

    //! non-copyable: delete copy-constructor
    SysFile(const SysFile&) = delete;
    //! non-copyable: delete assignment operator
    SysFile& operator = (const SysFile&) = delete;
    //! move-constructor
    SysFile(SysFile&& f) noexcept
        : fd_(f.fd_), pid_(f.pid_) {
        f.fd_ = -1, f.pid_ = 0;
    }
    //! move-assignment
    SysFile& operator = (SysFile&& f) {
        close();
        fd_ = f.fd_, pid_ = f.pid_;
        f.fd_ = -1, f.pid_ = 0;
        return *this;
    }

    ~SysFile() {
        close();
    }

    //! POSIX write function.
    ssize_t write(const void* data, size_t count) final {
        assert(fd_ >= 0);
#if defined(_MSC_VER)
        return ::_write(fd_, data, static_cast<unsigned>(count));
#else
        return ::write(fd_, data, count);
#endif
    }

    //! POSIX read function.
    ssize_t read(void* data, size_t count) final {
        assert(fd_ >= 0);
#if defined(_MSC_VER)
        return ::_read(fd_, data, static_cast<unsigned>(count));
#else
        ssize_t size =  ::read(fd_, data, count);
	return size;
#endif
    }

    //! close the file descriptor
    void close() final;

private:
    //! file descriptor
    int fd_ = -1;

#if defined(_MSC_VER)
    using pid_t = int;
#endif

    //! pid of child process to wait for
    pid_t pid_ = 0;
};

void SysFile::close() {
    if (fd_ >= 0) {
        sLOG << "SysFile::close(): fd" << fd_;
        if (::close(fd_) != 0)
        {
            LOG1 << "SysFile::close()"
                 << " fd_=" << fd_
                 << " errno=" << errno
                 << " error=" << strerror(errno);
        }
        fd_ = -1;
    }
#if !defined(_MSC_VER)
    if (pid_ != 0) {
        sLOG << "SysFile::close(): waitpid for" << pid_;
        int status;
        pid_t p = waitpid(pid_, &status, 0);
        if (p != pid_) {
            throw common::SystemException(
                      "SysFile: waitpid() failed to return child");
        }
        if (WIFEXITED(status)) {
            // child program exited normally
            if (WEXITSTATUS(status) != 0) {
                throw common::ErrnoException(
                          "SysFile: child failed with return code "
                          + std::to_string(WEXITSTATUS(status)));
            }
            else {
                // zero return code. good.
            }
        }
        else if (WIFSIGNALED(status)) {
            throw common::ErrnoException(
                      "SysFile: child killed by signal "
                      + std::to_string(WTERMSIG(status)));
        }
        else {
            throw common::ErrnoException(
                      "SysFile: child failed with an unknown error");
        }
        pid_ = 0;
    }
#endif
}

/******************************************************************************/

ReadStreamPtr SysOpenReadStream(
    const std::string& path, const common::Range& range) {

    static constexpr bool debug = false;

    // first open the file and see if it exists at all.

    int fd = ::open(path.c_str(), O_RDONLY | O_BINARY, 0);
    if (fd < 0) {
        throw common::ErrnoException("Cannot open file " + path);
    }

    // then figure out whether we need to pipe it through a decompressor.

    const char* decompressor;

    if (tlx::ends_with(path, ".xz")) {
        decompressor = "xz";
    }
    else if (tlx::ends_with(path, ".lzo")) {
        decompressor = "lzop";
    }
    else if (tlx::ends_with(path, ".lz4")) {
        decompressor = "lz4";
    }
    else {
        // not a compressed file
        common::PortSetCloseOnExec(fd);

        sLOG << "SysFile::OpenForRead(): filefd" << fd;

        if (range.begin) {
            //! POSIX lseek function from current position.
            ::lseek(fd, range.begin, SEEK_CUR);
        }

        return tlx::make_counting<SysFile>(fd);
    }

#if defined(_MSC_VER)
    throw common::SystemException(
              "Reading compressed files is not supported on windows, yet. "
              "Please submit a patch.");
#else
    // if decompressor: fork a child program which calls the decompressor and
    // connect file descriptors via a pipe.

    // pipe[0] = read, pipe[1] = write
    int pipefd[2];
    common::MakePipe(pipefd);

    pid_t pid = fork();
    if (pid == 0) {
        // close read end
        ::close(pipefd[0]);

        // replace stdin with file descriptor to file opened above.
        dup2(fd, STDIN_FILENO);
        ::close(fd);
        // replace stdout with pipe going back to Thrill process
        dup2(pipefd[1], STDOUT_FILENO);
        ::close(pipefd[1]);

        execlp(decompressor, decompressor, "-d", nullptr);

        LOG1 << "Pipe execution failed: " << strerror(errno);
        // close write end
        ::close(pipefd[1]);
        exit(-1);
    }
    else if (pid < 0) {
        throw common::ErrnoException("Error creating child process");
    }

    sLOG << "SysFile::OpenForRead(): pipefd" << pipefd[0] << "to pid" << pid;

    // close pipe write end
    ::close(pipefd[1]);

    // close the file descriptor
    ::close(fd);

    if (range.begin) {
        //! POSIX lseek function from current position.
        ::lseek(pipefd[0], range.begin, SEEK_CUR);
    }

    return tlx::make_counting<SysFile>(pipefd[0], pid);
#endif
}

WriteStreamPtr SysOpenWriteStream(const std::string& path) {

    static constexpr bool debug = false;

    // first create the file and see if we can write it at all.

    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_BINARY, 0666);
    if (fd < 0) {
        throw common::ErrnoException("Cannot create file " + path);
    }

    // then figure out whether we need to pipe it through a compressor.

    const char* compressor;

    if (tlx::ends_with(path, ".xz")) {
        compressor = "xz";
    }
    else if (tlx::ends_with(path, ".lzo")) {
        compressor = "lzop";
    }
    else if (tlx::ends_with(path, ".lz4")) {
        compressor = "lz4";
    }
    else {
        // not a compressed file
        common::PortSetCloseOnExec(fd);

        sLOG << "SysFile::OpenForWrite(): filefd" << fd;

        return tlx::make_counting<SysFile>(fd);
    }

#if defined(_MSC_VER)
    throw common::SystemException(
              "Reading compressed files is not supported on windows, yet. "
              "Please submit a patch.");
#else
    // if compressor: fork a child program which calls the compressor and
    // connect file descriptors via a pipe.

    // pipe[0] = read, pipe[1] = write
    int pipefd[2];
    common::MakePipe(pipefd);

    pid_t pid = fork();
    if (pid == 0) {
        // close write end
        ::close(pipefd[1]);

        // replace stdin with pipe
        dup2(pipefd[0], STDIN_FILENO);
        ::close(pipefd[0]);
        // replace stdout with file descriptor to file created above.
        dup2(fd, STDOUT_FILENO);
        ::close(fd);

        execlp(compressor, compressor, nullptr);

        LOG1 << "Pipe execution failed: " << strerror(errno);
        // close read end
        ::close(pipefd[0]);
        exit(-1);
    }
    else if (pid < 0) {
        throw common::ErrnoException("Error creating child process");
    }

    sLOG << "SysFile::OpenForWrite(): pipefd" << pipefd[0] << "to pid" << pid;

    // close read end
    ::close(pipefd[0]);

    // close file descriptor (it is used by the fork)
    ::close(fd);

    return tlx::make_counting<SysFile>(pipefd[1], pid);
#endif
}

} // namespace vfs
} // namespace thrill

/******************************************************************************/
