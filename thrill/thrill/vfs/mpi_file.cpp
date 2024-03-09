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
#include <iostream>
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
#include <mpi.h>

namespace thrill {
namespace vfs {
	static void MPIGlobWalkRecursive(const std::string& path, FileList& filelist){
             DIR* dir = opendir(path.c_str());
	     if(dir == nullptr)
		     throw common::ErrnoException("could not read directory " + path);

	     struct dirent* de;
	     struct stat st;

	     std::vector<std::string> list;

	     while ((de=common::ts_readdir(dir))!=nullptr){
		     if(de->d_name[0] == '.')continue;

		     list.emplace_back(path + "/" + de->d_name);
	     }
	     closedir(dir);
	     std::sort(list.begin(), list.end());

             for (const std::string& entry : list) {
		 if (stat(entry.c_str(), &st) != 0)
	             throw common::ErrnoException("Could not lstat() " + entry);

		 if (S_ISDIR(st.st_mode)) {
	             // descend into directories
	             MPIGlobWalkRecursive(entry, filelist);                     
		 }
		 else if (S_ISREG(st.st_mode)) {
		     FileInfo fi;
		     fi.type = Type::File;
		     fi.path = entry;
		     fi.size = static_cast<uint64_t>(st.st_size);
	             filelist.emplace_back(fi);
												             
		 }
				     
	      }
    
}

void MPIGlob(const std::string& path, const GlobType& gtype, FileList& filelist){
	std::vector<std::string> list;
	glob_t glob_result;
	glob(path.c_str(),GLOB_TILDE,nullptr,&glob_result);

	for(unsigned int i=0;i<glob_result.gl_pathc;++i){
		list.push_back(glob_result.gl_pathv[i]);
	}
	globfree(&glob_result);
	std::sort(list.begin(),list.end());
	struct stat filestat;
	for(const std::string& file:list){
		if(::stat(file.c_str(),&filestat)!=0){
			die("ERROR: could not stat() path "+ file);
		}

		if(S_ISREG(filestat.st_mode)){
			if(gtype == GlobType::All || gtype == GlobType::File){
				FileInfo fi;
				fi.type = Type::Directory;
				fi.path = file;
				fi.size = 0;
				filelist.emplace_back(fi);
			}
			else if(gtype == GlobType::File){
				MPIGlobWalkRecursive(file,filelist);
			}
		}
	}
}

class MPIFile final : public virtual ReadStream, public virtual WriteStream{

	static constexpr bool debug = false;
public:
	MPIFile(){
	}
	explicit MPIFile(MPI_File file_des, int pid=0) noexcept
		: file_des_(file_des), pid_(pid){}
	~MPIFile(){
              MPI_File_close(&file_des_);
	}

	ssize_t write(const void* data, size_t count) final{
	       MPI_Status status;
               MPI_File_write(file_des_,data,count,MPI_CHAR,&status);
	       MPI_File_seek(file_des_,status.count_lo,MPI_SEEK_CUR);
	       return status.count_lo;
	}

	ssize_t read(void* data, size_t count) final{
		MPI_Status status;
		MPI_File_read(file_des_,data,count,MPI_CHAR,&status);
		MPI_File_seek(file_des_,status.count_lo,MPI_SEEK_CUR);
		return status.count_lo;
	}

	void close() final;

private:
	MPI_File file_des_;
	using pid_t = int;
	pid_t pid_ = 0;

};

void MPIFile::close(){

}
ReadStreamPtr MPIOpenReadStream(const std::string& path,const common::Range& range){
	static constexpr bool debug = false;

	Initialize();
	std::cout << "MPIOpenReadStream" << std::endl;
	MPI_File file_des;
	MPI_File_open(MPI_COMM_SELF,path.data(),MPI_MODE_RDONLY,MPI_INFO_NULL,&file_des);
	std::cout << "MPIOpenReadStream done." << std::endl;

	MPI_Offset offset = range.begin;
        MPI_File_seek(file_des,offset,MPI_SEEK_SET);
	return tlx::make_counting<MPIFile>(file_des);
	
}

WriteStreamPtr MPIOpenWriteStream(const std::string& path){
	static constexpr bool debug = false;
	
        Initialize();
	int rank, size;
	MPI_Comm_rank(MPI_COMM_WORLD,&rank);
	MPI_Comm_size(MPI_COMM_WORLD,&size);
	std::cout << "MPIOpenWriteStream, path:" << path << " rank:" << rank << " size:" << size << std::endl;
	MPI_File file_des;
	int error = MPI_File_open(MPI_COMM_SELF,path.data(),MPI_MODE_CREATE|MPI_MODE_WRONLY,MPI_INFO_NULL,&file_des);
	std::cout << "error:" << error << std::endl;
	std::cout << "MPIOpenWriteStream done." << std::endl;
	return tlx::make_counting<MPIFile>(file_des);
}

}
}
