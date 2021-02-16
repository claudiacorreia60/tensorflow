/* Copyright 2015 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/mman.h>
#if defined(__linux__)
#include <sys/sendfile.h>
#endif
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <thread>
#include <chrono>

#include "tensorflow/core/platform/prisma/prisma_file_system.h"
#include "tensorflow/core/lib/io/path.h"

#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/error.h"
#include "tensorflow/core/platform/file_system_helper.h"
#include "tensorflow/core/platform/logging.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/platform/strcat.h"
#include "tensorflow/core/protobuf/error_codes.pb.h"

#include "third_party/prisma/prisma.h"


namespace tensorflow {

// 128KB of copy buffer
    constexpr size_t kPrismaCopyFileBufferSize = 128 * 1024;


// pread() based random-access
    class PrismaRandomAccessFile : public RandomAccessFile {
    private:
        string filename_;
        int fd_;

    public:
        PrismaRandomAccessFile(const string &fname, int fd)
                : filename_(fname), fd_(fd) {
            //LOG(INFO) << "Create Read Random Access File: name " << filename_;
        }

        ~PrismaRandomAccessFile() override { close(fd_); }

        Status Name(StringPiece *result) const override {
            *result = filename_;
            return Status::OK();
        }

        Status Read(uint64 offset, size_t n, StringPiece *result,
                    char *scratch) const override {
            Status s;
            char *dst = scratch;
	    Prisma prisma = Prisma();

	    //std::thread::id this_id = std::this_thread::get_id();
            //LOG(INFO) << "Read Random Access File: name " << filename_ <<
            //          " offset " << offset << " size " << n << " (THREAD " << this_id << ")";

            while (n > 0 && s.ok()) {
                // Some platforms, notably macs, throw EINVAL if pread is asked to read
                // more than fits in a 32-bit integer.
                size_t requested_read_length;
                if (n > INT32_MAX) {
                    requested_read_length = INT32_MAX;
                } else {
                    requested_read_length = n;
                }
		//ssize_t r =
		//	pread(fd_, dst, requested_read_length, static_cast<off_t>(offset));
                ssize_t r =
                       prisma.read(filename_, dst, requested_read_length, static_cast<off_t>(offset));
                if (r > 0) {
                    dst += r;
                    n -= r;
                    offset += r;
                } else if (r == 0) {
                    s = Status(error::OUT_OF_RANGE, "Read less bytes than requested");
                } else if (errno == EINTR || errno == EAGAIN) {
                    // Retry
                } else {
                    s = IOError(filename_, errno);
                }
            }
            *result = StringPiece(scratch, dst - scratch);
            return s;
        }
    };

    class PrismaWritableFile : public WritableFile {
    private:
        string filename_;
        FILE *file_;

    public:
        PrismaWritableFile(const string &fname, FILE *f)
                : filename_(fname), file_(f) { 
			//LOG(INFO) << "Create Writable File: name " << filename_; 
		}

        ~PrismaWritableFile() override {
            if (file_ != nullptr) {
                // Ignoring any potential errors
                fclose(file_);
            }
        }

        Status Append(StringPiece data) override {

            //LOG(INFO) << "Write Access File - Append: name " << filename_ << " size " << data.size();

            size_t r = fwrite(data.data(), 1, data.size(), file_);
            if (r != data.size()) {
                return IOError(filename_, errno);
            }
            return Status::OK();
        }

        Status Close() override {
            if (file_ == nullptr) {
                return IOError(filename_, EBADF);
            }
            Status result;
            if (fclose(file_) != 0) {
                result = IOError(filename_, errno);
            }
            file_ = nullptr;
            return result;
        }

        Status Flush() override {
            if (fflush(file_) != 0) {
                return IOError(filename_, errno);
            }
            return Status::OK();
        }

        Status Name(StringPiece *result) const override {
            *result = filename_;
            return Status::OK();
        }

        Status Sync() override {
            Status s;
            if (fflush(file_) != 0) {
                s = IOError(filename_, errno);
            }
            return s;
        }

        Status Tell(int64 *position) override {
            Status s;
            *position = ftell(file_);

            if (*position == -1) {
                s = IOError(filename_, errno);
            }

            return s;
        }
    };

    class PrismaReadOnlyMemoryRegion : public ReadOnlyMemoryRegion {
    public:
        PrismaReadOnlyMemoryRegion(const void *address, uint64 length)
                : address_(address), length_(length) { 
			//LOG(INFO) << "Read Only Memory Region";
		}

        ~PrismaReadOnlyMemoryRegion() override {
            munmap(const_cast<void *>(address_), length_);
        }

        const void *data() override { return address_; }

        uint64 length() override { return length_; }

    private:
        const void *const address_;
        const uint64 length_;
    };

    Status PrismaFileSystem::NewRandomAccessFile(
            const string &fname, std::unique_ptr <RandomAccessFile> *result) {
        string translated_fname = TranslateName(fname);
        Status s;
        int fd = open(translated_fname.c_str(), O_RDONLY);
        if (fd < 0) {
            s = IOError(fname, errno);
        } else {
            result->reset(new PrismaRandomAccessFile(translated_fname, fd));

            //LOG(INFO) << "Open Random Access File: name " << fname;

        }
        return s;
    }

    Status PrismaFileSystem::NewWritableFile(const string &fname,
                                           std::unique_ptr <WritableFile> *result) {
        string translated_fname = TranslateName(fname);
        Status s;
        FILE *f = fopen(translated_fname.c_str(), "w");
        if (f == nullptr) {
            s = IOError(fname, errno);
        } else {
            result->reset(new PrismaWritableFile(translated_fname, f));

            //LOG(INFO) << "Open Writable File: name " << fname;

        }
        return s;
    }

    Status PrismaFileSystem::NewAppendableFile(
            const string &fname, std::unique_ptr <WritableFile> *result) {
        string translated_fname = TranslateName(fname);
        Status s;
        FILE *f = fopen(translated_fname.c_str(), "a");
        if (f == nullptr) {
            s = IOError(fname, errno);
        } else {
            result->reset(new PrismaWritableFile(translated_fname, f));

            //LOG(INFO) << "Open Appendable File: name " << fname;
        }
        return s;
    }

    Status PrismaFileSystem::NewReadOnlyMemoryRegionFromFile(
            const string &fname, std::unique_ptr <ReadOnlyMemoryRegion> *result) {
        string translated_fname = TranslateName(fname);
        Status s = Status::OK();
        int fd = open(translated_fname.c_str(), O_RDONLY);
        if (fd < 0) {
            s = IOError(fname, errno);
        } else {
            struct stat st;
            ::fstat(fd, &st);
            const void *address =
                    mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (address == MAP_FAILED) {
                s = IOError(fname, errno);
            } else {
                //LOG(INFO) << "MMAP File: name " << fname;
                result->reset(new PrismaReadOnlyMemoryRegion(address, st.st_size));
            }
            close(fd);
        }
        return s;
    }

    Status PrismaFileSystem::FileExists(const string &fname) {
        if (access(TranslateName(fname).c_str(), F_OK) == 0) {
            return Status::OK();
        }
        return errors::NotFound(fname, " not found");
    }

    Status PrismaFileSystem::GetChildren(const string &dir,
                                       std::vector <string> *result) {
        string translated_dir = TranslateName(dir);
        result->clear();
        DIR *d = opendir(translated_dir.c_str());
        if (d == nullptr) {
            return IOError(dir, errno);
        }
        struct dirent *entry;
        while ((entry = readdir(d)) != nullptr) {
            StringPiece basename = entry->d_name;
            if ((basename != ".") && (basename != "..")) {
                result->push_back(entry->d_name);
            }
        }
        closedir(d);
        return Status::OK();
    }

    Status PrismaFileSystem::GetMatchingPaths(const string &pattern,
                                            std::vector <string> *results) {
        return internal::GetMatchingPaths(this, Env::Default(), pattern, results);
    }

    Status PrismaFileSystem::DeleteFile(const string &fname) {
        Status result;
        if (unlink(TranslateName(fname).c_str()) != 0) {
            result = IOError(fname, errno);
        }
        return result;
    }

    Status PrismaFileSystem::CreateDir(const string &name) {
        string translated = TranslateName(name);
        if (translated.empty()) {
            return errors::AlreadyExists(name);
        }
        if (mkdir(translated.c_str(), 0755) != 0) {
            return IOError(name, errno);
        }
        return Status::OK();
    }

    Status PrismaFileSystem::DeleteDir(const string &name) {
        Status result;
        if (rmdir(TranslateName(name).c_str()) != 0) {
            result = IOError(name, errno);
        }
        return result;
    }

    Status PrismaFileSystem::GetFileSize(const string &fname, uint64 *size) {
        Status s;
        struct stat sbuf;
        if (stat(TranslateName(fname).c_str(), &sbuf) != 0) {
            *size = 0;
            s = IOError(fname, errno);
        } else {
            *size = sbuf.st_size;
        }
        return s;
    }

    Status PrismaFileSystem::Stat(const string &fname, FileStatistics *stats) {
        Status s;
        struct stat sbuf;
        if (stat(TranslateName(fname).c_str(), &sbuf) != 0) {
            s = IOError(fname, errno);
        } else {
            stats->length = sbuf.st_size;
            stats->mtime_nsec = sbuf.st_mtime * 1e9;
            stats->is_directory = S_ISDIR(sbuf.st_mode);
        }
        return s;
    }

    Status PrismaFileSystem::RenameFile(const string &src, const string &target) {
        Status result;
        if (rename(TranslateName(src).c_str(), TranslateName(target).c_str()) != 0) {
            result = IOError(src, errno);
        }
        return result;
    }

    Status PrismaFileSystem::CopyFile(const string &src, const string &target) {
        string translated_src = TranslateName(src);
        struct stat sbuf;
        if (stat(translated_src.c_str(), &sbuf) != 0) {
            return IOError(src, errno);
        }
        int src_fd = open(translated_src.c_str(), O_RDONLY);
        if (src_fd < 0) {
            return IOError(src, errno);
        }
        string translated_target = TranslateName(target);
        // O_WRONLY | O_CREAT | O_TRUNC:
        //   Open file for write and if file does not exist, create the file.
        //   If file exists, truncate its size to 0.
        // When creating file, use the same permissions as original
        mode_t mode = sbuf.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO);
        int target_fd =
                open(translated_target.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
        if (target_fd < 0) {
            close(src_fd);
            return IOError(target, errno);
        }
        int rc = 0;
        off_t offset = 0;
        std::unique_ptr<char[]> buffer(new char[kPrismaCopyFileBufferSize]);
        while (offset < sbuf.st_size) {
            // Use uint64 for safe compare SSIZE_MAX
            uint64 chunk = sbuf.st_size - offset;
            if (chunk > SSIZE_MAX) {
                chunk = SSIZE_MAX;
            }
#if defined(__linux__) && !defined(__ANDROID__)
            rc = sendfile(target_fd, src_fd, &offset, static_cast<size_t>(chunk));
#else
            if (chunk > kPrismaCopyFileBufferSize) {
                chunk = kPrismaCopyFileBufferSize;
            }
            rc = read(src_fd, buffer.get(), static_cast<size_t>(chunk));
            if (rc <= 0) {
                break;
            }
            rc = write(target_fd, buffer.get(), static_cast<size_t>(chunk));
            offset += chunk;
#endif
            if (rc <= 0) {
                break;
            }
        }

        Status result = Status::OK();
        if (rc < 0) {
            result = IOError(target, errno);
        }

        // Keep the error code
        rc = close(target_fd);
        if (rc < 0 && result == Status::OK()) {
            result = IOError(target, errno);
        }
        rc = close(src_fd);
        if (rc < 0 && result == Status::OK()) {
            result = IOError(target, errno);
        }

        return result;
    }

    string PrismaFileSystem::TranslateName(const string &name) const {
        StringPiece scheme, namenode, path;
        io::ParseURI(name, &scheme, &namenode, &path);
        return string(path);
    }

    REGISTER_FILE_SYSTEM("prisma", PrismaFileSystem);

}  // namespace tensorflow
