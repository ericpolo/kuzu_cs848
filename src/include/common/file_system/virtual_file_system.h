#pragma once

#include <memory>
#include <vector>

#include "file_system.h"

namespace kuzu {
namespace common {

class VirtualFileSystem final : public FileSystem {

public:
    VirtualFileSystem();

    void registerFileSystem(std::unique_ptr<FileSystem> fileSystem);

    std::unique_ptr<FileInfo> openFile(const std::string& path, int flags,
        main::ClientContext* context = nullptr,
        FileLockType lockType = FileLockType::NO_LOCK) const override;

    std::vector<std::string> glob(const std::string& path) const override;

    void overwriteFile(const std::string& from, const std::string& to) const override;

    void createDir(const std::string& dir) const override;

    void removeFileIfExists(const std::string& path) const override;

    bool fileOrPathExists(const std::string& path) const override;

protected:
    void readFromFile(
        FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) const override;

    int64_t readFile(FileInfo* fileInfo, void* buf, size_t nbyte) const override;

    void writeFile(FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes,
        uint64_t offset) const override;

    int64_t seek(FileInfo* fileInfo, uint64_t offset, int whence) const override;

    void truncate(FileInfo* fileInfo, uint64_t size) const override;

    uint64_t getFileSize(FileInfo* fileInfo) const override;

private:
    FileSystem* findFileSystem(const std::string& path) const;

private:
    std::vector<std::unique_ptr<FileSystem>> subSystems;
    std::unique_ptr<FileSystem> defaultFS;
};

} // namespace common
} // namespace kuzu
