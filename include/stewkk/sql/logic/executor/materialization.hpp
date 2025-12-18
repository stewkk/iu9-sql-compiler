#pragma once

#include <fstream>

#include <boost/filesystem.hpp>

#include <stewkk/sql/models/executor/tuple.hpp>

namespace stewkk::sql {

namespace fs = boost::filesystem;

class DiskFileReader {
    public:
        DiskFileReader(fs::path path, std::size_t tuple_size);
        ~DiskFileReader();
        Tuples Read();
    private:
        fs::path path_;
        std::ifstream f_;
        std::size_t tuple_size_;
};

class DiskFileWriter {
    public:
        DiskFileWriter();
        void Write(const Tuples& tuples);
        DiskFileReader GetDiskFileReader() &&;
    private:
        fs::path path_;
        std::ofstream f_;
        std::size_t tuple_size_;
};

}  // namespace stewkk::sql
