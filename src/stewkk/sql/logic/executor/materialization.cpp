#include <stewkk/sql/logic/executor/materialization.hpp>

#include <stewkk/sql/logic/executor/buffer_size.hpp>

// TODO: implement async file io with asio (and benchmark it)

namespace stewkk::sql {

DiskFileWriter::DiskFileWriter()
    : path_(fs::temp_directory_path() / fs::unique_path("%%%%.tmp")), f_(path_, std::ios::binary) {}

void DiskFileWriter::Write(const Tuples& tuples) {
    tuple_size_ = tuples.front().size();
    for (const auto& tuple : tuples) {
      std::size_t tuple_size = tuple.size();
      // NOTE: better to store tuples in continious buffer and than flush to
      // disk
      f_.write(reinterpret_cast<const char*>(tuple.data()),
               tuple.size() * sizeof(Tuple::value_type));
    }
}

DiskFileReader DiskFileWriter::GetDiskFileReader() && {
    f_.flush();
    f_.close();
    return DiskFileReader(std::move(path_), tuple_size_);
}

DiskFileReader::DiskFileReader(fs::path path, std::size_t tuple_size)
    : path_(std::move(path)), f_(path_, std::ios::binary), tuple_size_(tuple_size) {}

DiskFileReader::~DiskFileReader() {
    f_.close();
    fs::remove(path_);
}

Tuples DiskFileReader::Read() {
    Tuples buf;
    buf.reserve(kBufSize);
    while (buf.size() < kBufSize) {
      Tuple tuple;
      tuple.resize(tuple_size_);
      f_.read(reinterpret_cast<char*>(tuple.data()), tuple_size_*sizeof(Tuple::value_type));
      if (f_.gcount() == 0) {
          break;
      }
      buf.push_back(std::move(tuple));
    }
    return buf;
}

}  // namespace stewkk::sql
