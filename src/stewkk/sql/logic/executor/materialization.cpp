#include <stewkk/sql/logic/executor/materialization.hpp>

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include <stewkk/sql/logic/executor/buffer_size.hpp>

// TODO: implement async file io with asio (and benchmark it)

namespace stewkk::sql {

namespace {

constexpr std::size_t kDiskValueSize = sizeof(uint8_t) + sizeof(int64_t);

void AppendValue(std::vector<char>& buf, const Value& value) {
    const int64_t payload = value.is_null ? 0 : value.value.int_value;
    buf.push_back(static_cast<char>(value.is_null));
    const auto* payload_bytes = reinterpret_cast<const char*>(&payload);
    buf.insert(buf.end(), payload_bytes, payload_bytes + sizeof(payload));
}

Value ReadValue(const char* buf) {
    int64_t payload;
    std::memcpy(&payload, buf + sizeof(uint8_t), sizeof(payload));
    return Value{static_cast<bool>(buf[0]), {.int_value = payload}};
}

}  // namespace

DiskFileWriter::DiskFileWriter()
    : path_(fs::temp_directory_path() / fs::unique_path("%%%%.tmp")), f_(path_, std::ios::binary) {}

void DiskFileWriter::Write(const Tuples& tuples) {
    if (tuples.empty()) {
      return;
    }
    if (tuple_size_ == 0) {
      tuple_size_ = tuples.front().size();
    }
    std::vector<char> buf;
    buf.reserve(tuples.size() * tuple_size_ * kDiskValueSize);
    for (const auto& tuple : tuples) {
      if (tuple.size() != tuple_size_) {
        throw std::runtime_error{"cannot materialize tuples with different sizes"};
      }
      for (const auto& value : tuple) {
        AppendValue(buf, value);
      }
    }
    f_.write(buf.data(), static_cast<std::streamsize>(buf.size()));
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

void DiskFileReader::Rewind() {
    f_.clear();
    f_.seekg(0, std::ios::beg);
}

Tuples DiskFileReader::Read() {
    Tuples buf;
    buf.reserve(kBufSize);
    std::vector<char> tuple_buf(tuple_size_ * kDiskValueSize);
    while (buf.size() < kBufSize) {
      Tuple tuple;
      tuple.reserve(tuple_size_);
      f_.read(tuple_buf.data(), static_cast<std::streamsize>(tuple_buf.size()));
      if (f_.gcount() == 0) {
          break;
      }
      if (f_.gcount() != static_cast<std::streamsize>(tuple_buf.size())) {
        throw std::runtime_error{"truncated materialized tuple"};
      }
      for (std::size_t offset = 0; offset < tuple_buf.size(); offset += kDiskValueSize) {
        tuple.push_back(ReadValue(tuple_buf.data() + offset));
      }
      buf.push_back(std::move(tuple));
    }
    return buf;
}

}  // namespace stewkk::sql
