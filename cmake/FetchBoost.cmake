include(FetchContent)

FetchContent_Declare(
  Boost
  URL https://github.com/boostorg/boost/releases/download/boost-1.90.0/boost-1.90.0-cmake.tar.gz
  URL_HASH SHA256=913ca43d49e93d1b158c9862009add1518a4c665e7853b349a6492d158b036d4
  DOWNLOAD_EXTRACT_TIMESTAMP ON
  EXCLUDE_FROM_ALL
  OVERRIDE_FIND_PACKAGE
)

set(BOOST_INCLUDE_LIBRARIES asio thread filesystem)

FetchContent_MakeAvailable(Boost)
