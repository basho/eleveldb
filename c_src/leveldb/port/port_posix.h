// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#ifndef STORAGE_LEVELDB_PORT_PORT_POSIX_H_
#define STORAGE_LEVELDB_PORT_PORT_POSIX_H_

#if defined(OS_MACOSX) || defined(OS_FREEBSD)
  #include <machine/endian.h>
#elif defined(OS_SOLARIS)
  #include <sys/isa_defs.h>
  #ifdef _LITTLE_ENDIAN
    #define LITTLE_ENDIAN
  #else
    #define BIG_ENDIAN
  #endif
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) ||\
      defined(OS_DRAGONFLYBSD)
  #include <sys/types.h>
  #include <sys/endian.h>
#else
  #include <endian.h>
#endif
#include <pthread.h>
#ifdef SNAPPY
#include <snappy.h>
#endif
#include <stdint.h>
#include <string>
#include "port/atomic_pointer.h"

#ifdef LITTLE_ENDIAN
#define IS_LITTLE_ENDIAN true
#else
#define IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#if defined(OS_MACOSX) || defined(OS_SOLARIS) || defined(OS_FREEBSD) ||\
    defined(OS_NETBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD)
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) ||\
    defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD)
#define fdatasync fsync
#endif

// Some compilers do not provide access to nested classes of a declared friend class
// Defining PUBLIC_NESTED_FRIEND_ACCESS will cause those declarations to be made
// public as a workaround
#if defined(OS_MACOSX) || defined(OS_SOLARIS)
#define USED_BY_NESTED_FRIEND(a) public: a; private:
#define USED_BY_NESTED_FRIEND2(a,b) public: a,b; private:
#else
#define USED_BY_NESTED_FRIEND(a) a;
#define USED_BY_NESTED_FRIEND2(a,b) a,b;
#endif

namespace leveldb {
namespace port {

static const bool kLittleEndian = IS_LITTLE_ENDIAN;

class CondVar;

class Mutex {
 public:
  Mutex();
  ~Mutex();

  void Lock();
  void Unlock();
  void AssertHeld() { }

 private:
  friend class CondVar;
  pthread_mutex_t mu_;

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  void Signal();
  void SignalAll();
 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};

inline bool Snappy_Compress(const char* input, size_t length,
                            ::std::string* output) {
#ifdef SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif
}

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {
#ifdef SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif
}

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  return false;
}

} // namespace port
} // namespace leveldb

#endif  // STORAGE_LEVELDB_PORT_PORT_POSIX_H_
