// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#ifndef STORAGE_LEVELDB_PORT_PORT_STD_H_
#define STORAGE_LEVELDB_PORT_PORT_STD_H_

#include "build/build_config.h"
#if defined(OS_MACOSX)
#include <machine/endian.h>
#else
#include <endian.h>
#endif
#include <pthread.h>
#include <stdint.h>
#include <string>
#include "base/atomicops.h"

#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#if defined(OS_MACOSX)
#define fdatasync fsync
#endif

namespace leveldb {
namespace port {

static const bool kLittleEndian = (BYTE_ORDER == LITTLE_ENDIAN);

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

// Storage for a lock-free pointer
class AtomicPointer {
 private:
  typedef base::subtle::AtomicWord Rep;
  Rep rep_;
 public:
  AtomicPointer() { }
  explicit AtomicPointer(void* p) : rep_(reinterpret_cast<Rep>(p)) {}
  inline void* Acquire_Load() const {
    return reinterpret_cast<void*>(::base::subtle::Acquire_Load(&rep_));
  }
  inline void Release_Store(void* v) {
    ::base::subtle::Release_Store(&rep_, reinterpret_cast<Rep>(v));
  }
  inline void* NoBarrier_Load() const {
    return reinterpret_cast<void*>(::base::subtle::NoBarrier_Load(&rep_));
  }
  inline void NoBarrier_Store(void* v) {
    ::base::subtle::NoBarrier_Store(&rep_, reinterpret_cast<Rep>(v));
  }
};

// TODO(gabor): Implement actual compress
inline bool Snappy_Compress(const char* input, size_t input_length,
                            std::string* output) {
  return false;
}

// TODO(gabor): Implement actual uncompress
inline bool Snappy_Uncompress(const char* input_data, size_t input_length,
                              std::string* output) {
  return false;
}

inline bool GetHeapProfile(void (*func)(void*, const char*, int), void* arg) {
  return false;
}

}
}

#endif  // STORAGE_LEVELDB_PORT_PORT_STD_H_
