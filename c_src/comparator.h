// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ELEVELDB_COMPARATOR_H_
#define ELEVELDB_COMPARATOR_H_

#include <string>

#include "leveldb/comparator.h"

namespace leveldb {

class Slice;

// Return a builtin comparator that uses lexicographic byte-wise
// ordering. 

extern const Comparator* GetBytewiseComparator();
extern const Comparator* GetTSComparator();

}  // namespace leveldb

#endif  // ELEVELDB_COMPARATOR_H_
