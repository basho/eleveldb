// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include <string>
#include "comparator.h"
#include "leveldb/slice.h"

using namespace leveldb;

namespace leveldb {

namespace {

// Time series key comparator.
// Assumes first 8 bytes are a little endian 64 bit unsigned integer,
// followed by family name size encoded in 2 little endian bytes,
// followed by the bytes forming the series name.
// TODO: Implement data dictionary, which changes the second part of the key
// from a byte array to a 4 byte little endian 32 bit unsigned integer.
// TODO: Add series family. A variable length string before the series name.
// When the family is present, sorting changes to (family, time, series).
// Without a family, it's (series, time).
class TSComparator : public Comparator {
  protected:

  public:
    TSComparator() { }

    virtual const char* Name() const {
      return "TSComparator";
    }

    static Slice GetTimeSlice(const Slice & s) {
      return Slice(s.data(), 8);
    }

    static Slice GetFamilySlice(const Slice & s) {
      return Slice(s.data() + 8, 4);
    }

    static Slice GetSeriesSlice(const Slice & s) {
      return Slice(s.data() + 12, 4);
    }

    static bool HasFamily(const Slice & s) {
      return s[8] || s[9] || s[10] || s[11];
    }

    virtual int Compare(const Slice& a, const Slice& b) const {
      Slice afam = GetFamilySlice(a), bfam = GetFamilySlice(b);
      int fam_cmp = afam.compare(bfam);

      if (fam_cmp)
        return fam_cmp;

      // Here, either same family or neither in a family.
      // If same family, sort by (time, series).
      if (HasFamily(a)) {
        Slice a_time = GetTimeSlice(a), b_time = GetTimeSlice(b);
        int time_cmp = a_time.compare(b_time);

        if (time_cmp)
          return time_cmp;

        Slice a_series = GetSeriesSlice(b), b_series = GetSeriesSlice(b);
        return a_series.compare(b_series);
      } else {
        // No family, so sort by (series, time)
        Slice a_series = GetSeriesSlice(b), b_series = GetSeriesSlice(b);
        int series_cmp = a_series.compare(b_series);

        if (series_cmp)
          return series_cmp;

        Slice a_time = GetTimeSlice(a), b_time = GetTimeSlice(b);
        return a_time.compare(b_time);
      }
    }

    // No need to shorten keys since it's fixed size.
    virtual void FindShortestSeparator(std::string* start,
                                       const Slice& limit) const {
    }

    // No need to shorten keys since it's fixed size.
    virtual void FindShortSuccessor(std::string* key) const {
    }

};

}  // namespace

static const TSComparator           tsComparator;

//------------------------------------------------------------
// GetBytewiseComparator just returns leveldb's default bytewise
// comparator
//------------------------------------------------------------

const Comparator* GetBytewiseComparator() {
    return leveldb::BytewiseComparator();
}

//------------------------------------------------------------
// GetTSComparator returns this module's internal TSComparator object
//------------------------------------------------------------

const Comparator* GetTSComparator() {
    return &tsComparator;
}

}  // namespace leveldb
