// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "translator.h"
#include "exceptionutils.h"

namespace leveldb {

size_t DefaultKeyTranslator::GetInternalKeySize(const Slice & user_key) const {
  return user_key.size();
}

void DefaultKeyTranslator::TranslateExternalKey(const Slice & user_key,
                                   char * buffer) const {
  memcpy(buffer, user_key.data(), user_key.size());
}

void DefaultKeyTranslator::TranslateInternalKey(const Slice & internal_key,
                                                std::string * out) const {
  out->append(internal_key.data(), internal_key.size());
}

static DefaultKeyTranslator default_translator;

DefaultKeyTranslator* GetDefaultKeyTranslator() {
    return &default_translator;
}

} // namespace leveldb
