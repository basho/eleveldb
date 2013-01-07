// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#ifndef __ELEVELDB_DETAIL_HPP
 #define __ELEVELDB_DETAIL_HPP 1

#include <stdint.h>

/* These can be hopefully-replaced with constexpr or compile-time assert later: */
#if defined(OS_SOLARIS) || defined(SOLARIS) || defined(sun)
 #define ELEVELDB_IS_SOLARIS 1
#else
 #undef ELEVELDB_IS_SOLARIS
#endif

#ifdef ELEVELDB_IS_SOLARIS
 #include <atomic.h>
#endif

namespace eleveldb {

// primary template
template <typename PtrT, typename ValueT>
inline bool compare_and_swap(volatile PtrT *ptr, const ValueT& comp_val, const ValueT& exchange_val);


// uint32 size (needed for solaris)
template <>
inline bool compare_and_swap(volatile uint32_t *ptr, const int& comp_val, const int& exchange_val)
{
#if ELEVELDB_IS_SOLARIS
    return (1==atomic_cas_32(ptr, comp_val, exchange_val));
#else
    return __sync_bool_compare_and_swap(ptr, comp_val, exchange_val);
#endif
}


// generic specification ... for pointers
template <typename PtrT, typename ValueT>
inline bool compare_and_swap(volatile PtrT *ptr, const ValueT& comp_val, const ValueT& exchange_val)
{
#if ELEVELDB_IS_SOLARIS
    return (comp_val==atomic_cas_ptr(ptr, comp_val, exchange_val));
#else
    return __sync_bool_compare_and_swap(ptr, comp_val, exchange_val);
#endif
}


template <typename ValueT>
inline ValueT add_and_fetch(volatile ValueT *ptr, const int& comp_val);

template <>
inline uint64_t add_and_fetch(volatile uint64_t *ptr, const int& val)
{
#if ELEVELDB_IS_SOLARIS
    return atomic_add_64_nv(ptr, val);
#else
    return __sync_add_and_fetch(ptr, val);
#endif
}

template <>
inline uint32_t add_and_fetch(volatile uint32_t *ptr, const int& val)
{
#if ELEVELDB_IS_SOLARIS
    return atomic_add_32_nv(ptr, val);
#else
    return __sync_add_and_fetch(ptr, val);
#endif
}


template <typename ValueT>
inline ValueT sub_and_fetch(volatile ValueT *ptr, const int& comp_val);

template <>
inline uint64_t sub_and_fetch(volatile uint64_t *ptr, const int& val)
{
#if ELEVELDB_IS_SOLARIS
    return atomic_dec_64_nv(ptr, val);
#else
    return __sync_sub_and_fetch(ptr, val);
#endif
}

template <>
inline uint32_t sub_and_fetch(volatile uint32_t *ptr, const int& val)
{
#if ELEVELDB_IS_SOLARIS
    return atomic_dec_32_nv(ptr, val);
#else
    return __sync_sub_and_fetch(ptr, val);
#endif
}
} // namespace eleveldb::detail

#endif
