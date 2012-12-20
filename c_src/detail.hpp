#ifndef __ELEVELDB_DETAIL_HPP
 #define __ELEVELDB_DETAIL_HPP 1

/* These can be hopefully-replaced with constexpr or compile-time assert later: */
#if defined(OS_SOLARIS) || defined(SOLARIS) || defined(sun)
 #define ELEVELDB_IS_SOLARIS 1
#else
 #undef ELEVELDB_IS_SOLARIS 
#endif

#ifdef ELEVELDB_IS_SOLARIS
 #include <atomic.h>
#endif

namespace eleveldb { namespace detail {

template <class PtrT, class ValueT>
inline bool compare_and_swap(PtrT *ptr, const ValueT& comp_val, const ValueT& exchange_val)
{
#if ELEVELDB_IS_SOLARIS
    return (1==atomic_cas_32(ptr, comp_val, exchange_val));
#else
    return __sync_bool_compare_and_swap(ptr, comp_val, exchange_val);
#endif
}

// JFW: note: we don't support variadic version of this right now:
template <class PtrT, class ValueT>
inline void sync_add_and_fetch(PtrT *ptr, const ValueT& val)
{
#if ELEVELDB_IS_SOLARIS
    atomic_add_64(ptr, val);
#else
    __sync_add_and_fetch(ptr, val);
#endif
}

template <class PtrT>
inline void atomic_dec(PtrT ptr)
{
#if ELEVELDB_IS_SOLARIS
    // JFW: not found on this Solaris? atomic_sub_64(&h.work_queue_atomic, 1);
    atomic_dec_64(ptr);
#else
    __sync_sub_and_fetch(ptr, 1);
#endif
}

}} // namespace eleveldb::detail

#endif
