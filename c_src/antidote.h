#ifndef ANTIDOTE_COMPARATOR_H_
#define ANTIDOTE_COMPARATOR_H_

#include "leveldb/comparator.h"

namespace leveldb {

    extern const Comparator* GetAntidoteComparator();
}

#endif
