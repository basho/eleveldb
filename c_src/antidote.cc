#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include <iostream>
using namespace std;

namespace leveldb {

    //Comparator::~Comparator() { }

    namespace {

        class AntidoteComparator : public Comparator {
          protected:

          public:
            AntidoteComparator() { }

            virtual const char* Name() const {
              return "AntidoteComparator";
            }

            virtual int Compare(const Slice& a, const Slice& b) const {
                if(a == b) {
                    return 0;
                }

                Slice ac = Slice(a.data());//, bc = Slice(b.data());

                //Slice aname = Get32PrefData(ac), bname = Get32PrefData(bc);

                //int set_cmp = aname.compare(bname);

                cout << ac.ToString() << endl;

                return 1;
            }

            // No need to shorten keys since it's fixed size.
            virtual void FindShortestSeparator(std::string* start,
                const Slice& limit) const {
            }

            // No need to shorten keys since it's fixed size.
            virtual void FindShortSuccessor(std::string* key) const {
            }


        };

    }

    static port::OnceType antidote_once = LEVELDB_ONCE_INIT;
    static const Comparator* antidote_cmp = NULL;

    static void InitAntidoteComparator() {
      antidote_cmp = new AntidoteComparator();
    }

    const Comparator* GetAntidoteComparator() {
      port::InitOnce(&antidote_once, InitAntidoteComparator);
      return antidote_cmp;
    }

    void AntidoteComparatorShutdown() {
        delete antidote_cmp;
        antidote_cmp = NULL;
    }

} //namespace antidote
