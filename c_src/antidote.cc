#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include <iostream>
#include <map>
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

                Slice ac = Slice(a.data(), a.size()), bc = Slice(b.data(), b.size());

                // Trim Slices and compare Antidote keys (atoms)
                int aKeySize = checkAndTrimFirstBytes(ac);
                int bKeySize = checkAndTrimFirstBytes(bc);

                Slice aKey = Slice(ac.data(), aKeySize);
                Slice bKey = Slice(bc.data(), bKeySize);

                int key_compare = aKey.compare(bKey);

                if(key_compare) {
                    return key_compare;
                } else {
                    // If we are supplied with a key that only contains
                    // the antidote key, we can't continue parsing, therefore
                    // return -1 or 1 according to which key is the shorter.
                    if ((ac.size() - aKeySize) == 0) return -1;
                    if ((bc.size() - bKeySize) == 0) return 1;
                }

                // If keys are equal, continue with the vector clock
                // First trim the key
                ac.remove_prefix(aKeySize);
                bc.remove_prefix(bKeySize);

                // Check lists
                int aVCSize = checkList(ac);
                int bVCSize = checkList(bc);

                // Parse and compare VCs
                return compareVCs(parseVCMap(ac, aVCSize), parseVCMap(bc, bVCSize));
            }

            // Given a slice, checks that the first bytes match Erlang
            // external format + Antidote key, which starts with an atom.
            // Returns the size of the atom to read.
            static int checkAndTrimFirstBytes(Slice &s) {
                // External Term Format -> first byte = 131
                assert(s[0] == (char) 131);
                s.remove_prefix(1);

                // SMALL_TUPLE_EXT = 104
                assert(s[0] == (char) 104);
                s.remove_prefix(1);

                // ELEMENTS in tuple = not checked for now
                // assert(res[0] == (char) 1);
                s.remove_prefix(1);

                // ATOM_EXT = 100
                assert(s[0] == (char) 100);
                s.remove_prefix(1);

                // LENGTH of key
                Slice sc = Slice(s.data(), 2);
                s.remove_prefix(2);
                sc.remove_prefix(1);

                return (int) sc[0];
            }

            // Given a slice, checks that a list follows, and returns its size.
            static int checkList(Slice &s) {
                // NIL_EXT = 106 (Empty list = Empty Snapshot)
                if(s[0] == (char) 106) {
                    s.remove_prefix(1);
                    return 0;
                }

                // LIST_EXT = 108
                assert(s[0] == (char) 108);
                s.remove_prefix(1);

                // parse list size
                unsigned char size[4];
                size[3] = s[0];
                size[2] = s[1];
                size[1] = s[2];
                size[0] = s[3];

                s.remove_prefix(4);
                return *(int *)size;
            }

            static map<string, int> parseVCMap(Slice &s, int size) {
                map<string, int> VC;
                int value;
                string key;
                while(size > 0) {
                    checkTwoElementTuple(s);
                    key = parseAtom(s);
                    value = parseInt(s);
                    VC[key] = value;
                    size--;
                }
                return VC;
            }

            static void checkTwoElementTuple(Slice &s) {
                // SMALL_TUPLE_EXT == 104
                assert(s[0] == (char) 104);
                s.remove_prefix(1);
                // LENGTH == 2 (DC, clock)
                assert(s[0] == (char) 2);
                s.remove_prefix(1);
            }

            // Given a Slice parses a SMALL_INTEGER_EXT (97) or INTEGER_EXT (98)
            static int parseInt(Slice &s) {
                assert(s[0] == (char) 97 || s[0] == (char) 98);
                int res;
                if (s[0] == (char) 97) {
                    unsigned char size[1];
                    size[0] = s[1];
                    res = *(int *) size;
                    s.remove_prefix(2);
                } else {
                    unsigned char size[4];
                    size[3] = s[1];
                    size[2] = s[2];
                    size[1] = s[3];
                    size[0] = s[4];

                    s.remove_prefix(5);
                    res = *(int *) size;
                }
                return res;
            }

            // Given a Slice parses an ATOM_EXT
            static string parseAtom(Slice &s) {
                // ATOM_EXT = 100
                assert(s[0] == (char) 100);
                s.remove_prefix(1);

                // LENGTH
                Slice sc = Slice(s.data(), 2);
                s.remove_prefix(2);
                sc.remove_prefix(1);

                // Create the result string and trim its sice from the Slice.
                string res (s.data(), sc[0]);
                s.remove_prefix(sc[0]);
                return res;
            }

            // This method returns -1 * the comparison value, since
            // we are sorting keys from oldest to newest first.
            static int compareVCs(map<string, int> a, map<string, int> b) {
                if (a.size() > b.size()) {
                    // a is "newer" since it contains more keys.
                    return -1;
                }
                if (a.size() < b.size()) {
                    // b is "newer" since it contains more keys.
                    return 1;
                }
                map<string, int>::iterator keyIt;
                for(map<string, int>::iterator iterator = b.begin();
                                iterator != b.end(); iterator++) {
                    keyIt = a.find(iterator->first);
                    if(keyIt == a.end()) { // Key sets are !=
                        // we return 1 since a doesn't contain that key
                        // an therefore we should treat is as a 0.
                        return -1;
                    }
                    if(iterator->second > keyIt->second) {
                        continue;
                    } else {
                        return -1;
                    }
                }
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
