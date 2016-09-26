#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include <iostream>
#include <map>
#include <cmath>
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

                if ((a[3] != (char) 100) || (b[3] != (char) 100)) {
                    return 1;
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
                return compareVCs(ac, aVCSize, bc, bVCSize);
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

            static void checkTwoElementTuple(Slice &s) {
                // SMALL_TUPLE_EXT == 104
                assert(s[0] == (char) 104);
                s.remove_prefix(1);
                // LENGTH == 2 (DC, clock)
                assert(s[0] == (char) 2);
                s.remove_prefix(1);
            }

            // Given a Slice parses a SMALL_INTEGER_EXT (97), INTEGER_EXT (98)
            // SMALL_BIG_EXT (110) or LARGE_BIG_EXT (111)
            static unsigned long long int parseInt(Slice &s) {
                assert(s[0] == (char) 97 || s[0] == (char) 98
                    || s[0] == (char) 110 || s[0] == (char) 111);

                if (s[0] == (char) 97 || s[0] == (char) 98) {
                    return parseSmallInt(s);
                }

                return parseBigInt(s);
            }

            static unsigned long long int parseSmallInt(Slice &s) {
                unsigned long long int res;
                if (s[0] == (char) 97) {
                    unsigned char size[1];
                    size[0] = s[1];
                    res = *(unsigned long long int *) size;
                    s.remove_prefix(2);
                } else {
                    unsigned char size[4];
                    size[3] = s[1];
                    size[2] = s[2];
                    size[1] = s[3];
                    size[0] = s[4];

                    s.remove_prefix(5);
                    res = *(unsigned long long int *) size;
                }
                return res;
            }

            static unsigned long long int parseBigInt(Slice &s) {
                int intSize;
                unsigned long long int res = 0;
                if (s[0] == (char) 110) {
                    unsigned char size[1];
                    size[0] = s[1];
                    s.remove_prefix(2);
                    intSize = *(int *) size;
                } else {
                    unsigned char size[4];
                    size[3] = s[1];
                    size[2] = s[2];
                    size[1] = s[3];
                    size[0] = s[4];

                    s.remove_prefix(5);
                    intSize = *(int *) size;
                }
                // Clock time can't be negative, therefore this byte must be 0
                assert((int) s[0] == 0);
                s.remove_prefix(1);
                unsigned char current[1];
                int originalSize = intSize;
                while (intSize > 0) {
                    current[0] = s[0];
                    res += ((*(int *) current) * power(256, originalSize - intSize));
                    s.remove_prefix(1);
                    intSize--;
                }
                return res;
            }

            static unsigned long long int power(unsigned long long int base, int exp) {
                unsigned long long int result = 1;
                while(exp > 0) {
                    result *= base;
                    exp--;
                }
                return result;
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

                // Create the result string and trim its size from the Slice.
                string res (s.data(), sc[0]);
                s.remove_prefix(sc[0]);
                return res;
            }

            // This method returns -1 * the comparison value, since
            // we are sorting keys from oldest to newest first.
            static int compareVCs(Slice a, int aVCSize, Slice b, int bVCSize) {
                // If any of them is an empty snapshot,
                // return the comparison of the sizes
                if (aVCSize == 0 || bVCSize == 0) {
                    return sizeComparison(aVCSize, bVCSize);
                }
                unsigned long long int valueA, valueB;
                string keyA, keyB;
                int aSize = aVCSize, bSize = bVCSize;
                // Iterate the vector clocks and compare them
                while(aSize > 0 && bSize > 0) {
                    checkTwoElementTuple(a);
                    checkTwoElementTuple(b);
                    keyA = parseAtom(a);
                    keyB = parseAtom(b);
                    valueA = parseInt(a);
                    valueB = parseInt(b);

                    // Keys are sorted, so we leverage that
                    if(keyA.compare(keyB) == 0) {
                        // Same key
                        if(valueB == valueA) {
                            // Values for this key are equal,
                            // continue to next key
                            aSize--;
                            bSize--;
                            continue;
                        }
                        // Values are different, so return the comparison
                        if(valueB > valueA) {
                            return 1;
                        } else {
                            return -1;
                        }
                    } else {
                        // Key is different so return the comparison
                        return keyA.compare(keyB);
                    }
                }
                // VCs are the same until now
                // Check if any of them has more keys
                int vcSize = sizeComparison(aSize, bSize);
                if (vcSize != 0) {
                    return vcSize;
                } else {
                    // If the VC is equal, check if it's an op or a snap.
                    // If this returns 0, the key is identical
                    return sizeComparison(a.size(), b.size());
                }
            }

            static int sizeComparison(int sizeA, int sizeB) {
                if (sizeA == 0 && sizeB == 0){
                    return 0;
                } else {
                    if (sizeA > 0 && sizeB == 0) {
                        return -1;
                    } else {
                        return 1;
                    }
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
