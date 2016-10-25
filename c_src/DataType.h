#ifndef ELEVELDB_DATATYPE_H
#define ELEVELDB_DATATYPE_H

/**
 * DataType
 * 
 *   A class for managing data-type specifications
 *
 * Tagged: Wed Sep  9 11:10:10 PDT 2015
 * 
 * Original author: eleitch@basho.com
 */
#include <sstream>

namespace eleveldb {

  class DataType {
  public:

    enum Type {
      UNKNOWN   =      0x0,
      UNHANDLED =      0x1,
      NIL       =      0x2,
      ANY       =      0x4, 
      ARRAY     =      0x8,
      BIN       =     0x10,
      BOOL      =     0x20,
      CONST     =     0x40,
      CHAR      =     0x80,
      UCHAR_PTR =    0x100,
      EXT       =    0x200,
      INT8      =    0x400,
      UINT8     =    0x800,
      INT16     =   0x1000,
      UINT16    =   0x2000,
      INT32     =   0x4000,
      UINT32    =   0x8000,
      INT64     =  0x10000,
      UINT64    =  0x20000,
      FLOAT     =  0x40000,
      DOUBLE    =  0x80000,
      STRING    = 0x100000,
      MAP       = 0x200000,
      TIMESTAMP = 0x400000,
      SMALL_BIG = 0x800000
    };

    /**
     * Constructor.
     */
    DataType();

    /**
     * Destructor.
     */
    virtual ~DataType();

    friend std::ostream& operator<<(std::ostream& os,  DataType::Type type);

  }; // End class DataType

  //------------------------------------------------------------
  // Operator for printing data types
  //------------------------------------------------------------

  std::ostream& operator<<(std::ostream& os,  DataType::Type type);

} // End namespace eleveldb



#endif // End #ifndef ELEVELDB_DATATYPE_H
