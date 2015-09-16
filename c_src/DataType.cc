#include "DataType.h"

using namespace std;

using namespace eleveldb;

/**.......................................................................
 * Constructor.
 */
DataType::DataType() {}

/**.......................................................................
 * Destructor.
 */
DataType::~DataType() {}

ostream& 
eleveldb::operator<<(ostream& os, DataType::Type type)
{
  switch (type) {
  case DataType::ANY:
      os << "ANY";
      break;
  case DataType::UNHANDLED:
      os << "UNHANDLED";
      break;
  case DataType::NIL:
      os << "NIL";
      break;
  case DataType::ARRAY:
      os << "ARRAY";
      break;
  case DataType::BIN:
      os << "BIN";
      break;
  case DataType::BOOL:
      os << "BOOL";
      break;
  case DataType::CONST:
      os << "CONST";
      break;
  case DataType::CHAR:
      os << "CHAR";
      break;
  case DataType::UCHAR_PTR:
      os << "UCHAR_PTR";
      break;
  case DataType::EXT:
      os << "EXT";
      break;
  case DataType::INT8:
      os << "INT8";
      break;
  case DataType::UINT8:
      os << "UINT8";
      break;
  case DataType::INT16:
      os << "INT16";
      break;
  case DataType::UINT16:
      os << "UINT16";
      break;
  case DataType::INT32:
      os << "INT32";
      break;
  case DataType::UINT32:
      os << "UINT32";
      break;
  case DataType::INT64:
      os << "INT64";
      break;
  case DataType::UINT64:
      os << "UINT64";
      break;
  case DataType::FLOAT:
      os << "FLOAT";
      break;
  case DataType::DOUBLE:
      os << "DOUBLE";
      break;
  case DataType::STRING:
      os << "STRING";
      break;
  case DataType::MAP:
      os << "MAP";
      break;
  case DataType::TIMESTAMP:
      os << "TIMESTAMP";
      break;
  default:
      os << "UNKNOWN";
      break;
  }

  return os;
}
