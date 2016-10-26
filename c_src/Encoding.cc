#include "Encoding.h"

#include "exceptionutils.h"

using namespace std;

using namespace eleveldb;

/**.......................................................................
 * Constructor.
 */
Encoding::Encoding() {}

/**.......................................................................
 * Destructor.
 */
Encoding::~Encoding() {}

ostream& 
eleveldb::operator<<(ostream& os, Encoding::Type type)
{
  switch (type) {
  case Encoding::ERLANG:
      os << "ERLANG";
      break;
  case Encoding::MSGPACK:
      os << "MSGPACK";
      break;
  default:
      os << "UNKNOWN";
      break;
  }

  return os;
}

std::string Encoding::encodingAtom(Encoding::Type type)
{
    switch(type) {
    case Encoding::ERLANG:
        return "erlang";
        break;
    case Encoding::MSGPACK:
        return "msgpack";
        break;
    default:
        return "unknown";
        break;
    }
}

unsigned char Encoding::encodingByte(Encoding::Type type)
{
    switch(type) {
    case Encoding::ERLANG:
        return ERLANG_MAGIC;
        break;
    case Encoding::MSGPACK:
        return MSGPACK_MAGIC;
        break;
    default:
        return 0;
        break;
    }
}

Encoding::Type Encoding::typeOf(std::string str, bool doThrow)
{
    if(str == encodingAtom(ERLANG))
        return ERLANG;

    if(str == encodingAtom(MSGPACK))
        return MSGPACK;

    if(doThrow)
        ThrowRuntimeError("Unrecognized encoding type: " << str);

    return UNKNOWN;
}

