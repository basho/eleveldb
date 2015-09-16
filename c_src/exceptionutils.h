#ifndef EXCEPTIONUTILS_H
#define EXCEPTIONUTILS_H

#include <iostream>
#include <sstream>
#include <stdexcept>

#define ThrowRuntimeError(text) {\
    std::ostringstream _macroOs;\
    _macroOs << text;		\
    throw std::runtime_error(_macroOs.str());\
  }

#define COUT(text) {\
    std::ostringstream _macroOs;\
    _macroOs << text;		\
    std::cout << '\r' << _macroOs.str() << std::endl << "\r";\
}
#endif
