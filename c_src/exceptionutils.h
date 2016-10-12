#ifndef EXCEPTIONUTILS_H
#define EXCEPTIONUTILS_H

#include <iostream>
#include <fstream>
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

#define FOUT(text) {                                                    \
        std::fstream outfile;                                           \
        outfile.open("/tmp/eleveldb.txt", std::fstream::out|std::fstream::app); \
        outfile << text << std::endl;                                   \
        outfile.close();                                                \
    }

#endif
