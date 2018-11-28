// nonce.cpp

/*    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#define _CRT_RAND_S

#include "nonce.h"
#include <fstream>
#include <boost/static_assert.hpp>
#include <boost/thread/mutex.hpp>

namespace Nonce {

    BOOST_STATIC_ASSERT( sizeof(nonce) == 8 );

    Security::Security() {
        static int n;
        std::cout << n;
        assert(++n == 1 && "Security is a singleton class");
        init();
    }

    void Security::init() {
        if( _initialized ) return;
            _initialized = true;

      #if defined(__linux__) || defined(__sunos__)
        _devrandom = new std::ifstream("/dev/urandom", std::ios::binary|std::ios::in);
        assert(_devrandom->is_open() && "can't open dev/urandom");
      #elif defined(_WIN32)
        srand(time(NULL));
      #else
        srandomdev();
      #endif
    }

    nonce Security::getNonce() {
        static boost::mutex m;
        boost::mutex::scoped_lock lk(m);

        /* question/todo: /dev/random works on OS X.  is it better
           to use that than random() / srandom()?
        */

        nonce n;
      #if defined(__linux__) || defined(__sunos__)
        _devrandom->read((char*)&n, sizeof(n));
        assert(!_devrandom->fail() && "devrandom failed");
      #elif defined(_WIN32)
        unsigned a=0, b=0;
        errno_t err = rand_s(&a);
        assert( err == 0 );
        err = rand_s(&b);
        assert( err == 0 );
        n = (((unsigned long long)a)<<32) | b;
      #else
        n = (((unsigned long long)random())<<32) | random();
      #endif
        return n;
    }
    unsigned getRandomNumber() { return (unsigned) security.getNonce(); }

    bool Security::_initialized;
    Security security;


} // namespace mongo
