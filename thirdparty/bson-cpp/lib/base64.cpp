// util/base64.cpp


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

#include "base64.h"
#include <assert.h>

using namespace std;

namespace base64 {

    Alphabet alphabet;

    void encode( std::stringstream& ss , const char * data , int size ) {
        for ( int i=0; i<size; i+=3 ) {
            int left = size - i;
            const unsigned char * start = (const unsigned char*)data + i;

            // byte 0
            ss << alphabet.e(start[0]>>2);

            // byte 1
            unsigned char temp = ( start[0] << 4 );
            if ( left == 1 ) {
                ss << alphabet.e(temp);
                break;
            }
            temp |= ( ( start[1] >> 4 ) & 0xF );
            ss << alphabet.e(temp);

            // byte 2
            temp = ( start[1] & 0xF ) << 2;
            if ( left == 2 ) {
                ss << alphabet.e(temp);
                break;
            }
            temp |= ( ( start[2] >> 6 ) & 0x3 );
            ss << alphabet.e(temp);

            // byte 3
            ss << alphabet.e(start[2] & 0x3f);
        }

        int mod = size % 3;
        if ( mod == 1 ) {
            ss << "==";
        }
        else if ( mod == 2 ) {
            ss << "=";
        }
    }


    std::string encode( const char * data , int size ) {
        std::stringstream ss;
        encode( ss , data ,size );
        return ss.str();
    }

    std::string encode( const std::string& s ) {
        return encode( s.c_str() , s.size() );
    }


    void decode( std::stringstream& ss , const std::string& s ) {
        assert( s.size() % 4 == 0 );
        const unsigned char * data = (const unsigned char*)s.c_str();
        int size = s.size();

        unsigned char buf[3];
        for ( int i=0; i<size; i+=4) {
            const unsigned char * start = data + i;
            buf[0] = ( ( alphabet.decode[start[0]] << 2 ) & 0xFC ) |
                     ( ( alphabet.decode[start[1]] >> 4 ) & 0x3 );
            buf[1] = ( ( alphabet.decode[start[1]] << 4 ) & 0xF0 ) |
                     ( ( alphabet.decode[start[2]] >> 2 ) & 0xF );
            buf[2] = ( ( alphabet.decode[start[2]] << 6 ) & 0xC0 ) |
                     ( ( alphabet.decode[start[3]] & 0x3F ) );

            int len = 3;
            if ( start[3] == '=' ) {
                len = 2;
                if ( start[2] == '=' ) {
                    len = 1;
                }
            }
            ss.write( (const char*)buf , len );
        }
    }

    std::string decode( const std::string& s ) {
        std::stringstream ss;
        decode( ss , s );
        return ss.str();
    }

}

