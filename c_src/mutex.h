// -------------------------------------------------------------------
//
// eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
//
// Copyright (c) 2011-2013 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------
#ifndef INCL_MUTEX_H
#define INCL_MUTEX_H

#include <pthread.h>

namespace eleveldb {


/**
 * Autoinitializing mutex object
 */
class Mutex
{
protected:
    pthread_mutex_t m_Mutex;

public:
    Mutex() {pthread_mutex_init(&m_Mutex, NULL);};

    ~Mutex() {pthread_mutex_destroy(&m_Mutex);};

    pthread_mutex_t & get() {return(m_Mutex);};

//    pthread_mutex_t * operator() {return(&m_Mutex);};

    void Lock() {pthread_mutex_lock(&m_Mutex);};

    void Unlock() {pthread_mutex_unlock(&m_Mutex);};

private:
    Mutex(const Mutex & rhs);             // no copy
    Mutex & operator=(const Mutex & rhs); // no assignment

};  // class Mutex


/**
 * Automatic lock and unlock of mutex
 */
class MutexLock
{
protected:

    Mutex & m_MutexObject;

public:

    explicit MutexLock(Mutex & MutexObject)
        : m_MutexObject(MutexObject)
    {m_MutexObject.Lock();};

    ~MutexLock() {m_MutexObject.Unlock();};

private:

    MutexLock();                                  // no default constructor
    MutexLock(const MutexLock & rhs);             // no copy constructor
    MutexLock & operator=(const MutexLock & rhs); // no assignment constructor
};  // class MutexLock

} // namespace eleveldb


#endif  // INCL_MUTEX_H
