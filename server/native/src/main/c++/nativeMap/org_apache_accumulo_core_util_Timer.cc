/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "org_apache_accumulo_core_util_Timer.h"

extern "C" {
#include <sys/resource.h>
#include <unistd.h>
}

// FIXME
// for compiling on OSX...not to be used...should
// throw unsupported feature or something
#ifndef RUSAGE_THREAD
#define RUSAGE_THREAD RUSAGE_SELF
#endif

//////////////////////////////////////////////
// helper functions

static jdouble
getCpuTime(int type)
{
    double res;
    struct rusage r;
    getrusage(type, &r);
    res = r.ru_utime.tv_sec + r.ru_stime.tv_sec;
    res += 0.000001 * (r.ru_utime.tv_usec + r.ru_stime.tv_usec);
    return (jdouble) res;
}

static jdouble
getUserTime(int type)
{
    double res;
    struct rusage r;
    getrusage(type, &r);
    res = r.ru_utime.tv_sec + 0.000001 * r.ru_utime.tv_usec;
    return (jdouble) res;
}

static jdouble
getSystemTime(int type)
{
    double res;
    struct rusage r;
    getrusage(type, &r);
    res = r.ru_stime.tv_sec + 0.000001 * r.ru_stime.tv_usec;
    return (jdouble) res;
}

static void
getRusage(int type, JNIEnv * env, jclass cls, jlongArray arr)
{
    struct rusage r;
    getrusage(type, &r);
    env->SetLongArrayRegion(arr, 0, sizeof(r)/sizeof(long int), (const jlong *)&r);
}

//////////////////////////////////////////////
// jni calls 

JNIEXPORT jdouble JNICALL
Java_org_apache_accumulo_core_util_Timer_getProcessCpuTime(JNIEnv * env, jclass cls)
{
    return getCpuTime(RUSAGE_SELF);
}

JNIEXPORT jdouble JNICALL
Java_org_apache_accumulo_core_util_Timer_getProcessUserTime(JNIEnv * env, jclass cls)
{
    return getUserTime(RUSAGE_SELF);
}

JNIEXPORT jdouble JNICALL
Java_org_apache_accumulo_core_util_Timer_getProcessSystemTime(JNIEnv * env, jclass cls)
{
    return(RUSAGE_SELF);
}

JNIEXPORT void JNICALL
Java_org_apache_accumulo_core_util_Timer_getProcessRUsage(JNIEnv * env, jclass cls, jlongArray arr)
{
    getRusage(RUSAGE_SELF, env, cls, arr);
}


JNIEXPORT jdouble JNICALL
Java_org_apache_accumulo_core_util_Timer_getThreadCpuTime(JNIEnv *env, jclass cls)
{
    return getCpuTime(RUSAGE_THREAD);
}

JNIEXPORT jdouble JNICALL
Java_org_apache_accumulo_core_util_Timer_getThreadUserTime(JNIEnv *env, jclass cls)
{
    return getUserTime(RUSAGE_THREAD);
}

JNIEXPORT jdouble JNICALL
Java_org_apache_accumulo_core_util_Timer_getThreadSystemTime(JNIEnv *env, jclass cls)
{
    return getSystemTime(RUSAGE_THREAD);
}

JNIEXPORT void JNICALL
Java_org_apache_accumulo_core_util_Timer_getThreadRUsage(JNIEnv * env, jclass cls, jlongArray arr)
{
    getRusage(RUSAGE_THREAD, env, cls, arr);
}
