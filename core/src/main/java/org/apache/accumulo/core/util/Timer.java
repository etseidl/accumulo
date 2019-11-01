/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.core.util;

public class Timer {
  // total cpu (user + system). gets both in one call to getrusage()
  public static native double getProcessCpuTime();

  // just user
  public static native double getProcessUserTime();

  // just system
  public static native double getProcessSystemTime();

  // returns rusage struct as array. access via RUsage class
  private static native void getProcessRUsage(long[] struct);

  // total cpu (user + system). gets both in one call to getrusage()
  public static native double getThreadCpuTime();

  // just user
  public static native double getThreadUserTime();

  // just system
  public static native double getThreadSystemTime();

  // returns rusage struct as array. access via RUsage class
  private static native void getThreadRUsage(long[] struct);

  // TODO expose all members of struct rusage
  public static class RUsage {
    public enum RUsageType {
      SELF, THREAD
    };

    private static final int POS_USER_SEC = 0;
    private static final int POS_USER_USEC = 1;
    private static final int POS_SYS_SEC = 2;
    private static final int POS_SYS_USEC = 3;
    private static final int POS_MAX_RSS = 4;
    private static final int POS_IX_RSS = 5;
    private static final int POS_ID_RSS = 6;
    private static final int POS_IS_RSS = 7;
    private static final int POS_MIN_FLT = 8;
    private static final int POS_MAJ_FLT = 9;
    private static final int POS_NSWAP = 10;
    private static final int POS_IN_BLK = 11;
    private static final int POS_OUT_BLK = 12;
    private static final int POS_MSG_SND = 13;
    private static final int POS_MSG_RCV = 14;
    private static final int POS_N_SIG = 15;
    private static final int POS_NVCSW = 16;
    private static final int POS_NIVCSW = 17;

    private static final int LEN_RUSAGE = POS_NIVCSW + 1;

    private final long[] rusageArr = new long[LEN_RUSAGE];
    private final RUsageType rutype;

    // create struct and call getrusage() with default type of RUsageType.SELF
    // this will return rusage for the process.
    public RUsage() {
      this(RUsageType.SELF);
    }

    // create struct and call getrusage() with passed in type. RUsageType.SELF
    // will return rusage for the process, RUsageType.THREAD will return rusage
    // for the current thread.
    public RUsage(RUsageType type) {
      rutype = type;
      getRUsage();
    }

    // call getrusage() again...will overwrite old values. uses RUsageType passed
    // in constructor
    public void getRUsage() {
      switch (rutype) {
        case SELF:
          Timer.getProcessRUsage(rusageArr);
          break;
        case THREAD:
          Timer.getThreadRUsage(rusageArr);
          break;
      }
    }

    private double toMicros(long t, long ut) {
      return (double) t + (double) ut * 0.000001;
    }

    public double userTime() {
      return toMicros(rusageArr[POS_USER_SEC], rusageArr[POS_USER_USEC]);
    }

    public double systemTime() {
      return toMicros(rusageArr[POS_SYS_SEC], rusageArr[POS_SYS_USEC]);
    }

    public double cpuTime() {
      return userTime() + systemTime();
    }

    public long inBlock() {
      return rusageArr[POS_IN_BLK];
    }

    public long outBlock() {
      return rusageArr[POS_OUT_BLK];
    }
  }
}
