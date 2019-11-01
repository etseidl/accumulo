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

import java.io.File;
import java.util.HashMap;

// singleton to manage per-thread RegionTimer instances.  kind of
// annoying that I have to use HashMap (modernizer complained about
// Hashtable, which is synchronized) rather than ConcurrentHashMap
// due to spot-bugs flagging the non-atomic check/put.  Should figure
// out how to suppress, since only the current thread can supply
// the missing key...don't have to worry about another thread adding
// this thread to the map.  Oh well, I don't expect this to be
// called all that often so it shouldn't be a big issue...just be
// sure to call once and save a timer, rather than repeatedly
// calling timerForThread() inside loops, say.
public class TimerManager {
  private static final HashMap<Thread,RegionTimer> sThreadTimers = new HashMap<>();

  static {
    String envAccumuloHome = System.getenv("ACCUMULO_HOME");
    File nativeDir = new File(envAccumuloHome + "/lib/native");
    String libname = System.mapLibraryName("accumulo");
    File libFile = new File(nativeDir, libname);
    System.out.println("load " + libFile.getAbsolutePath());
    try {
      System.load(libFile.getAbsolutePath());
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  public static RegionTimer timerForThread() {
    Thread curr = Thread.currentThread();

    synchronized (sThreadTimers) {
      if (!sThreadTimers.containsKey(curr)) {
        RegionTimer timer = new RegionTimer(curr.getName());
        sThreadTimers.put(curr, timer);
        return timer;
      }
    }
    return sThreadTimers.get(curr);
  }

  public static void setTimerForThread(RegionTimer timer) {
    Thread curr = Thread.currentThread();
    synchronized (sThreadTimers) {
      // will remove existing timer for this thread if one
      // exists
      sThreadTimers.put(curr, timer);
    }
  }

  public static RegionTimer removeTimerForThread() {
    Thread curr = Thread.currentThread();
    synchronized (sThreadTimers) {
      if (sThreadTimers.containsKey(curr)) {
        return sThreadTimers.remove(curr);
      }
    }
    return null;
  }
}
