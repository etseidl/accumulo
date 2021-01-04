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
package org.apache.accumulo.core.util;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.fate.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionSampler implements Runnable, Closeable {
  private static final Logger log = LoggerFactory.getLogger(ExecutionSampler.class);

  private static long DEFAULT_MILLIS = 10;

  private String name;
  private Thread sampledThread;
  private Thread myThread = null;
  private long sampleMillis;

  private long startTime = -1;
  private long endTime = -1;

  private boolean shouldStop = false;
  private HashMap<String,Sample> samples = new HashMap<>();

  // set on command line: -Daccumulo.sampling=true
  private static final boolean isSampling;

  static {
    var t = System.getProperty("accumulo.sampling", "false").trim();
    isSampling = t.equalsIgnoreCase("true");
  }

  private static class Sample implements Comparable<Sample> {
    String name;
    long count = 0;

    Sample(String nm) {
      name = nm;
    }

    void inc() {
      count++;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Sample) {
        Sample s = (Sample) o;
        return name.equals(s.name) && count == s.count;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public int compareTo(Sample s) {
      // sort in descending order
      if (count > s.count)
        return -1;
      else if (count == s.count)
        return 0;
      return 1;
    }
  }

  public ExecutionSampler(String exname, Thread threadToSample, long millis) {
    this.name = exname;
    this.sampledThread = threadToSample;
    this.sampleMillis = millis;
  }

  public ExecutionSampler(String exname, Thread threadToSample) {
    this(exname, threadToSample, DEFAULT_MILLIS);
  }

  public ExecutionSampler(String exname) {
    this(exname, Thread.currentThread());
  }

  public ExecutionSampler() {
    this(Thread.currentThread().getName());
  }

  public ExecutionSampler(String exname, long millis) {
    this(exname, Thread.currentThread(), millis);
  }

  @Override
  public void run() {
    log.debug("starting sampler {}", name);
    startTime = System.currentTimeMillis();
    while (!shouldStop) {
      if (sampledThread.isAlive()) {
        var trace = sampledThread.getStackTrace();
        if (trace.length > 0) {
          var curr = trace[0];
          var method = curr.getClassName() + "." + curr.getMethodName();
          // System.out.println("in method: " + method);
          var sample = samples.get(method);
          if (sample == null) {
            sample = new Sample(method);
            samples.put(method, sample);
            // log.debug("new method {} {}", method, samples.size());
          }
          sample.inc();
        }
        UtilWaitThread.sleepUninterruptibly(sampleMillis, TimeUnit.MILLISECONDS);
      }
    }
    endTime = System.currentTimeMillis();
    log.debug("sampler {} done", name);
  }

  public void dumpSamples() {
    if (!isSampling)
      return;

    if (myThread != null && myThread.isAlive()) {
      System.out.println("cannot dump samples while active");
      return;
    }

    if (endTime == -1)
      endTime = System.currentTimeMillis();

    var s = new ArrayList<>(samples.values());
    Collections.sort(s);

    var b = new StringBuilder();
    b.append("samples for ").append(name).append('\n');
    b.append("  execution time ").append(endTime - startTime).append('\n');
    for (Sample k : s) {
      b.append(k.name).append(": ").append(k.count).append('\n');
    }
    System.out.println(b.toString());
  }

  /**
   * Signal sampler to stop, and then wait on the sampler thread to exit.
   */
  public void stop() {
    shouldStop = true;

    while (myThread != null) {
      try {
        myThread.join();
        myThread = null;
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * For Closable interface. Simply calls stop().
   */
  @Override
  public void close() {
    stop();
  }

  /**
   * Create a sampler and a thread to run it in. Thread will be started. calling stop() on the
   * returned ExecutionSampler will join() the thread.
   *
   * @param name
   *          name for sampler object
   * @param millis
   *          how long to sleep between samples
   * @return sampler which contains running thread
   */
  public static ExecutionSampler sample(String name, long millis) {
    ExecutionSampler sampler = new ExecutionSampler(name, millis);
    if (isSampling) {
      Thread samplerThread = new Thread(sampler);
      sampler.myThread = samplerThread;
      samplerThread.start();
    }
    return sampler;
  }

  public static ExecutionSampler sample(String name) {
    return sample(name, DEFAULT_MILLIS);
  }

  /* for debug...remove later */
  private static void foo(long niter) {
    for (long i = 0; i < niter; i++) {
      if (i < 0)
        System.out.println("ha!");
    }
  }

  private static void bar(long niter) {
    for (long i = 0; i < niter; i++) {
      if (i < 0)
        System.out.println("ha!");
    }
  }

  public static void main(String[] args) {
    try (var sampler = ExecutionSampler.sample("test", 10)) {

      for (int i = 0; i < 10; i++) {
        System.out.println(i);
        long niter = 100000000;
        foo(niter * 10);
        bar(niter);
      }

      sampler.stop();
      sampler.dumpSamples();
    }
  }
}
