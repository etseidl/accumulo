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

import java.io.Closeable;
import java.util.HashMap;

public class ExecutionSampler implements Runnable {
  private String name;
  private Thread sampledThread;
  private boolean shouldStop = false;
  private int sampleMillis = 10; // make this settable
  private HashMap<String,Sample> samples = new HashMap<>();

  private class Sample {
    long count = 0;

    void inc() {
      count++;
    }
  }

  public ExecutionSampler(String exname, Thread threadToSample) {
    this.name = exname;
    this.sampledThread = threadToSample;
  }

  public ExecutionSampler(Thread threadToSample) {
    this(threadToSample.getName(), threadToSample);
  }

  public ExecutionSampler(String exname) {
    this(exname, Thread.currentThread());
  }

  public ExecutionSampler() {
    this(Thread.currentThread().getName(), Thread.currentThread());
  }

  @Override
  public void run() {
    while (!shouldStop) {
      if (sampledThread.isAlive()) {
        var curr = sampledThread.getStackTrace()[0];
        var method = curr.getClassName() + "." + curr.getMethodName();
        // System.out.println("in method: " + method);
        var sample = samples.get(method);
        if (sample == null) {
          sample = new Sample();
          samples.put(method, sample);
        }
        sample.inc();
        try {
          Thread.sleep(sampleMillis);
        } catch (InterruptedException ie) {
          // ignored
        }
      }
    }
  }

  public void dumpSamples() {
    StringBuilder b = new StringBuilder();
    b.append("samples for " + name);
    b.append('\n');
    for (String k : samples.keySet()) {
      b.append(k + ": " + samples.get(k).count);
      b.append('\n');
    }
    System.out.println(b.toString());
  }

  public void stop() {
    shouldStop = true;
  }

  public static class SamplerThread extends Thread implements Closeable {
    ExecutionSampler sampler;

    SamplerThread(ExecutionSampler s) {
      super();
      sampler = s;
    }

    public ExecutionSampler sampler() {
      return sampler;
    }

    @Override
    public void close() {
      sampler.stop();
      try {
        this.join();
      } catch (InterruptedException ie) {
        // ignored
      }
    }

    public void dumpSamples() {
      sampler.dumpSamples();
    }
  }

  public static SamplerThread sample(String name) {
    ExecutionSampler sampler = new ExecutionSampler(name);
    SamplerThread sthread = new SamplerThread(sampler);
    sthread.start();
    return sthread;
  }

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
    ExecutionSampler sampler = new ExecutionSampler();
    Thread samplerThread = new Thread(sampler);
    samplerThread.start();

    long t1 = System.currentTimeMillis();

    for (int i = 0; i < 10; i++) {
      System.out.println(i);
      long niter = 100000000;
      foo(niter * 10);
      bar(niter);
    }

    long t2 = System.currentTimeMillis();

    System.out.println("execution time: " + (t2 - t1));

    sampler.stop();
    try {
      samplerThread.join();
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }

    sampler.dumpSamples();
  }
}
