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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionTimer {
  private static final Logger log = LoggerFactory.getLogger(RegionTimer.class);

  private boolean wall_time_;
  private boolean cpu_time_; // includes read and write blocks, since they're collected at the same
                             // time
  private boolean count_;

  private Timer.RUsage rusage_;

  private TimedRegion top_;
  private TimedRegion current_;

  public RegionTimer() {
    this("total", true, true, true);
  }

  public RegionTimer(String topname) {
    this(topname, true, true, true);
  }

  public RegionTimer(String topname, boolean cpu_time, boolean wall_time, boolean counts) {
    wall_time_ = wall_time;
    cpu_time_ = cpu_time;
    count_ = counts;
    top_ = new TimedRegion(topname);
    if (count_)
      top_.count_enter();
    if (wall_time_)
      top_.wall_enter(get_wall_time());
    if (cpu_time_) {
      rusage_ = new Timer.RUsage(Timer.RUsage.RUsageType.THREAD);
      top_.cpu_enter(rusage_.cpuTime());
      top_.rd_blks_enter(rusage_.inBlock());
      top_.wrt_blks_enter(rusage_.outBlock());
    }
    current_ = top_;
  }

  // find first occurence of name and return copy of corresponding TimedRegion
  public Region getRegion(String name) {
    if (top_ == null)
      return null;

    if (name.equals(top_.name()))
      return new Region(top_);

    TimedRegion r = top_.findRegion(name);
    if (r != null)
      return new Region(r);

    // not found
    return null;
  }

  public void enter(String name) {
    current_ = current_.findinsubregion(name);
    if (count_)
      current_.count_enter();
    if (wall_time_)
      current_.wall_enter(get_wall_time());
    if (cpu_time_) {
      rusage_.getRUsage();
      current_.cpu_enter(rusage_.cpuTime());
      current_.rd_blks_enter(rusage_.inBlock());
      current_.wrt_blks_enter(rusage_.outBlock());
    }
  }

  public void change(String newname) {
    if (current_ == null) {
      log.warn("change({}): no current region begin timed", newname);
      return;
    }
    double wall = 0.0;
    if (count_)
      current_.count_exit();
    if (wall_time_)
      current_.wall_exit(wall = get_wall_time());
    if (cpu_time_) {
      rusage_.getRUsage();
      current_.cpu_exit(rusage_.cpuTime());
      current_.rd_blks_exit(rusage_.inBlock());
      current_.wrt_blks_exit(rusage_.outBlock());
    }
    if (current_.up() == null) {
      log.warn("change: already at top level");
      return;
    }
    current_ = current_.up();
    current_ = current_.findinsubregion(newname);
    if (count_)
      current_.count_enter();
    if (wall_time_)
      current_.wall_enter(wall);
    if (cpu_time_) {
      // don't need to call rusage again
      current_.cpu_enter(rusage_.cpuTime());
      current_.rd_blks_enter(rusage_.inBlock());
      current_.wrt_blks_enter(rusage_.outBlock());
    }
  }

  public void exit(String name) {
    if (current_ == null) {
      log.warn("exit({}): no current region being timed", name);
      return;
    }
    if (!current_.name().equals(name)) {
      log.warn("exit: region names don't match. {} != {}", name, current_.name());
      // see if we exited prematurely from a child
      TimedRegion tr = current_.up();
      boolean isChild = false;
      while (tr != null) {
        if (tr.name().equals(name)) {
          isChild = true;
          break;
        }
        tr = tr.up();
      }
      // ok, orphan, so exit() until we reach "name")
      if (isChild) {
        do {
          exit(current_.name());
        } while (!current_.name().equals(name));
      } else {
        // this is a true mess...punt and hope a subsequent exit will
        // clean things up.
        log.error("{} is not a parent of {}. examine use of timing code!", name, current_.name());
        return;
      }
    }
    if (count_)
      current_.count_exit();
    if (wall_time_)
      current_.wall_exit(get_wall_time());
    if (cpu_time_) {
      rusage_.getRUsage();
      current_.cpu_exit(rusage_.cpuTime());
      current_.rd_blks_exit(rusage_.inBlock());
      current_.wrt_blks_exit(rusage_.outBlock());
    }
    if (current_.up() == null) {
      log.warn("exit({}): already at top level", name);
      return;
    }
    current_ = current_.up();
  }

  public void add_wall_time(String name, double t) {
    if (wall_time_) {
      current_ = current_.findinsubregion(name);
      current_.wall_add(t);
      current_ = current_.up();
    }
  }

  public void add_cpu_time(String name, double t) {
    if (cpu_time_) {
      current_ = current_.findinsubregion(name);
      current_.cpu_add(t);
      current_ = current_.up();
    }
  }

  public void add_count(String name, long c) {
    if (count_) {
      current_ = current_.findinsubregion(name);
      current_.count_add(c);
      current_ = current_.up();
    }
  }

  public void add_rd_blks(String name, long c) {
    if (count_) {
      current_ = current_.findinsubregion(name);
      current_.rd_blks_add(c);
      current_ = current_.up();
    }
  }

  public void add_wrt_blks(String name, long c) {
    if (count_) {
      current_ = current_.findinsubregion(name);
      current_.wrt_blks_add(c);
      current_ = current_.up();
    }
  }

  private void update_top() {
    if (count_)
      top_.count_exit();
    if (wall_time_)
      top_.wall_exit(get_wall_time());
    if (cpu_time_) {
      rusage_.getRUsage();
      top_.cpu_exit(rusage_.cpuTime());
      top_.rd_blks_exit(rusage_.inBlock());
      top_.wrt_blks_exit(rusage_.outBlock());
    }
  }

  private int nregion() {
    return top_.nregion();
  }

  private void get_region_names(ArrayList<String> names) {
    top_.get_region_names(names);
  }

  private void get_counts(ArrayList<Long> counts) {
    top_.get_counts(counts);
  }

  private void get_rd_blks(ArrayList<Long> counts) {
    top_.get_rd_blks(counts);
  }

  private void get_wrt_blks(ArrayList<Long> counts) {
    top_.get_wrt_blks(counts);
  }

  private void get_wall_times(ArrayList<Double> wall) {
    top_.get_wall_times(wall);
  }

  private void get_cpu_times(ArrayList<Double> cpu) {
    top_.get_cpu_times(cpu);
  }

  private void get_depth(ArrayList<Integer> depth) {
    top_.get_depth(depth);
  }

  private double get_wall_time() {
    return System.currentTimeMillis() / 1000.0;
  }

  public void print() {
    print(System.out);
  }

  public void print(PrintStream out) {
    update_top();

    int n = nregion();
    ArrayList<Double> cpu_time = null;
    ArrayList<Double> wall_time = null;
    ArrayList<Long> count = null;
    ArrayList<Long> rd_blks = null;
    ArrayList<Long> wrt_blks = null;

    if (cpu_time_) {
      cpu_time = new ArrayList<>(n);
      get_cpu_times(cpu_time);
      rd_blks = new ArrayList<>(n);
      get_rd_blks(rd_blks);
      wrt_blks = new ArrayList<>(n);
      get_wrt_blks(wrt_blks);
    }

    if (wall_time_) {
      wall_time = new ArrayList<>(n);
      get_wall_times(wall_time);
    }

    if (count_) {
      count = new ArrayList<>(n);
      get_counts(count);
    }

    ArrayList<String> names = new ArrayList<>(n);
    get_region_names(names);

    ArrayList<Integer> depth = new ArrayList<>(n);
    get_depth(depth);

    int maxwidth = 0;
    double maxcputime = 0.0;
    double maxwalltime = 0.0;
    double maxcount = 0.0;
    double maxrd = 0.0;
    double maxwrt = 0.0;
    for (int i = 0; i < n; i++) {
      int width = names.get(i).length() + 2 * depth.get(i) + 2;
      if (width > maxwidth)
        maxwidth = width;
      if (cpu_time_) {
        if (cpu_time.get(i) > maxcputime)
          maxcputime = cpu_time.get(i);
        if (rd_blks.get(i) > maxrd)
          maxrd = rd_blks.get(i);
        if (wrt_blks.get(i) > maxwrt)
          maxwrt = wrt_blks.get(i);
      }
      if (wall_time_ && wall_time.get(i) > maxwalltime)
        maxwalltime = wall_time.get(i);
      if (count_ && count.get(i) > maxcount)
        maxcount = count.get(i);
    }

    int maxcountwidth = 5;
    maxcount /= 10000.0; // take into account heading
    while (maxcount >= 10.0) {
      maxcount /= 10.0;
      maxcountwidth++;
    }

    int maxrdwidth = 7;
    maxrd /= 1000000.0;
    while (maxrd >= 10.0) {
      maxrd /= 10.0;
      maxrdwidth++;
    }

    int maxwrtwidth = 8;
    maxwrt /= 10000000.0;
    while (maxwrt >= 10.0) {
      maxwrt /= 10.0;
      maxwrtwidth++;
    }

    int maxwallwidth = 4;
    while (maxwalltime >= 10.0) {
      maxwalltime /= 10.0;
      maxwallwidth++;
    }

    int maxcpuwidth = 4;
    while (maxcputime >= 10.0) {
      maxcputime /= 10.0;
      maxcpuwidth++;
    }

    StringBuilder b = new StringBuilder();
    Formatter f = new Formatter(b, Locale.US);

    for (int i = 0; i < maxwidth; i++)
      b.append(" ");
    if (cpu_time_)
      f.format(" %" + maxcpuwidth + "s", "CPU");
    if (wall_time_)
      f.format(" %" + maxwallwidth + "s", "Wall");
    if (cpu_time_) {
      f.format(" %" + maxrdwidth + "s", "Rd Blks");
      f.format(" %" + maxwrtwidth + "s", "Wrt Blks");
    }
    if (count_)
      f.format(" %" + maxcountwidth + "s", "Count");
    b.append('\n');

    for (int i = 0; i < n; i++) {
      int width = names.get(i).length() + 2 * depth.get(i) + 2;
      for (int j = 0; j < depth.get(i); j++)
        b.append("  ");
      b.append(names.get(i) + ": ");
      for (int j = width; j < maxwidth; j++)
        b.append(" ");
      if (cpu_time_)
        f.format(" %" + maxcpuwidth + ".2f", cpu_time.get(i));
      if (wall_time_)
        f.format(" %" + maxwallwidth + ".2f", wall_time.get(i));
      if (cpu_time_) {
        f.format(" %" + maxrdwidth + "d", rd_blks.get(i));
        f.format(" %" + maxwrtwidth + "d", wrt_blks.get(i));
      }
      if (count_)
        f.format(" %" + maxcountwidth + "d", count.get(i));
      b.append('\n');
    }

    out.print(b.toString());
  }

  public String toJSON() {
    StringBuilder b = new StringBuilder();
    top_.toJSON(b, wall_time_, cpu_time_);
    return b.toString();
  }

  public static void main(String[] args) {
    try {
      long n = 1024000;
      System.loadLibrary("accumulo");
      RegionTimer t = new RegionTimer("test", true, true, true);
      t.enter("loop");
      FileOutputStream fos = new FileOutputStream("foo.out");
      FileInputStream fis = new FileInputStream("foo.in");
      byte[] buf = new byte[1024];
      Timer.RUsage thd1 = new Timer.RUsage(Timer.RUsage.RUsageType.THREAD);
      Timer.RUsage self1 = new Timer.RUsage(Timer.RUsage.RUsageType.SELF);
      for (int i = 0; i < n; i++) {
        t.enter("potrzebie");
        int nr = fis.read(buf);
        if (nr < 0)
          throw new RuntimeException("args");
        fos.write(buf);
        // if (i*i < 0)
        // System.out.println("foo");
        /*
         * try { Thread.sleep(1); } catch (Exception e) { System.out.println("sleep"); }
         */
        t.exit("potrzebie");
      }
      Timer.RUsage thd2 = new Timer.RUsage(Timer.RUsage.RUsageType.THREAD);
      Timer.RUsage self2 = new Timer.RUsage(Timer.RUsage.RUsageType.SELF);
      System.out.println(self1.inBlock() + " " + self2.inBlock());
      System.out.println(self1.outBlock() + " " + self2.outBlock());
      System.out.println(thd1.inBlock() + " " + thd2.inBlock());
      System.out.println(thd1.outBlock() + " " + thd2.outBlock());

      fos.close();
      fis.close();

      t.exit("loop");
      t.enter("loop2");
      double td = 0.0;
      for (int i = 0; i < n; i++) {
        Timer.RUsage t1 = new Timer.RUsage(Timer.RUsage.RUsageType.THREAD);
        Timer.RUsage t2 = new Timer.RUsage(Timer.RUsage.RUsageType.THREAD);
        double delta = t2.userTime() + t2.systemTime() - t1.userTime() - t1.systemTime();
        td += delta;
        if (delta > 0.1) {
          System.out.println("huh? " + delta);
        }
      }
      t.add_cpu_time("inner", td);
      t.add_count("inner", (int) n);
      t.exit("loop2");
      t.print();
      System.out.println("td = " + td);
      System.out.println(t.toJSON());

      // test error handling
      t.enter("good1");
      t.enter("good2");
      t.enter("good3");
      t.exit("good1");
      System.out.println(t.toJSON());

      t.enter("good1");
      t.enter("good2");
      t.enter("good3");
      t.exit("bad1");
      t.exit("good1");
      System.out.println(t.toJSON());

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  class TimedRegion {
    private String name_;
    private TimedRegion up_;
    private TimedRegion subregions_;
    private TimedRegion next_;
    private TimedRegion prev_;
    private double cpu_time_;
    private long count_;
    private double wall_time_;
    private double cpu_enter_;
    private double wall_enter_;
    private long rd_blks_;
    private long rd_enter_;
    private long wrt_blks_;
    private long wrt_enter_;

    private TimedRegion insert_after(String name) {
      TimedRegion res = new TimedRegion(name);
      res.prev_ = this;
      res.next_ = this.next_;
      if (res.next_ != null)
        res.next_.prev_ = res;
      res.up_ = this.up_;
      this.next_ = res;
      return res;
    }

    private TimedRegion insert_before(String name) {
      TimedRegion res = new TimedRegion(name);
      res.next_ = this;
      res.prev_ = this.prev_;
      if (res.prev_ != null)
        res.prev_.next_ = res;
      res.up_ = this.up_;
      this.prev_ = res;
      return res;
    }

    private void rewindSubregions() {
      if (subregions_ != null) {
        while (subregions_.prev_ != null)
          subregions_ = subregions_.prev_;
      }
    }

    TimedRegion(String name) {
      name_ = name;
      wall_time_ = cpu_time_ = 0.0;
      count_ = 0;
      up_ = null;
      subregions_ = null;
      next_ = prev_ = null;
    }

    public String name() {
      return name_;
    }

    // do DFS to find region named "name"
    TimedRegion findRegion(String name) {
      if (name.equals(name_))
        return this;

      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        if (name.equals(r.name_))
          return r;
        TimedRegion sr;
        if ((sr = r.findRegion(name)) != null)
          return sr;
        r = r.next_;
      }

      return null;
    }

    TimedRegion findinsubregion(String name) {
      if (subregions_ == null) {
        subregions_ = new TimedRegion(name);
        subregions_.up_ = this;
        return subregions_;
      }
      int cmp = subregions_.name_.compareTo(name);
      if (cmp < 0) {
        do {
          if (subregions_.next_ == null) {
            return subregions_.insert_after(name);
          }
          subregions_ = subregions_.next_;
        } while ((cmp = subregions_.name_.compareTo(name)) < 0);
        if (cmp == 0)
          return subregions_;
        subregions_ = subregions_.insert_before(name);
      } else if (cmp > 0) {
        do {
          if (subregions_.prev_ == null) {
            return subregions_.insert_before(name);
          }
          subregions_ = subregions_.prev_;
        } while ((cmp = subregions_.name_.compareTo(name)) > 0);
        if (cmp == 0)
          return subregions_;
        subregions_ = subregions_.insert_after(name);
      }
      return subregions_;
    }

    void cpu_enter(double t) {
      cpu_enter_ = t;
    }

    void count_enter() {
      count_++;
    }

    void wall_enter(double t) {
      wall_enter_ = t;
    }

    void rd_blks_enter(long l) {
      rd_enter_ = l;
    }

    void wrt_blks_enter(long l) {
      wrt_enter_ = l;
    }

    void cpu_exit(double t) {
      cpu_time_ += t - cpu_enter_;
      cpu_enter_ = t;
    }

    void count_exit() {}

    void wall_exit(double t) {
      wall_time_ += t - wall_enter_;
      wall_enter_ = t;
    }

    void rd_blks_exit(long l) {
      rd_blks_ += l - rd_enter_;
      rd_enter_ = l;
    }

    void wrt_blks_exit(long l) {
      wrt_blks_ += l - wrt_enter_;
      wrt_enter_ = l;
    }

    void cpu_add(double t) {
      cpu_time_ += t;
    }

    void wall_add(double t) {
      wall_time_ += t;
    }

    void count_add(long t) {
      count_ += t;
    }

    void rd_blks_add(long l) {
      rd_blks_ += l;
    }

    void wrt_blks_add(long l) {
      wrt_blks_ += l;
    }

    public TimedRegion up() {
      return up_;
    }

    int nregion() {
      int n = 1;
      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        n += r.nregion();
        r = r.next_;
      }
      return n;
    }

    void get_region_names(ArrayList<String> names) {
      names.add(name());
      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        r.get_region_names(names);
        r = r.next_;
      }
    }

    void get_counts(ArrayList<Long> count) {
      count.add(count_);
      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        r.get_counts(count);
        r = r.next_;
      }
    }

    void get_rd_blks(ArrayList<Long> rd_blks) {
      rd_blks.add(rd_blks_);
      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        r.get_rd_blks(rd_blks);
        r = r.next_;
      }
    }

    void get_wrt_blks(ArrayList<Long> wrt_blks) {
      wrt_blks.add(wrt_blks_);
      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        r.get_wrt_blks(wrt_blks);
        r = r.next_;
      }
    }

    void get_wall_times(ArrayList<Double> wall) {
      wall.add(wall_time_);
      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        r.get_wall_times(wall);
        r = r.next_;
      }
    }

    void get_cpu_times(ArrayList<Double> cpu) {
      cpu.add(cpu_time_);
      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        r.get_cpu_times(cpu);
        r = r.next_;
      }
    }

    void get_depth(ArrayList<Integer> depth) {
      get_depth(depth, 0);
    }

    void get_depth(ArrayList<Integer> depth, int current_depth) {
      depth.add(current_depth);
      rewindSubregions();
      TimedRegion r = subregions_;
      while (r != null) {
        r.get_depth(depth, current_depth + 1);
        r = r.next_;
      }
    }

    void toJSON(StringBuilder b, boolean wall, boolean cpu) {
      b.append('{');
      b.append("\"name\": \"");
      b.append(name_);
      b.append("\", \"count\": ");
      b.append(count_);
      if (wall) {
        b.append(", \"wall\": ");
        b.append(wall_time_);
      }
      if (cpu) {
        b.append(", \"cpu\": ");
        b.append(cpu_time_);
      }
      if (subregions_ != null) {
        b.append(", \"subRegions\": [");
        rewindSubregions();
        TimedRegion r = subregions_;
        boolean first = true;
        while (r != null) {
          if (!first)
            b.append(',');
          r.toJSON(b, wall, cpu);
          r = r.next_;
          first = false;
        }
        b.append("]");
      }
      b.append('}');
    }
  }

  // publicly available read-only copy of a TimedRegion
  // make everything public since it's just a struct anyway
  public static class Region {
    public String name;
    public double cpu_time;
    public long count;
    public double wall_time;
    public long rd_blks;
    public long wrt_blks;

    Region(TimedRegion tr) {
      if (tr != null) {
        name = tr.name();
        count = tr.count_;
        cpu_time = tr.cpu_time_;
        wall_time = tr.wall_time_;
        rd_blks = tr.rd_blks_;
        wrt_blks = tr.wrt_blks_;
      }
    }
  }
}
