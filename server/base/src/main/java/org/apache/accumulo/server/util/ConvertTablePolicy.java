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

package org.apache.accumulo.server.util;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.beust.jcommander.Parameter;

/**
 * Utility to sync HDFS table directories with configured storage and erasure coding policies. After
 * changing table.hdfs.policy.storage or table.hdfs.policy.encoding, this tool can be run to change
 * the policies for the directories of the affected tables.
 *
 * The "-t" flag can be used to pass a comma separated list of tables to update, or the "-ns" flag
 * can be used to specify that all tables in the given namespace are to be converted.
 *
 * If the "-compact" flag is passed, then each table will be compacted to complete the conversion of
 * the RFiles.
 *
 * If the "-offline" flag is passed, then each table is taken offline before the conversion, and
 * brought back online after.
 */
public class ConvertTablePolicy {
  private static class ConvertOpts extends ServerUtilOpts {
    @Parameter(names = {"-ns", "--namespace"}, description = "Namespace to convert")
    String namespace = null;
    @Parameter(names = {"-t", "-table", "--table"},
        description = "Comma separated list of tables to convert")
    String table = null;
    @Parameter(names = {"-compact", "--compact"},
        description = "Perform table compaction after conversion")
    boolean doCompact;
    @Parameter(names = {"-offline", "--offline"},
        description = "Take table offline before conversion, and bring online after")
    boolean takeOffline;
  }

  private static void checkDirPoliciesRecursively(ServerContext ctx, Path path, Policies policies)
      throws IOException {
    var vm = ctx.getVolumeManager();
    FileSystem fs = vm.getVolumeByPath(path).getFileSystem();
    // check that path exists...it may not yet, which is ok. just return
    if (!fs.exists(path)) {
      System.err.println("check " + path + ": path does not exist");
      return;
    }

    // only need to do checks if HDFS
    if (fs instanceof DistributedFileSystem) {
      // check toplevel
      vm.checkDirPolicies(path, policies);

      // and then check children
      // TODO does the directory tree for a table ever get more than one level deep?
      var fstats = fs.listStatus(path);
      for (FileStatus fstat : fstats) {
        if (fstat.isDirectory()) {
          vm.checkDirPolicies(fstat.getPath(), policies);
        }
      }
    }
  }

  private static void updateTable(TableId tableId, ServerContext ctx) throws IOException {
    VolumeChooserEnvironment chooserEnv = new VolumeChooserEnvironmentImpl(tableId, null, ctx);

    System.out.println("  convert table...");

    // find all volumes table could live on
    String[] volumes =
        ctx.getVolumeManager().choosable(chooserEnv, ServerConstants.getBaseUris(ctx));

    // and ensure each is changed to the appropriate policies
    var policies =
        Policies.getPoliciesForTable(ctx.getServerConfFactory().getTableConfiguration(tableId));

    boolean sawError = false;
    for (String volume : volumes) {
      String tableDir = volume + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableId;
      System.out.println("    converting table directory " + tableDir + " to ("
          + policies.getStoragePolicy() + ", " + policies.getEncodingPolicy() + ")");
      try {
        checkDirPoliciesRecursively(ctx, new Path(tableDir), policies);
      } catch (IOException ioe) {
        // catch for this volume and continue
        System.err.println("error setting policies for tableId=" + tableId);
        System.err.println(ioe.getMessage());
        sawError = true;
      }
    }

    if (sawError)
      throw new IOException("trouble setting table policies for tableId=" + tableId);

    System.out.println("  done");
  }

  private static void offlineTable(AccumuloClient client, String table) throws Exception {
    System.out.print("  take table offline...");
    System.out.flush();
    client.tableOperations().offline(table, true);
    System.out.println("done");
  }

  private static void onlineTable(AccumuloClient client, String table) throws Exception {
    System.out.print("  bring table online...");
    System.out.flush();
    client.tableOperations().online(table, true);
    System.out.println("done");
  }

  private static void doTables(ConvertOpts opts, AccumuloClient client) {
    var serverContext = opts.getServerContext();

    StringTokenizer tok = new StringTokenizer(opts.table, ",", false);
    while (tok.hasMoreTokens()) {
      String tab = tok.nextToken().trim();
      System.out.println("converting table " + tab);

      try {
        TableId tid = Tables.getTableId(serverContext, tab);

        if (opts.takeOffline)
          offlineTable(client, tab);

        updateTable(tid, serverContext);

        if (opts.takeOffline)
          onlineTable(client, tab);

        if (opts.doCompact) {
          System.out.println("  starting compaction");
          client.tableOperations().compact(tab, null, null, true, false);
        }
      } catch (TableNotFoundException te) {
        System.err.println("no such table " + tab);
        System.err.println("skipping");
      } catch (IOException ioe) {
        System.err.println(ioe.getMessage());
      } catch (Exception ex) {
        System.err.println("error converting table " + tab);
        ex.printStackTrace(System.err);
      }
    }
  }

  private static void doNamespace(ConvertOpts opts, AccumuloClient client) {
    var serverContext = opts.getServerContext();

    List<TableId> tableIds = null;
    try {
      NamespaceId nsid = Namespaces.getNamespaceId(serverContext, opts.namespace);
      tableIds = Namespaces.getTableIds(serverContext, nsid);
    } catch (NamespaceNotFoundException nfe) {
      System.err.println("no such namespace " + opts.namespace);
      System.err.println("exiting");
      System.exit(1);
    }

    for (TableId tid : tableIds) {
      try {
        String tabName = Tables.getTableName(serverContext, tid);
        System.out.println("converting table " + tabName);

        if (opts.takeOffline)
          offlineTable(client, tabName);

        updateTable(tid, serverContext);

        if (opts.takeOffline)
          onlineTable(client, tabName);

        if (opts.doCompact) {
          try {
            System.out.println("  starting compaction");
            client.tableOperations().compact(tabName, null, null, true, false);
          } catch (Exception e) {
            System.err.println("error compacting tableid " + tid);
            System.err.println(e.getMessage());
            System.err.println("not compacting");
          }
        }
      } catch (IOException | TableNotFoundException ioe) {
        System.err.println(ioe.getMessage());
      } catch (Exception e) {
        System.err.println("error converting table");
        e.printStackTrace(System.err);
      }
    }
  }

  public static void main(String[] args) {
    ConvertOpts opts = new ConvertOpts();
    opts.parseArgs(ConvertTablePolicy.class.getName(), args);

    if (opts.namespace != null && opts.table != null) {
      System.err.println("can only use one of -ns or -t");
      return;
    }

    if (opts.namespace == null && opts.table == null) {
      System.err.println("must specify one of -ns or -t");
      return;
    }

    try (AccumuloClient client = Accumulo.newClient().from(opts.getClientProps()).build()) {
      if (opts.namespace != null) {
        doNamespace(opts, client);
      } else if (opts.table != null) {
        doTables(opts, client);
      }
    }
  }
}
