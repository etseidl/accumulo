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
package org.apache.accumulo.master.tableOps.propertyChange;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment;
import org.apache.accumulo.server.fs.VolumeChooserEnvironmentImpl;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TablePropertyChanged extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(TablePropertyChanged.class);

  private TableId tableId;
  private NamespaceId namespaceId;

  public TablePropertyChanged(NamespaceId namespaceId, TableId tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public long isReady(long tid, Master env) throws Exception {
    log.debug("tablePropChange: reserve NS {} TABLE {}", namespaceId, tableId);
    return Utils.reserveNamespace(env, namespaceId, tid, false, true,
        TableOperation.PROPERTY_CHANGE)
        + Utils.reserveTable(env, tableId, tid, true, true, TableOperation.PROPERTY_CHANGE);
  }

  static void updateTable(TableId tableId, Master env) throws IOException {
    VolumeChooserEnvironment chooserEnv =
        new VolumeChooserEnvironmentImpl(tableId, null, env.getContext());

    // find all volumes table could live on
    String[] volumes =
        env.getFileSystem().choosable(chooserEnv, ServerConstants.getBaseUris(env.getContext()));

    // and ensure each is changed to the appropriate polices
    var policies = Utils.getPoliciesForTable(env.getConfigurationFactory(), tableId);
    boolean sawError = false;
    for (String volume : volumes) {
      String tableDir = volume + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableId;

      log.debug("check table {} ({},{})", tableDir, policies.storagePolicy,
          policies.encodingPolicy);

      try {
        env.getFileSystem().checkDirPoliciesRecursively(new Path(tableDir), policies.storagePolicy,
            policies.encodingPolicy);
      } catch (IOException e) {
        // catch this table and continue
        log.warn("error setting policies for tableId=" + tableId, e);
        sawError = true;
      }
    }

    if (sawError)
      throw new IOException("trouble setting table properties, check error logs for more info.");
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    try {
      updateTable(tableId, env);
    } finally {
      Utils.unreserveTable(env, tableId, tid, true);
      Utils.unreserveNamespace(env, namespaceId, tid, false);
    }

    return null;
  }

  @Override
  public void undo(long tid, Master env) {
    Utils.unreserveTable(env, tableId, tid, true);
    Utils.unreserveNamespace(env, namespaceId, tid, false);
  }
}
