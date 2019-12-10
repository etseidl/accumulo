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

package org.apache.accumulo.master.tableOps.propertyChange;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.Namespaces;
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

public class NamespacePropertyChanged extends MasterRepo {
  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(NamespacePropertyChanged.class);

  private NamespaceId namespaceId;

  public NamespacePropertyChanged(NamespaceId namespaceId) {
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master env) throws Exception {
    log.debug("nsPropChange: reserve NS {}", namespaceId);
    return Utils.reserveNamespace(env, namespaceId, tid, false, false,
        TableOperation.PROPERTY_CHANGE);
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    // enumerate tables in namespace
    var tableIds = Namespaces.getTableIds(env.getContext(), namespaceId);

    log.debug("ns property change {}", namespaceId);

    // we could skip tables that override the namespace policies, but the check won't
    // affect them anyway. revisit if this operation is too slow.
    boolean sawError = false;
    for (TableId tableId : tableIds) {
      var tablePols = Utils.getPoliciesForTable(env.getConfigurationFactory(), tableId);
      VolumeChooserEnvironment chooserEnv =
          new VolumeChooserEnvironmentImpl(tableId, null, env.getContext());
      String tableDir =
          env.getFileSystem().choose(chooserEnv, ServerConstants.getBaseUris(env.getContext()))
              + Constants.HDFS_TABLES_DIR + Path.SEPARATOR + tableId;

      log.debug("check table {} ({},{})", tableDir, tablePols.storagePolicy,
          tablePols.encodingPolicy);

      try {
        env.getFileSystem().checkDirPoliciesRecursively(new Path(tableDir), tablePols.storagePolicy,
            tablePols.encodingPolicy);
      } catch (IOException e) {
        // catch this table and continue
        log.warn("error setting policies for tableId=" + tableId, e);
        sawError = true;
      }
    }

    if (sawError)
      throw new IOException(
          "trouble setting namespace properties, check error logs for more info.");

    Utils.getReadLock(env, namespaceId, tid).unlock();

    return null;
  }

  @Override
  public void undo(long tid, Master env) {
    Utils.unreserveNamespace(env, namespaceId, tid, false);
  }
}
