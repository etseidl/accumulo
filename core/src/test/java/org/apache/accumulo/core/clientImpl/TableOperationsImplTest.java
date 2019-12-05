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

package org.apache.accumulo.core.clientImpl;

import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TableOperationsImplTest {

  @Test(expected = IllegalArgumentException.class)
  public void setInvalidStoragePolicyThrowsExcept()
      throws AccumuloException, AccumuloSecurityException {
    ClientContext context = new ClientContext(new Properties());
    TableOperationsImpl tableOpsImpl = new TableOperationsImpl(context);

    tableOpsImpl.setProperty("foo", Property.TABLE_STORAGE_POLICY.getKey(), "SPICY");
  }

  // the next three just need to validate we get past the arguments checking...
  // will throw AccumuloException because instance.name property not set
  @Rule
  public ExpectedException illegalArg = ExpectedException.none();

  @Test
  public void setEncodingPolicy() throws AccumuloException, AccumuloSecurityException {
    ClientContext context = new ClientContext(new Properties());
    TableOperationsImpl tableOpsImpl = new TableOperationsImpl(context);

    illegalArg.expect(AccumuloException.class);
    illegalArg.expectMessage("instance.name must be set!");
    tableOpsImpl.setProperty("foo", Property.TABLE_CODING_POLICY.getKey(), "something");
  }

  @Test
  public void setStoragePolicy() throws AccumuloException, AccumuloSecurityException {
    ClientContext context = new ClientContext(new Properties());
    TableOperationsImpl tableOpsImpl = new TableOperationsImpl(context);

    illegalArg.expect(AccumuloException.class);
    illegalArg.expectMessage("instance.name must be set!");
    tableOpsImpl.setProperty("foo", Property.TABLE_STORAGE_POLICY.getKey(),
        HdfsConstants.HOT_STORAGE_POLICY_NAME);
  }

  @Test
  public void setProvidedStoragePolicy() throws AccumuloException, AccumuloSecurityException {
    ClientContext context = new ClientContext(new Properties());
    TableOperationsImpl tableOpsImpl = new TableOperationsImpl(context);

    illegalArg.expect(AccumuloException.class);
    illegalArg.expectMessage("instance.name must be set!");
    tableOpsImpl.setProperty("foo", Property.TABLE_STORAGE_POLICY.getKey(), "PROVIDED");
  }
}
