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
 *
 */

package org.apache.skywalking.oap.server.storage.plugin.jdbc.h2.dao;

import org.apache.skywalking.oap.server.core.register.DatabaseAccessInventory;
import org.apache.skywalking.oap.server.core.storage.cache.IDatabaseAccessInventoryCacheDAO;
import org.apache.skywalking.oap.server.library.client.jdbc.hikaricp.JDBCHikariCPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wusheng
 */
public class H2DataAccessInventoryCacheDAO extends H2SQLExecutor implements IDatabaseAccessInventoryCacheDAO {
    private static final Logger logger = LoggerFactory.getLogger(H2DataAccessInventoryCacheDAO.class);
    private JDBCHikariCPClient h2Client;

    public H2DataAccessInventoryCacheDAO(JDBCHikariCPClient h2Client) {
        this.h2Client = h2Client;
    }

    @Override public int getSqlId(int serviceId,int endpointId,String name,String sql) {
        String id = DatabaseAccessInventory.buildId(serviceId,endpointId,name,sql);
        return getEntityIDByID(h2Client, DatabaseAccessInventory.SEQUENCE, DatabaseAccessInventory.INDEX_NAME, id);
    }



    @Override public DatabaseAccessInventory get(int sqlId) {
        try {
            return (DatabaseAccessInventory)getByColumn(h2Client, DatabaseAccessInventory.INDEX_NAME, DatabaseAccessInventory.SEQUENCE, sqlId, new DatabaseAccessInventory.Builder());
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override public List<DatabaseAccessInventory> loadLastUpdate(long lastUpdateTime) {
        List<DatabaseAccessInventory> serviceInventories = new ArrayList<>();

        try {
            StringBuilder sql = new StringBuilder("select * from ");
            sql.append(DatabaseAccessInventory.INDEX_NAME);
            sql.append(" and ").append(DatabaseAccessInventory.LAST_UPDATE_TIME).append(">?");

            try (Connection connection = h2Client.getConnection()) {
                try (ResultSet resultSet = h2Client.executeQuery(connection, sql.toString(),  lastUpdateTime)) {
                    DatabaseAccessInventory serviceInventory;
                    do {
                        serviceInventory = (DatabaseAccessInventory)toStorageData(resultSet, DatabaseAccessInventory.INDEX_NAME, new DatabaseAccessInventory.Builder());
                        if (serviceInventory != null) {
                            serviceInventories.add(serviceInventory);
                        }
                    }
                    while (serviceInventory != null);
                }
            } catch (SQLException e) {
                throw new IOException(e);
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
        return serviceInventories;
    }
}
