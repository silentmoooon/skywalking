/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.core.register.service;

import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.CoreModule;
import org.apache.skywalking.oap.server.core.cache.DatabaseAccessInventoryCache;
import org.apache.skywalking.oap.server.core.register.DatabaseAccessInventory;
import org.apache.skywalking.oap.server.core.register.worker.InventoryStreamProcessor;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static java.util.Objects.isNull;

/**
 * @author peng-yongsheng
 */
public class DataAccessInventoryRegister implements IDataAccessInventoryRegister {

    private static final Logger logger = LoggerFactory.getLogger(DataAccessInventoryRegister.class);

    private final ModuleDefineHolder moduleDefineHolder;
    private DatabaseAccessInventoryCache cacheService;

    public DataAccessInventoryRegister(ModuleDefineHolder moduleDefineHolder) {
        this.moduleDefineHolder = moduleDefineHolder;
    }

    private DatabaseAccessInventoryCache getCacheService() {
        if (isNull(cacheService)) {
            cacheService =
                moduleDefineHolder.find(CoreModule.NAME).provider().getService(DatabaseAccessInventoryCache.class);
        }
        return cacheService;
    }


    @Override
    public int get(int serviceId, int endpointId,String name, String sql) {
        return getCacheService().getSqlId(serviceId, endpointId,name, sql);
    }

    @Override
    public int getOrCreate(int serviceId, int endpointId, String name, String sql) {
        int sqlId = getCacheService().getSqlId(serviceId,endpointId, name, sql);

        if (sqlId == Const.NONE) {
            DatabaseAccessInventory endpointInventory = new DatabaseAccessInventory();
            endpointInventory.setServiceId(serviceId);
            endpointInventory.setName(name);
            endpointInventory.setSql(sql);

            long now = System.currentTimeMillis();
            endpointInventory.setRegisterTime(now);
            endpointInventory.setHeartbeatTime(now);

            InventoryStreamProcessor.getInstance().in(endpointInventory);
        }
        return sqlId;
    }

    @Override
    public void heartbeat(int sqlId, long heartBeatTime) {
        DatabaseAccessInventory endpointInventory = getCacheService().get(sqlId);
        if (Objects.nonNull(endpointInventory)) {
            endpointInventory.setHeartbeatTime(heartBeatTime);

            InventoryStreamProcessor.getInstance().in(endpointInventory);
        } else {
            logger.warn("Endpoint {} heartbeat, but not found in storage.", sqlId);
        }
    }
}
