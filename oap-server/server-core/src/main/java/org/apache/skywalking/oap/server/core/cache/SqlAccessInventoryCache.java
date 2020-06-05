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

package org.apache.skywalking.oap.server.core.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.CoreModuleConfig;
import org.apache.skywalking.oap.server.core.register.SqlAccessInventory;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.cache.ISqlAccessInventoryCacheDAO;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.library.module.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * @author peng-yongsheng
 */
public class SqlAccessInventoryCache implements Service {

    private static final Logger logger = LoggerFactory.getLogger(SqlAccessInventoryCache.class);

    private final Cache<String, Integer> databaseCache;
    private final Cache<Integer, SqlAccessInventory> databaseIdCache;
    private final SqlAccessInventory sqlAccessInventory;
    private final ModuleManager moduleManager;
    private ISqlAccessInventoryCacheDAO cacheDAO;

    public SqlAccessInventoryCache(ModuleManager moduleManager, CoreModuleConfig moduleConfig) {
        this.moduleManager = moduleManager;
        this.sqlAccessInventory = new SqlAccessInventory();
        this.sqlAccessInventory.setSequence(Const.USER_ENDPOINT_ID);
        this.sqlAccessInventory.setName(Const.USER_CODE);
        this.sqlAccessInventory.setServiceId(Const.USER_SERVICE_ID);

        long initialSize = moduleConfig.getMaxSizeOfServiceInventory() / 10L;
        int initialCapacitySize = (int)(initialSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : initialSize);

        databaseCache = CacheBuilder.newBuilder().initialCapacity(initialCapacitySize).maximumSize(moduleConfig.getMaxSizeOfServiceInventory()).build();
        databaseIdCache = CacheBuilder.newBuilder().initialCapacity(initialCapacitySize).maximumSize(moduleConfig.getMaxSizeOfServiceInventory()).build();
    }

    private ISqlAccessInventoryCacheDAO getCacheDAO() {
        if (isNull(cacheDAO)) {
            this.cacheDAO = moduleManager.find(StorageModule.NAME).provider().getService(ISqlAccessInventoryCacheDAO.class);
        }
        return this.cacheDAO;
    }

    public int getSqlId(int serviceId,int endpointId,String name,String sql) {
        String id= SqlAccessInventory.buildId(serviceId,endpointId,name,sql);
        Integer sqlId = databaseCache.getIfPresent(id);

        if (Objects.isNull(sqlId) || sqlId == Const.NONE) {
            sqlId = getCacheDAO().getSqlId(serviceId,endpointId,name,sql);
            if (sqlId != Const.NONE) {
                databaseCache.put(id, sqlId);
            }
        }
        return sqlId;
    }



    public SqlAccessInventory get(int sqlId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get service by id {} from cache", sqlId);
        }
        if (Const.USER_ENDPOINT_ID == sqlId) {
            return sqlAccessInventory;
        }


        SqlAccessInventory serviceInventory = databaseIdCache.getIfPresent(sqlId);

        if (isNull(serviceInventory)) {
            serviceInventory = getCacheDAO().get(sqlId);
            if (nonNull(serviceInventory)) {
                databaseIdCache.put(sqlId, serviceInventory);
            }
        }

        if (logger.isDebugEnabled()) {
            if (Objects.isNull(serviceInventory)) {
                logger.debug("service id {} not find in cache.", sqlId);
            }
        }

        return serviceInventory;
    }
}
