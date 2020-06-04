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

package org.apache.skywalking.oap.server.core.register;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.analysis.Stream;
import org.apache.skywalking.oap.server.core.register.worker.InventoryStreamProcessor;
import org.apache.skywalking.oap.server.core.remote.grpc.proto.RemoteData;
import org.apache.skywalking.oap.server.core.source.ScopeDeclaration;
import org.apache.skywalking.oap.server.core.storage.StorageBuilder;
import org.apache.skywalking.oap.server.core.storage.annotation.Column;

import java.util.HashMap;
import java.util.Map;

import static org.apache.skywalking.oap.server.core.source.DefaultScopeDefine.DATABASE_ACCESS_INVENTORY;

/**
 * @author peng-yongsheng
 */
@ScopeDeclaration(id = DATABASE_ACCESS_INVENTORY, name = "DatabaseAccessInventory")
@Stream(name = DatabaseAccessInventory.INDEX_NAME, scopeId = DATABASE_ACCESS_INVENTORY, builder = DatabaseAccessInventory.Builder.class, processor = InventoryStreamProcessor.class)
public class DatabaseAccessInventory extends RegisterSource {

    public static final String INDEX_NAME = "database_access_inventory";
    public static final String SERVICE_ID = "service_id";
    public static final String ENDPOINT_ID = "endpoint_id";
    public static final String NAME = "name";
    public static final String SQL="sql";

    @Setter @Getter @Column(columnName = SERVICE_ID) private int serviceId;
    @Setter @Getter @Column(columnName = ENDPOINT_ID) private int endpointId;
    @Setter @Getter @Column(columnName = NAME, matchQuery = true) private String name = Const.EMPTY_STRING;
    @Setter @Getter @Column(columnName = SQL) private String sql=Const.EMPTY_STRING;


    public static String buildId(int serviceId,int endPointId,String name,String sql) {
        return serviceId+Const.ID_SPLIT+endPointId + Const.ID_SPLIT + name + Const.ID_SPLIT + sql;
    }


    @Override public String id() {
//Base64.encodeBase64String(sql.getBytes(StandardCharsets.UTF_8)).substring(0,10)
        String baseSql;
        if (sql.length() <= 64) {
            baseSql = sql;
        } else {
            baseSql = sql.substring(0, 63);
        }
            return buildId(serviceId,endpointId,name,baseSql );

    }

    @Override public int hashCode() {
        int result = 17;
        result = 31 * result + serviceId;
        result = 31 * result + endpointId;
        result = 31 * result + name.hashCode();
        result = 31 * result + sql.hashCode();
        return result;
    }



    public DatabaseAccessInventory getClone() {
        DatabaseAccessInventory inventory = new DatabaseAccessInventory();
        inventory.setSequence(getSequence());
        inventory.setRegisterTime(getRegisterTime());
        inventory.setHeartbeatTime(getHeartbeatTime());
        inventory.setServiceId(serviceId);
        inventory.setEndpointId(endpointId);
        inventory.setName(name);
        inventory.setSql(sql);
        inventory.setLastUpdateTime(getLastUpdateTime());

        return inventory;
    }

    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        DatabaseAccessInventory source = (DatabaseAccessInventory)obj;
        if(serviceId!=source.getServiceId())
            return false;
        if(endpointId!=source.getEndpointId())
            return false;
        if (!name.equals(source.getName()))
            return false;
        if(!sql.equals(source.getSql()))
            return false;

        return true;
    }

    @Override public RemoteData.Builder serialize() {
        RemoteData.Builder remoteBuilder = RemoteData.newBuilder();
        remoteBuilder.addDataIntegers(getSequence());
        remoteBuilder.addDataIntegers(serviceId);
        remoteBuilder.addDataIntegers(endpointId);

        remoteBuilder.addDataLongs(getRegisterTime());
        remoteBuilder.addDataLongs(getHeartbeatTime());
        remoteBuilder.addDataLongs(getLastUpdateTime());

        remoteBuilder.addDataStrings(Strings.isNullOrEmpty(name) ? Const.EMPTY_STRING : name);
        remoteBuilder.addDataStrings(Strings.isNullOrEmpty(sql) ? Const.EMPTY_STRING : sql);
        return remoteBuilder;
    }

    @Override public void deserialize(RemoteData remoteData) {
        setSequence(remoteData.getDataIntegers(0));
        setServiceId(remoteData.getDataIntegers(1));
        setEndpointId(remoteData.getDataIntegers(2));
        setRegisterTime(remoteData.getDataLongs(0));
        setHeartbeatTime(remoteData.getDataLongs(1));
        setLastUpdateTime(remoteData.getDataLongs(2));

        setName(remoteData.getDataStrings(0));
        setSql(remoteData.getDataStrings(1));

    }

    @Override public int remoteHashCode() {
        return 0;
    }

    @Override public boolean combine(RegisterSource registerSource) {
        boolean isChanged = super.combine(registerSource);
        DatabaseAccessInventory serviceInventory = (DatabaseAccessInventory)registerSource;

        if (serviceInventory.getLastUpdateTime() >= this.getLastUpdateTime()) {
            this.serviceId=serviceInventory.getServiceId();
            this.endpointId=serviceInventory.getEndpointId();
            this.sql=serviceInventory.getSql();
            isChanged = true;
        }

        return isChanged;
    }



    public static class Builder implements StorageBuilder<DatabaseAccessInventory> {

        @Override public DatabaseAccessInventory map2Data(Map<String, Object> dbMap) {
            DatabaseAccessInventory inventory = new DatabaseAccessInventory();
            inventory.setSequence(((Number)dbMap.get(SEQUENCE)).intValue());
            inventory.setServiceId(((Number)dbMap.get(SERVICE_ID)).intValue());
            inventory.setEndpointId(((Number)dbMap.get(ENDPOINT_ID)).intValue());
            inventory.setName((String)dbMap.get(NAME));
            inventory.setSql((String)dbMap.get(SQL));
            inventory.setRegisterTime(((Number)dbMap.get(REGISTER_TIME)).longValue());
            inventory.setHeartbeatTime(((Number)dbMap.get(HEARTBEAT_TIME)).longValue());
            inventory.setLastUpdateTime(((Number)dbMap.get(LAST_UPDATE_TIME)).longValue());
            return inventory;
        }

        @Override public Map<String, Object> data2Map(DatabaseAccessInventory storageData) {
            Map<String, Object> map = new HashMap<>();
            map.put(SEQUENCE, storageData.getSequence());
            map.put(SERVICE_ID, storageData.getServiceId());
            map.put(ENDPOINT_ID, storageData.getEndpointId());
            map.put(NAME, storageData.getName());
            map.put(SQL, storageData.getSql());
            map.put(REGISTER_TIME, storageData.getRegisterTime());
            map.put(HEARTBEAT_TIME, storageData.getHeartbeatTime());
            map.put(LAST_UPDATE_TIME, storageData.getLastUpdateTime());
            return map;
        }
    }
}
