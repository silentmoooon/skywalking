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

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.cache;

import org.apache.skywalking.oap.server.core.Const;
import org.apache.skywalking.oap.server.core.register.DatabaseAccessInventory;
import org.apache.skywalking.oap.server.core.register.RegisterSource;
import org.apache.skywalking.oap.server.core.storage.cache.IDatabaseAccessInventoryCacheDAO;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base.EsDAO;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author peng-yongsheng
 */
public class DataAccessInventoryCacheEsDAO extends EsDAO implements IDatabaseAccessInventoryCacheDAO {

    private static final Logger logger = LoggerFactory.getLogger(DataAccessInventoryCacheEsDAO.class);

    protected final DatabaseAccessInventory.Builder builder = new DatabaseAccessInventory.Builder();
    protected final int resultWindowMaxSize;
    public DataAccessInventoryCacheEsDAO(ElasticSearchClient client,int resultWindowMaxSize) {
        super(client);
        this.resultWindowMaxSize = resultWindowMaxSize;
    }


    @Override
    public int getSqlId(int serviceId, int endpointId, String name, String sql) {
        try {
            String id = DatabaseAccessInventory.buildId(serviceId,endpointId,name,sql);
            GetResponse response = getClient().get(DatabaseAccessInventory.INDEX_NAME, id);
            if (response.isExists()) {
                return (int)response.getSource().getOrDefault(RegisterSource.SEQUENCE, 0);
            } else {
                return Const.NONE;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            return Const.NONE;
        }

    }

    @Override public DatabaseAccessInventory get(int sqlId) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery(DatabaseAccessInventory.SEQUENCE, sqlId));
            searchSourceBuilder.size(1);

            SearchResponse response = getClient().search(DatabaseAccessInventory.INDEX_NAME, searchSourceBuilder);
            if (response.getHits().totalHits == 1) {
                SearchHit searchHit = response.getHits().getAt(0);
                return builder.map2Data(searchHit.getSourceAsMap());
            } else {
                return null;
            }
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public List<DatabaseAccessInventory> loadLastUpdate(long lastUpdateTime) {
        List<DatabaseAccessInventory> addressInventories = new ArrayList<>();

        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.rangeQuery(DatabaseAccessInventory.LAST_UPDATE_TIME).gte(lastUpdateTime));
            searchSourceBuilder.size(resultWindowMaxSize);

            SearchResponse response = getClient().search(DatabaseAccessInventory.INDEX_NAME, searchSourceBuilder);

            for (SearchHit searchHit : response.getHits().getHits()) {
                addressInventories.add(this.builder.map2Data(searchHit.getSourceAsMap()));
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }

        return addressInventories;
    }
}
