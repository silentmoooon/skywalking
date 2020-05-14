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

package org.apache.skywalking.apm.plugin.activemq;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.skywalking.apm.agent.core.context.CarrierItem;
import org.apache.skywalking.apm.agent.core.context.ContextCarrier;
import org.apache.skywalking.apm.agent.core.context.ContextManager;
import org.apache.skywalking.apm.agent.core.context.tag.Tags;
import org.apache.skywalking.apm.agent.core.context.trace.AbstractSpan;
import org.apache.skywalking.apm.agent.core.context.trace.SpanLayer;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.EnhancedInstance;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.InstanceMethodsAroundInterceptor;
import org.apache.skywalking.apm.agent.core.plugin.interceptor.enhance.MethodInterceptResult;
import org.apache.skywalking.apm.network.trace.component.ComponentsDefine;

import java.lang.reflect.Method;

/**
 * @author withlin
 */
public class ActiveMQAnnotationInterceptor implements InstanceMethodsAroundInterceptor {

    public static final String OPERATE_NAME_PREFIX = "ActiveMQ/";
    public static final String CONSUMER_OPERATE_NAME_SUFFIX = "/Consumer";
    public static final byte QUEUE_TYPE = 1;
    public static final byte TOPIC_TYPE = 2;
    public static final byte TEMP_TOPIC_TYPE = 6;
    public static final byte TEMP_QUEUE_TYPE = 5;

    @Override
    public void beforeMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
        MethodInterceptResult result) throws Throwable {
        ContextCarrier contextCarrier = new ContextCarrier();
        ActiveMQMessage message = (ActiveMQMessage)allArguments[0];
        ActiveMQSession session=(ActiveMQSession)allArguments[1];
        ActiveMQDestination jmsDestination = message.getDestination();
        String url = session.getConnection().getTransport().getRemoteAddress().split("//")[1];
        if(jmsDestination.getPhysicalName().contains("-core-core-default")){
            contextCarrier=null;
        }
        if(contextCarrier!=null) {
            CarrierItem next = contextCarrier.items();
            while (next.hasNext()) {
                next = next.next();
                String propertyValue = message.getStringProperty(next.getHeadKey());
                if (propertyValue != null) {
                    next.setHeadValue(propertyValue);
                }
            }
        }

        AbstractSpan activeSpan = null;
        if (jmsDestination.getDestinationType() == QUEUE_TYPE || jmsDestination.getDestinationType() == TEMP_QUEUE_TYPE) {
            activeSpan = ContextManager.createEntrySpan(OPERATE_NAME_PREFIX + "Queue/" + jmsDestination.getPhysicalName() + CONSUMER_OPERATE_NAME_SUFFIX, contextCarrier);
            Tags.MQ_BROKER.set(activeSpan, url);
            Tags.MQ_QUEUE.set(activeSpan, jmsDestination.getPhysicalName());
        } else if (jmsDestination.getDestinationType() == TOPIC_TYPE || jmsDestination.getDestinationType() == TEMP_TOPIC_TYPE) {
            activeSpan = ContextManager.createEntrySpan(OPERATE_NAME_PREFIX + "Topic/" + jmsDestination.getPhysicalName() + CONSUMER_OPERATE_NAME_SUFFIX, contextCarrier);
            Tags.MQ_BROKER.set(activeSpan, url);
            Tags.MQ_TOPIC.set(activeSpan, jmsDestination.getPhysicalName());
        }
        activeSpan.setComponent(ComponentsDefine.ACTIVEMQ_CONSUMER);
        SpanLayer.asMQ(activeSpan);


    }

    @Override
    public Object afterMethod(EnhancedInstance objInst, Method method, Object[] allArguments, Class<?>[] argumentsTypes,
        Object ret) throws Throwable {
        ContextManager.stopSpan();
        return ret;

    }

    @Override
    public void handleMethodException(EnhancedInstance objInst, Method method, Object[] allArguments,
        Class<?>[] argumentsTypes, Throwable t) {
        ContextManager.activeSpan().errorOccurred().log(t);
    }
}
