package org.apache.skywalking.apm.plugin.activemq;

import org.apache.skywalking.apm.agent.core.context.tag.StringTag;

public   final class ActiveMQExtendTag {
        public static final StringTag TRANS_ID = new StringTag(11, "TransId");
    }