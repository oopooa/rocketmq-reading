package org.apache.rocketmq.store;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.CommitLog.MessageExtEncoder;

/**
 * not-thread-safe
 */
public class MultiDispatch {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final StringBuilder keyBuilder = new StringBuilder();
    private final DefaultMessageStore messageStore;
    private final CommitLog commitLog;

    public MultiDispatch(DefaultMessageStore messageStore, CommitLog commitLog) {
        this.messageStore = messageStore;
        this.commitLog = commitLog;
    }

    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        keyBuilder.setLength(0);
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = msgInner.getQueueId();
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
            queueId = 0;
        }
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    public boolean wrapMultiDispatch(final MessageExtBrokerInner msgInner) {
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return true;
        }
        String multiDispatchQueue = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return true;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        Long[] queueOffsets = new Long[queues.length];
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msgInner);
            Long queueOffset;
            try {
                queueOffset = getTopicQueueOffset(key);
            } catch (Exception e) {
                return false;
            }
            if (null == queueOffset) {
                queueOffset = 0L;
                if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                    commitLog.getLmqTopicQueueTable().put(key, queueOffset);
                } else {
                    commitLog.getTopicQueueTable().put(key, queueOffset);
                }
            }
            queueOffsets[i] = queueOffset;
        }
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                StringUtils.join(queueOffsets, MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        removeWaitStorePropertyString(msgInner);
        return rebuildMsgInner(msgInner);
    }

    private void removeWaitStorePropertyString(MessageExtBrokerInner msgInner) {
        if (msgInner.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            String waitStoreMsgOKValue = msgInner.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            msgInner.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        }
    }

    private boolean rebuildMsgInner(MessageExtBrokerInner msgInner) {
        MessageExtEncoder encoder = this.commitLog.getPutMessageThreadLocal().get().getEncoder();
        PutMessageResult encodeResult = encoder.encode(msgInner);
        if(encodeResult != null) {
           log.error("rebuild msgInner for multiDispatch", encodeResult);
           return false;
        }
        msgInner.setEncodedBuff(encoder.getEncoderBuffer());
        return true;

    }

    public void updateMultiQueueOffset(final MessageExtBrokerInner msgInner) {
        if (!messageStore.getMessageStoreConfig().isEnableMultiDispatch()) {
            return;
        }
        String multiDispatchQueue = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String multiQueueOffset = msgInner.getProperty(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiQueueOffset)) {
            log.error("[bug] no multiQueueOffset when updating {}", msgInner.getTopic());
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            log.error("[bug] num is not equal when updateMultiQueueOffset {}", msgInner.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msgInner);
            long queueOffset = Long.parseLong(queueOffsets[i]);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                commitLog.getLmqTopicQueueTable().put(key, ++queueOffset);
            } else {
                commitLog.getTopicQueueTable().put(key, ++queueOffset);
            }
        }
    }

    private Long getTopicQueueOffset(String key) throws Exception {
        Long offset = null;
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
            Long queueNextOffset = commitLog.getLmqTopicQueueTable().get(key);
            if (queueNextOffset != null) {
                offset = queueNextOffset;
            }
        } else {
            offset = commitLog.getTopicQueueTable().get(key);
        }
        return offset;
    }

}
