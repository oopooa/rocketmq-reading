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
package org.apache.rocketmq.store;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.queue.ConsumeQueueStoreInterface;
import org.apache.rocketmq.store.queue.RocksDBConsumeQueueStore;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class RocksDBMessageStore extends DefaultMessageStore {

    public RocksDBMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
        final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig, final ConcurrentMap<String, TopicConfig> topicConfigTable) throws
        IOException {
        super(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig, topicConfigTable);
        isNecessary2NotifyMessageArrive = false;
    }

    @Override
    public ConsumeQueueStoreInterface createConsumeQueueStore() {
        return new RocksDBConsumeQueueStore(this);
    }

    @Override
    public CleanConsumeQueueService createCleanConsumeQueueService() {
        return new RocksDBCleanConsumeQueueService();
    }

    @Override
    public FlushConsumeQueueService createFlushConsumeQueueService() {
        return new RocksDBFlushConsumeQueueService();
    }

    @Override
    public CorrectLogicOffsetService createCorrectLogicOffsetService() {
        return new RocksDBCorrectLogicOffsetService();
    }

    @Override
    public void recoverTopicQueueTable() {
        // try to set topicQueueTable = new HashMap<>(), otherwise it will cause bug when broker role changes
        // unlike method in DefaultMessageStore, we don't need to really recover topic queue table,
        // because we can recover topic queue table from rocksdb (you can see method assignQueueOffset in RocksDBConsumeQueue)
        this.consumeQueueStore.setTopicQueueTable(new ConcurrentHashMap<>());
    }

    @Override
    public boolean loadLogics() {
        return this.consumeQueueStore.load();
    }

    @Override
    public void finishCommitLogDispatch() {
        try {
            putMessagePositionInfo(null);
        } catch (Exception e) {
            ERROR_LOG.info("try to finish commitlog dispatch error.", e);
        }
    }

    class RocksDBCleanConsumeQueueService extends CleanConsumeQueueService {
        private final double diskSpaceWarningLevelRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        private final double diskSpaceCleanForciblyRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));

        @Override
        protected void deleteExpiredFiles() throws Exception {

            long minOffset = RocksDBMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                boolean spaceFull = isSpaceToDelete();
                boolean timeUp = cleanCommitLogService.isTimeToDelete();
                if (spaceFull || timeUp) {
                    RocksDBMessageStore.this.consumeQueueStore.cleanExpired(minOffset);
                }

                RocksDBMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        private boolean isSpaceToDelete() {
            double ratio = RocksDBMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            String storePathLogics = StorePathConfigHelper
                .getStorePathConsumeQueue(RocksDBMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            if (logicsRatio > diskSpaceWarningLevelRatio) {
                boolean diskOk = RocksDBMessageStore.this.runningFlags.getAndMakeLogicDiskFull();
                if (diskOk) {
                    RocksDBMessageStore.LOGGER.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                }
            } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
            } else {
                boolean diskok = RocksDBMessageStore.this.runningFlags.getAndMakeLogicDiskOK();
                if (!diskok) {
                    RocksDBMessageStore.LOGGER.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                }
            }

            if (logicsRatio < 0 || logicsRatio > ratio) {
                RocksDBMessageStore.LOGGER.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                return true;
            }

            return false;
        }
    }

    class RocksDBFlushConsumeQueueService extends FlushConsumeQueueService {
        @Override
        public void run() {
            /**
             * There is no need to flush consume queue,
             * we put all consume queues in RocksDBConsumeQueueStore,
             * it depends on rocksdb to flush consume queue to disk(sorted string table),
             * we even don't flush WAL of consume store, since we think it can recover consume queue from commitlog
             */
        }
    }

    class RocksDBCorrectLogicOffsetService extends CorrectLogicOffsetService {
        public void run() {
            /**
             * There is no need to correct min offset of consume queue, we already fix this problem
             *  @see RocksDBConsumeQueueStore#getMinOffsetInQueue()
             */
        }
    }

    @Override
    public long estimateMessageCount(String topic, int queueId, long from, long to, MessageFilter filter) {
        // todo
        return 0;
    }
}
