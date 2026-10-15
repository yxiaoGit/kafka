/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.config;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorConfigs;
import org.apache.kafka.coordinator.share.ShareCoordinatorConfig;
import org.apache.kafka.coordinator.transaction.AddPartitionsToTxnConfig;
import org.apache.kafka.coordinator.transaction.TransactionLogConfig;
import org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.storage.internals.log.CleanerConfig;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Locks the broker-config importance labels from KAFKA-4094.
 * HIGH is reserved for settings a deployment has to choose. MEDIUM is a usable default an operator may tune.
 * LOW is a knob that should normally be left alone, including deprecated settings.
 */
@SuppressWarnings("removal")
public class BrokerConfigImportanceTest {
    private static final Map<String, ConfigDef.Importance> EXPECTED = expected();

    @Test
    public void testImportanceLabels() {
        ConfigDef def = AbstractKafkaConfig.CONFIG_DEF;
        EXPECTED.forEach((name, importance) -> {
            ConfigDef.ConfigKey key = def.configKeys().get(name);
            assertNotNull(key, name);
            assertEquals(importance, key.importance, name);
        });
    }

    private static Map<String, ConfigDef.Importance> expected() {
        Map<String, ConfigDef.Importance> values = new HashMap<>();
        add(values, ConfigDef.Importance.LOW, low());
        add(values, ConfigDef.Importance.MEDIUM, medium());
        add(values, ConfigDef.Importance.HIGH, high());
        return Map.copyOf(values);
    }

    private static void add(Map<String, ConfigDef.Importance> values, ConfigDef.Importance importance, String[] names) {
        for (String name : names) {
            ConfigDef.Importance previous = values.put(name, importance);
            if (previous != null) {
                throw new IllegalStateException("Duplicate importance expectation for " + name);
            }
        }
    }

    private static String[] high() {
        return new String[] {
            KRaftConfigs.NODE_ID_CONFIG,
            KRaftConfigs.PROCESS_ROLES_CONFIG,
            KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG,
            QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG,
            SocketServerConfigs.LISTENERS_CONFIG,
            SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
            ServerLogConfigs.LOG_DIR_CONFIG,
            ServerLogConfigs.LOG_DIRS_CONFIG,
            GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG,
            GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG,
            TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG,
            TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG,
            TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG,
            ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_CONFIG,
            ShareCoordinatorConfig.STATE_TOPIC_NUM_PARTITIONS_CONFIG,
            ShareCoordinatorConfig.STATE_TOPIC_MIN_ISR_CONFIG,
            ClusterMirrorConfigs.MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG,
            ClusterMirrorConfigs.MIRROR_STATE_TOPIC_NUM_PARTITIONS_CONFIG
        };
    }

    private static String[] low() {
        return new String[] {
            ServerConfigs.BROKER_ID_CONFIG,
            ServerConfigs.EARLY_START_LISTENERS_CONFIG,
            ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG,
            ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG,
            GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG,
            CleanerConfig.LOG_CLEANER_ENABLE_PROP,
            RemoteLogManagerConfig.REMOTE_LOG_MANAGER_THREAD_POOL_SIZE_PROP,
            ReplicationConfigs.REPLICA_HIGH_WATERMARK_CHECKPOINT_INTERVAL_MS_CONFIG,
            ReplicationConfigs.LEADER_IMBALANCE_CHECK_INTERVAL_SECONDS_CONFIG,
            ServerLogConfigs.LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG,
            ServerLogConfigs.LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG,
            GroupCoordinatorConfig.OFFSETS_LOAD_BUFFER_SIZE_CONFIG,
            GroupCoordinatorConfig.OFFSETS_TOPIC_COMPRESSION_CODEC_CONFIG,
            GroupCoordinatorConfig.OFFSETS_RETENTION_CHECK_INTERVAL_MS_CONFIG,
            TransactionLogConfig.TRANSACTIONS_LOAD_BUFFER_SIZE_CONFIG,
            AddPartitionsToTxnConfig.ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MAX_MS_CONFIG,
            AddPartitionsToTxnConfig.ADD_PARTITIONS_TO_TXN_RETRY_BACKOFF_MS_CONFIG,
            ShareCoordinatorConfig.LOAD_BUFFER_SIZE_CONFIG,
            ShareCoordinatorConfig.STATE_TOPIC_COMPRESSION_CODEC_CONFIG
        };
    }

    private static String[] medium() {
        return new String[] {
            ServerConfigs.MESSAGE_MAX_BYTES_CONFIG,
            ServerConfigs.NUM_IO_THREADS_CONFIG,
            ServerConfigs.NUM_REPLICA_ALTER_LOG_DIRS_THREADS_CONFIG,
            ServerConfigs.BACKGROUND_THREADS_CONFIG,
            ServerConfigs.REQUEST_TIMEOUT_MS_CONFIG,
            ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG,
            ServerConfigs.COMPRESSION_TYPE_CONFIG,
            SocketServerConfigs.SOCKET_SEND_BUFFER_BYTES_CONFIG,
            SocketServerConfigs.SOCKET_RECEIVE_BUFFER_BYTES_CONFIG,
            SocketServerConfigs.SOCKET_REQUEST_MAX_BYTES_CONFIG,
            SocketServerConfigs.QUEUED_MAX_REQUESTS_CONFIG,
            SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG,
            ReplicationConfigs.REPLICA_LAG_TIME_MAX_MS_CONFIG,
            ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG,
            ReplicationConfigs.REPLICA_SOCKET_RECEIVE_BUFFER_BYTES_CONFIG,
            ReplicationConfigs.REPLICA_FETCH_WAIT_MAX_MS_CONFIG,
            ReplicationConfigs.REPLICA_FETCH_MIN_BYTES_CONFIG,
            ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG,
            ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG,
            ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
            ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG,
            ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG,
            ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG,
            ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG,
            ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG,
            ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG,
            ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG,
            ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG,
            ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG,
            ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG,
            ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG,
            ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG,
            ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG,
            ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG,
            ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG,
            ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG,
            KRaftConfigs.SASL_MECHANISM_CONTROLLER_PROTOCOL_CONFIG,
            QuorumConfig.QUORUM_VOTERS_CONFIG,
            QuorumConfig.QUORUM_ELECTION_TIMEOUT_MS_CONFIG,
            QuorumConfig.QUORUM_FETCH_TIMEOUT_MS_CONFIG,
            QuorumConfig.QUORUM_ELECTION_BACKOFF_MAX_MS_CONFIG,
            MetadataLogConfig.METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG,
            MetadataLogConfig.METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG,
            MetadataLogConfig.METADATA_LOG_DIR_CONFIG,
            MetadataLogConfig.METADATA_LOG_SEGMENT_BYTES_CONFIG,
            MetadataLogConfig.METADATA_LOG_SEGMENT_MILLIS_CONFIG,
            MetadataLogConfig.METADATA_MAX_RETENTION_BYTES_CONFIG,
            MetadataLogConfig.METADATA_MAX_RETENTION_MILLIS_CONFIG,
            GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_THREADS_CONFIG,
            GroupCoordinatorConfig.GROUP_COORDINATOR_NUM_BACKGROUND_THREADS_CONFIG,
            GroupCoordinatorConfig.OFFSET_COMMIT_TIMEOUT_MS_CONFIG,
            GroupCoordinatorConfig.OFFSETS_TOPIC_SEGMENT_BYTES_CONFIG,
            GroupCoordinatorConfig.OFFSET_METADATA_MAX_SIZE_CONFIG,
            GroupCoordinatorConfig.OFFSETS_RETENTION_MINUTES_CONFIG,
            TransactionLogConfig.TRANSACTIONS_TOPIC_SEGMENT_BYTES_CONFIG,
            TransactionStateManagerConfig.TRANSACTIONAL_ID_EXPIRATION_MS_CONFIG,
            TransactionStateManagerConfig.TRANSACTIONS_MAX_TIMEOUT_MS_CONFIG,
            ShareCoordinatorConfig.STATE_TOPIC_SEGMENT_BYTES_CONFIG,
            ShareCoordinatorConfig.WRITE_TIMEOUT_MS_CONFIG,
            ClusterMirrorConfigs.MIRROR_NUM_REPLICA_FETCHERS_CONFIG
        };
    }
}
