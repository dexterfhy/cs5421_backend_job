import { cpus } from "os";

export const KAFKA_APPLICATION_NAME = "Ladder-Web-Service";

export const KAFKA_JOB_INIT_TOPIC = "job-init";
export const KAFKA_JOB_INIT_TOPIC_NUM_PARTITIONS = 1;
export const KAFKA_JOB_INIT_TOPIC_REPLICATION_FACTOR = 1;

export const KAFKA_JOB_INIT_COMPLETION_TOPIC = "job-init-completion";
export const KAFKA_JOB_INIT_COMPLETION_TOPIC_NUM_PARTITIONS = 1;
export const KAFKA_JOB_INIT_COMPLETION_TOPIC_REPLICATION_FACTOR = 1;

export const KAFKA_JOB_ATTEMPT_FAST_TOPIC = "job-attempt-fast";
export const KAFKA_JOB_ATTEMPT_FAST_TOPIC_NUM_PARTITIONS = cpus().length;
export const KAFKA_JOB_ATTEMPT_FAST_TOPIC_REPLICATION_FACTOR = 1;

export const KAFKA_JOB_ATTEMPT_SLOW_TOPIC = "job-attempt-slow";
export const KAFKA_JOB_ATTEMPT_SLOW_TOPIC_NUM_PARTITIONS = cpus().length;
export const KAFKA_JOB_ATTEMPT_SLOW_TOPIC_REPLICATION_FACTOR = 1;

export const KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC = "job-attempt-completion";
export const KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC_NUM_PARTITIONS = cpus().length;
export const KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC_REPLICATION_FACTOR = 1;

export const KAFKA_JOB_INIT_CONSUMER_GROUP_ID = "job-init-group";
export const KAFKA_JOB_INIT_CONSUMER_SESSION_TIMEOUT = 6000000;
export const KAFKA_JOB_INIT_CONSUMER_HEARTBEAT_INTERVAL = 3000000;
export const KAFKA_JOB_INIT_CONSUMER_MAX_BYTES_PER_PARTITION = 134217728;
export const KAFKA_JOB_INIT_CONSUMER_MAX_BYTES = 268435456;

export const KAFKA_JOB_ATTEMPT_FAST_CONSUMER_GROUP_ID = "job-attempt-fast-group";
export const KAFKA_JOB_ATTEMPT_FAST_CONSUMER_SESSION_TIMEOUT = 6000000;
export const KAFKA_JOB_ATTEMPT_FAST_CONSUMER_HEARTBEAT_INTERVAL = 3000000;
export const KAFKA_JOB_ATTEMPT_FAST_CONSUMER_MAX_BYTES_PER_PARTITION = 134217728;
export const KAFKA_JOB_ATTEMPT_FAST_CONSUMER_MAX_BYTES = 268435456;

export const KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_GROUP_ID = "job-attempt-slow-group";
export const KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_SESSION_TIMEOUT = 6000000;
export const KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_HEARTBEAT_INTERVAL = 3000000;
export const KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_MAX_BYTES_PER_PARTITION = 134217728;
export const KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_MAX_BYTES = 268435456;

export const KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_FAILED = "FAILED";
export const KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_COMPLETED = "COMPLETED";