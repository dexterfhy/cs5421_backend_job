import { config } from "dotenv";
import { ConsumerConfig, ConsumerSubscribeTopic, ITopicConfig, KafkaConfig, KafkaMessage, ProducerConfig } from "kafkajs";
import { scheduleJob } from "node-schedule";
import { cpus } from "os";
import { join } from "path";
import { 
    KAFKA_APPLICATION_NAME, KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC, KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC_NUM_PARTITIONS, 
    KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC_REPLICATION_FACTOR, KAFKA_JOB_ATTEMPT_FAST_CONSUMER_GROUP_ID, 
    KAFKA_JOB_ATTEMPT_FAST_CONSUMER_HEARTBEAT_INTERVAL, KAFKA_JOB_ATTEMPT_FAST_CONSUMER_MAX_BYTES, 
    KAFKA_JOB_ATTEMPT_FAST_CONSUMER_MAX_BYTES_PER_PARTITION, KAFKA_JOB_ATTEMPT_FAST_CONSUMER_SESSION_TIMEOUT, 
    KAFKA_JOB_ATTEMPT_FAST_TOPIC, KAFKA_JOB_ATTEMPT_FAST_TOPIC_NUM_PARTITIONS, 
    KAFKA_JOB_ATTEMPT_FAST_TOPIC_REPLICATION_FACTOR, KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_GROUP_ID, 
    KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_HEARTBEAT_INTERVAL, KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_MAX_BYTES, 
    KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_MAX_BYTES_PER_PARTITION, KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_SESSION_TIMEOUT, 
    KAFKA_JOB_ATTEMPT_SLOW_TOPIC, KAFKA_JOB_ATTEMPT_SLOW_TOPIC_NUM_PARTITIONS, 
    KAFKA_JOB_ATTEMPT_SLOW_TOPIC_REPLICATION_FACTOR, KAFKA_JOB_INIT_COMPLETION_TOPIC, 
    KAFKA_JOB_INIT_COMPLETION_TOPIC_NUM_PARTITIONS, KAFKA_JOB_INIT_COMPLETION_TOPIC_REPLICATION_FACTOR, 
    KAFKA_JOB_INIT_CONSUMER_GROUP_ID, KAFKA_JOB_INIT_CONSUMER_HEARTBEAT_INTERVAL, KAFKA_JOB_INIT_CONSUMER_MAX_BYTES, 
    KAFKA_JOB_INIT_CONSUMER_MAX_BYTES_PER_PARTITION, KAFKA_JOB_INIT_CONSUMER_SESSION_TIMEOUT, 
    KAFKA_JOB_INIT_TOPIC, KAFKA_JOB_INIT_TOPIC_NUM_PARTITIONS, KAFKA_JOB_INIT_TOPIC_REPLICATION_FACTOR 
} from "./constants/kafka";
import { 
    PostgreSQLQueryType, POSTGRESQL_ADMIN_QUERY_TIMEOUT, POSTGRESQL_ADMIN_STATEMENT_TIMEOUT, 
    POSTGRESQL_DAILY_CLEANUP_TIME, POSTGRESQL_FAST_QUERY_TIMEOUT, POSTGRESQL_FAST_STATEMENT_TIMEOUT, 
    POSTGRESQL_SLOW_QUERY_TIMEOUT, POSTGRESQL_SLOW_STATEMENT_TIMEOUT 
} from "./constants/postgreSQL";
import { kafkaJobAttemptEventHandler, kafkaJobInitEventHandler } from "./job-handlers";
import { KafkaClientService } from './services/kafka-client-service';
import { PostgreSQLAdapter } from "./services/postgreSQL-adapter";

(async () => {
    const pathsToEnvironmentVariables: string[] = 
        [join(__dirname, "configs/.env.database"), join(__dirname, "configs/.env.kafka")];
    pathsToEnvironmentVariables
        .map(path => config({path}))
        .forEach(loadResult => {
            if (loadResult.error) throw loadResult.error
        });
    console.log("[Job Server] Environment has been initialized successfully");

    const postgreSQLAdapter: PostgreSQLAdapter = PostgreSQLAdapter.initialize({
        host: process.env.DB_HOST,
        port: parseInt(process.env.DB_PORT!),
        database: process.env.DB_NAME,
        adminUsername: process.env.DB_ADMIN_USERNAME,
        adminPassword: process.env.DB_ADMIN_PASSWORD,
        ordinaryUsername: process.env.DB_ORDINARY_USERNAME,
        ordinaryPassword: process.env.DB_ORDINARY_PASSWORD,
        adminStatementTimeout: POSTGRESQL_ADMIN_STATEMENT_TIMEOUT,
        adminQueryTimeout: POSTGRESQL_ADMIN_QUERY_TIMEOUT,
        fastStatementTimeout: POSTGRESQL_FAST_STATEMENT_TIMEOUT,
        fastQueryTimeout: POSTGRESQL_FAST_QUERY_TIMEOUT,
        slowStatementTimeout: POSTGRESQL_SLOW_STATEMENT_TIMEOUT,
        slowQueryTimeout: POSTGRESQL_SLOW_QUERY_TIMEOUT,
        adminQueryPoolMaxConnections: 2,
        fastQueryPoolMaxConnections: parseInt(process.env.DB_HOST_NUM_CPUS!) / 2,
        slowQueryPoolMaxConnections: parseInt(process.env.DB_HOST_NUM_CPUS!) / 2,
        applicationName: process.env.DB_APPLICATION_NAME
    });
    registerEventHandlers(
        async (code) => {
            console.log(`[Job Server] PostgreSQLAdapter instance cleans up allocated resources before Job Server terminates with an exit code ${code}`);
            await PostgreSQLAdapter.terminate();
        },
        async () => {
            console.log(`[Job Server] PostgreSQLAdapter instance cleans up allocated resources before Job Server is terminated by a signal with an exit code ${process.exitCode}`);
            await PostgreSQLAdapter.terminate();
            process.exit();
        }
    );
    console.log("[Job Server] PostgreSQLAdapter singleton instance has been created successfully");
    const { rows }: { rows: boolean[][] } = (await postgreSQLAdapter.query({
        text: "SELECT EXISTS (SELECT * FROM pg_tables WHERE schemaname = 'public' AND tablename = $1)",
        values: [process.env.DB_CHALLENGE_TABLE_NAME!],
        rowMode: 'array'
    }, PostgreSQLQueryType.ADMIN_QUERY))!;
    console.log(`[Job Server] rows: ${JSON.stringify(rows)}`);
    if (!rows[0][0]) {
        await postgreSQLAdapter.schematizedQuery(
            "public", 
            {
                text: 
                    `CREATE TABLE ${process.env.DB_CHALLENGE_TABLE_NAME!} (
                        challenge_id    INTEGER PRIMARY KEY,
                        challenge_name  VARCHAR(100),
                        expires_at      TIMESTAMP WITH TIME ZONE,
                        init            VARCHAR(10000),
                        test_cases      JSON,
                        solution        VARCHAR(10000),
                        times_to_run    INTEGER
                    )`
            }, 
            PostgreSQLQueryType.ADMIN_QUERY
        );
        console.log(`[Job Server] The table ${process.env.DB_CHALLENGE_TABLE_NAME!} has been successfully created`);
    }

    const kafkaConfig: KafkaConfig = {
        clientId: KAFKA_APPLICATION_NAME,
        brokers: [`${process.env.KAFKA_HOST}:${process.env.KAFKA_PORT}`]
    };
    const topicConfigs: ITopicConfig[] = [
        {
            topic: KAFKA_JOB_INIT_TOPIC,
            numPartitions: KAFKA_JOB_INIT_TOPIC_NUM_PARTITIONS,
            replicationFactor: KAFKA_JOB_INIT_TOPIC_REPLICATION_FACTOR
        },
        {
            topic: KAFKA_JOB_INIT_COMPLETION_TOPIC,
            numPartitions: KAFKA_JOB_INIT_COMPLETION_TOPIC_NUM_PARTITIONS,
            replicationFactor: KAFKA_JOB_INIT_COMPLETION_TOPIC_REPLICATION_FACTOR
        },
        {
            topic: KAFKA_JOB_ATTEMPT_FAST_TOPIC,
            numPartitions: KAFKA_JOB_ATTEMPT_FAST_TOPIC_NUM_PARTITIONS,
            replicationFactor: KAFKA_JOB_ATTEMPT_FAST_TOPIC_REPLICATION_FACTOR
        },
        {
            topic: KAFKA_JOB_ATTEMPT_SLOW_TOPIC,
            numPartitions: KAFKA_JOB_ATTEMPT_SLOW_TOPIC_NUM_PARTITIONS,
            replicationFactor: KAFKA_JOB_ATTEMPT_SLOW_TOPIC_REPLICATION_FACTOR
        },
        {
            topic: KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC,
            numPartitions: KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC_NUM_PARTITIONS,
            replicationFactor: KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC_REPLICATION_FACTOR
        }
    ];
    const producerConfigs: ProducerConfig[] = [
        {allowAutoTopicCreation: false}, // For JobInitCompletionEvent use
        {allowAutoTopicCreation: false} // For JobAttemptCompletionEvent use
    ];
    const consumerConfigs: ConsumerConfig[] = [
        {
            groupId: KAFKA_JOB_INIT_CONSUMER_GROUP_ID,
            sessionTimeout: KAFKA_JOB_INIT_CONSUMER_SESSION_TIMEOUT,
            heartbeatInterval: KAFKA_JOB_INIT_CONSUMER_HEARTBEAT_INTERVAL,
            allowAutoTopicCreation: false,
            maxBytesPerPartition: KAFKA_JOB_INIT_CONSUMER_MAX_BYTES_PER_PARTITION,
            maxBytes: KAFKA_JOB_INIT_CONSUMER_MAX_BYTES
        },
        {
            groupId: KAFKA_JOB_ATTEMPT_FAST_CONSUMER_GROUP_ID,
            sessionTimeout: KAFKA_JOB_ATTEMPT_FAST_CONSUMER_SESSION_TIMEOUT,
            heartbeatInterval: KAFKA_JOB_ATTEMPT_FAST_CONSUMER_HEARTBEAT_INTERVAL,
            allowAutoTopicCreation: false,
            maxBytesPerPartition: KAFKA_JOB_ATTEMPT_FAST_CONSUMER_MAX_BYTES_PER_PARTITION,
            maxBytes: KAFKA_JOB_ATTEMPT_FAST_CONSUMER_MAX_BYTES
        },
        {
            groupId: KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_GROUP_ID,
            sessionTimeout: KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_SESSION_TIMEOUT,
            heartbeatInterval: KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_HEARTBEAT_INTERVAL,
            allowAutoTopicCreation: false,
            maxBytesPerPartition: KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_MAX_BYTES_PER_PARTITION,
            maxBytes: KAFKA_JOB_ATTEMPT_SLOW_CONSUMER_MAX_BYTES
        }
    ];
    const consumerSubscribeTopics: ConsumerSubscribeTopic[] = [
        {topic: KAFKA_JOB_INIT_TOPIC},
        {topic: KAFKA_JOB_ATTEMPT_FAST_TOPIC},
        {topic: KAFKA_JOB_ATTEMPT_SLOW_TOPIC}
    ];
    const kafkaClientService: KafkaClientService = await KafkaClientService.initialize(
        kafkaConfig,
        topicConfigs,
        producerConfigs,
        consumerConfigs,
        consumerSubscribeTopics
    );
    registerEventHandlers(
        async (code) => {
            console.log(`[Job Server] KafkaClientService instance cleans up allocated resources before Job Server terminates with an exit code ${code}`);
            await KafkaClientService.terminate();
        },
        async () => {
            console.log(`[Job Server] KafkaClientService instance cleans up allocated resources before Job Server is terminated by a signal with an exit code ${process.exitCode}`);
            await KafkaClientService.terminate();
            process.exit();
        }
    );
    console.log("[Job Server] KafkaClientService singleton instance has been created successfully");

    scheduleJob(POSTGRESQL_DAILY_CLEANUP_TIME, async () => {
        const expiredChallenges: { challenge_id: number, challenge_name: string, test_cases: string }[] = 
            (await postgreSQLAdapter.schematizedQuery(
                "public",
                { 
                    text: 
                        `SELECT challenge_id, challenge_name, test_cases FROM 
                        ${process.env.DB_CHALLENGE_TABLE_NAME!} WHERE expires_at < $1`,
                    values: [new Date()]
                },
                PostgreSQLQueryType.ADMIN_QUERY
            ))!.rows;
        for (const expiredChallenge of expiredChallenges) {
            const { challenge_id, challenge_name, test_cases }: 
                { challenge_id: number, challenge_name: string, test_cases: string } = expiredChallenge;
            const parsedTestCases: { id: number, data: string }[] = JSON.parse(test_cases);
            await postgreSQLAdapter.transaction(
                [
                    { text: "SET search_path TO public" },
                    ...parsedTestCases.map((testCase: { id: number, data: string }) => {
                        const schemaName: string = `${challenge_name}_${challenge_id}_${testCase.id}`;
                        return { text: `DROP SCHEMA IF EXISTS ${schemaName} CASCADE` };
                    }),
                    { 
                        text: `DELETE FROM ${process.env.DB_CHALLENGE_TABLE_NAME!} WHERE challenge_id = $1`,
                        values: [challenge_id]
                    }
                ],
                PostgreSQLQueryType.ADMIN_QUERY
            );
        }
    });

    kafkaClientService.consumerRun({
        partitionsConsumedConcurrently: 1,
        eachMessage: async ({ topic, message, heartbeat }) => {
            console.log(`[Job Server] Kafka Consumer received a message from the topic ${topic} with the content ${JSON.stringify(message)}`);
            if (topic == KAFKA_JOB_INIT_TOPIC) {
                await kafkaJobInitEventHandler(
                    postgreSQLAdapter, 
                    PostgreSQLQueryType.ADMIN_QUERY, 
                    kafkaClientService, 
                    0, 
                    message, 
                    heartbeat
                );
            }
        }
    }, 0);

    kafkaClientService.consumerRun({
        partitionsConsumedConcurrently: cpus().length,
        eachMessage: async ({ topic, message, heartbeat }) => {
            console.log(`[Job Server] Kafka Consumer received a message from the topic ${topic} with the content ${JSON.stringify(message)}`);
            if (topic == KAFKA_JOB_ATTEMPT_FAST_TOPIC) {
                await kafkaJobAttemptEventHandler(
                    postgreSQLAdapter,
                    PostgreSQLQueryType.FAST_QUERY,
                    kafkaClientService,
                    1,
                    message,
                    heartbeat
                );
            }
        }
    }, 1);

    kafkaClientService.consumerRun({
        partitionsConsumedConcurrently: cpus().length,
        eachMessage: async ({ topic, message, heartbeat }) => {
            console.log(`[Job Server] Kafka Consumer received a message from the topic ${topic} with the content ${JSON.stringify(message)}`);
            if (topic == KAFKA_JOB_ATTEMPT_SLOW_TOPIC) {
                await kafkaJobAttemptEventHandler(
                    postgreSQLAdapter,
                    PostgreSQLQueryType.SLOW_QUERY,
                    kafkaClientService,
                    1,
                    message,
                    heartbeat
                );
            }
        }
    }, 2);
})();
 
function registerEventHandlers(
    beforeExitHandler: (code: string) => Promise<void>, 
    signalHandler: () => Promise<void>
) {
    process.on("beforeExit", beforeExitHandler)
           .on("SIGINT", signalHandler)
           .on("SIGTERM", signalHandler);
}