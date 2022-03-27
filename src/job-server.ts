import { config } from "dotenv";
import { ConsumerConfig, ConsumerSubscribeTopic, ITopicConfig, KafkaConfig, KafkaMessage, ProducerConfig } from "kafkajs";
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
    KAFKA_JOB_ATTEMPT_SLOW_TOPIC_REPLICATION_FACTOR, KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_COMPLETED, KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_FAILED, KAFKA_JOB_INIT_COMPLETION_TOPIC, 
    KAFKA_JOB_INIT_COMPLETION_TOPIC_NUM_PARTITIONS, KAFKA_JOB_INIT_COMPLETION_TOPIC_REPLICATION_FACTOR, 
    KAFKA_JOB_INIT_CONSUMER_GROUP_ID, KAFKA_JOB_INIT_CONSUMER_HEARTBEAT_INTERVAL, KAFKA_JOB_INIT_CONSUMER_MAX_BYTES, 
    KAFKA_JOB_INIT_CONSUMER_MAX_BYTES_PER_PARTITION, KAFKA_JOB_INIT_CONSUMER_SESSION_TIMEOUT, 
    KAFKA_JOB_INIT_TOPIC, KAFKA_JOB_INIT_TOPIC_NUM_PARTITIONS, KAFKA_JOB_INIT_TOPIC_REPLICATION_FACTOR 
} from "./constants/kafka";
import { 
    PostgreSQLQueryType, POSTGRESQL_ADMIN_QUERY_TIMEOUT, POSTGRESQL_ADMIN_STATEMENT_TIMEOUT, POSTGRESQL_FAST_QUERY_TIMEOUT, 
    POSTGRESQL_FAST_STATEMENT_TIMEOUT, POSTGRESQL_SLOW_QUERY_TIMEOUT, POSTGRESQL_SLOW_STATEMENT_TIMEOUT 
} from "./constants/postgreSQL";
// import { Pool, Client } from "pg"
// import {Worker, isMainThread, parentPort, workerData} from "worker_threads"
// import { PostgreSQLQueryType } from "./constants/postgreSQL";
// import { PostgreSQLAdapter } from "./services/postgreSQL-adapter";
// import { ConfigResourceTypes, Kafka } from "kafkajs";
import { KafkaClientService } from './services/kafka-client-service';
import { PostgreSQLAdapter } from "./services/postgreSQL-adapter";

interface JobInitEventValue {
    challenge_id: number,
    challenge_name?: string,
    expires_at: string,
    times_to_run?: number,
    init?: string,
    solution?: string,
    test_cases?: {
        id: number,
        data: string
    }[]
}

(async () => {
    const pathsToEnvironmentVariables: string[] = 
        [join(__dirname, "configs/.env.database"), join(__dirname, "configs/.env.kafka")];
    pathsToEnvironmentVariables
        .map(path => config({path}))
        .forEach(loadResult => {
            if (loadResult.error) throw loadResult.error
        });
    console.log("Environment has been initialized successfully");

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
        adminQueryPoolMaxConnections: 1,
        fastQueryPoolMaxConnections: parseInt(process.env.DB_HOST_NUM_CPUS!) / 2,
        slowQueryPoolMaxConnections: parseInt(process.env.DB_HOST_NUM_CPUS!) / 2,
        applicationName: process.env.DB_APPLICATION_NAME
    });
    registerEventHandlers(
        async (code) => {
            console.log(`PostgreSQLAdapter instance cleans up allocated resources before Job Server terminates with an exit code ${code}`);
            await PostgreSQLAdapter.terminate();
        },
        async () => {
            console.log(`PostgreSQLAdapter instance cleans up allocated resources before Job Server is terminated by a signal with an exit code ${process.exitCode}`);
            await PostgreSQLAdapter.terminate();
            process.exit();
        }
    );
    console.log("PostgreSQLAdapter singleton instance has been created successfully");

    const { rows }: { rows: boolean[][] } = (await postgreSQLAdapter.query({
        text: "SELECT EXISTS (SELECT * FROM pg_tables WHERE schemaname = 'public' AND tablename = $1)",
        values: [process.env.DB_CHALLENGE_TABLE_NAME!],
        rowMode: 'array'
    }, PostgreSQLQueryType.ADMIN_QUERY))!;
    if (!rows[0][0]) {
        await postgreSQLAdapter.query({
            text: 
                `CREATE TABLE public.${process.env.DB_CHALLENGE_TABLE_NAME!} (
                    challenge_id    INTEGER PRIMARY KEY,
                    challenge_name  VARCHAR(100),
                    expires_at      TIMESTAMP WITH TIME ZONE,
                    init            VARCHAR(10000),
                    test_cases      JSON,
                    solution        VARCHAR(10000),
                    times_to_run    INTEGER
                )`
        }, PostgreSQLQueryType.ADMIN_QUERY);
        console.log("Challenge table has been successfully created");
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
            console.log(`KafkaClientService instance cleans up allocated resources before Job Server terminates with an exit code ${code}`);
            await KafkaClientService.terminate();
        },
        async () => {
            console.log(`KafkaClientService instance cleans up allocated resources before Job Server is terminated by a signal with an exit code ${process.exitCode}`);
            await KafkaClientService.terminate();
            process.exit();
        }
    );
    console.log("KafkaClientService singleton instance has been created successfully");
    
    kafkaClientService.consumerRun({
        partitionsConsumedConcurrently: 1,
        eachMessage: async ({ topic, message, heartbeat }) => {
            if (topic == KAFKA_JOB_INIT_TOPIC) {
                
            }
        }
    }, 0);
    // postgreSQLAdapter.query({text: "select * from challenge"}, PostgreSQLQueryType.ADMIN_QUERY).then((res) => console.log(JSON.stringify(res!.rows[0].test_cases)));
    // postgreSQLAdapter.query({
    //     text: 
    //     `insert into challenge values (2, 'fabian pascal', now(), 'create table', '${JSON.stringify([{"id": 1, "data": "insert statement"}, {"id": 2, "data": "insert statement"}])}', 'solution select', 10);`
    // }, PostgreSQLQueryType.ADMIN_QUERY).then((res) => console.log(res));
})();
 
function registerEventHandlers(
    beforeExitHandler: (code: string) => Promise<void>, 
    signalHandler: () => Promise<void>
) {
    process.on("beforeExit", beforeExitHandler)
           .on("SIGINT", signalHandler)
           .on("SIGTERM", signalHandler);
}

async function kafkaJobInitEventHandler(
    postgreSQLAdapter: PostgreSQLAdapter,
    kafkaClientService: KafkaClientService,
    producerIndex: number,
    message: KafkaMessage, 
    heartbeat: () => Promise<void>
) {
    const heartbeatTimer: NodeJS.Timer = setInterval(heartbeat, KAFKA_JOB_INIT_CONSUMER_HEARTBEAT_INTERVAL)
    const jobInitEventValue: JobInitEventValue = JSON.parse(message.value!.toString());
    try {
        const { rows }: { rows: boolean[][] } = (await postgreSQLAdapter.query({
            text: `SELECT EXISTS (SELECT * FROM ${process.env.DB_CHALLENGE_TABLE_NAME!} WHERE challenge_id = $1)`,
            values: [jobInitEventValue.challenge_id.toString()],
            rowMode: 'array'
        }, PostgreSQLQueryType.ADMIN_QUERY))!;
        if (rows[0][0]) {
            await postgreSQLAdapter.query({
                text: `UPDATE ${process.env.DB_CHALLENGE_TABLE_NAME!} SET expires_at = $1 WHERE challenge_id = $2`,
                values: [new Date(jobInitEventValue.expires_at), jobInitEventValue.challenge_id],
            }, PostgreSQLQueryType.ADMIN_QUERY);
        } else {
            try {
                
            } catch (error) {
                
                throw error;
            }
        }
        await kafkaClientService.producerSend({
            topic: KAFKA_JOB_INIT_COMPLETION_TOPIC,
            messages: [{
                key: jobInitEventValue.challenge_id.toString(),
                value: JSON.stringify({
                    challenge_id: jobInitEventValue.challenge_id,
                    status: KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_COMPLETED
                })
            }]
        }, producerIndex);
    } catch(error) {
        await kafkaClientService.producerSend({
            topic: KAFKA_JOB_INIT_COMPLETION_TOPIC,
            messages: [{
                key: jobInitEventValue.challenge_id.toString(),
                value: JSON.stringify({
                    challenge_id: jobInitEventValue.challenge_id,
                    status: KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_FAILED,
                    error: (error as Error).toString()
                })
            }]
        }, producerIndex);
    } finally {
        clearInterval(heartbeatTimer);
    }

}


// const kafka = new Kafka({
//     clientId: 'job-server-kafka',
//     brokers: ['localhost:9092']
// });

// const consumer = kafka.consumer({groupId: 'haha'});
// consumer.run({
//     eachMessage: 
// })

// const admin = kafka.admin();
// admin.connect().then(() => console.log('hello'));
// admin.deleteTopics({topics: ['example', 'example1']}).then(console.log)
// admin.listTopics().then(x => console.log(x, x.includes('example')))
// admin.describeCluster().then(console.log)
// admin.listGroups().then(console.log);
// admin.createTopics({
//     validateOnly: false,
//     topics: [{
//         topic: 'example',
//         numPartitions: 2,
//         replicationFactor: 1
//     }, {
//         topic: 'example1',
//         numPartitions: 2,
//         replicationFactor: 1
//     }]
// }).then((result) => console.log(result));


// const pool = new Pool({
//     host: 'localhost',
//     port: 5432,
//     database: 'example',
//     user: 'postgres',
//     password: 'password'
// });

// pool.query({text: 'EXPLAIN ANALYZE SELECT * FROM items NATURAL JOIN warehouses NATURAL JOIN stocks;'}).then(console.log);

// const myPool = new PostgreSQLAdapter({
//     adminUsername: 'postgres',
//     host: 'localhost',
//     database: 'example',
//     adminPassword: 'password',
//     ordinaryUsername: 'postgres',
//     ordinaryPassword: 'password',
//     adminQueryPoolMaxConnections: 1,
//     fastQueryPoolMaxConnections: 1,
//     slowQueryPoolMaxConnections: 1,
//     port: 5432
// });
// PostgreSQLAdapter.execute("select 1; select now();", {
//     host: "localhost",
//     port: 5432,
//     dbname: "example",
//     username: "postgres",
//     password: "password"
// }).then((result) => console.log(result));
// console.log(process.env.PGPASSWORD);

// import {spawn} from 'child_process';
// const result = spawn('psql', ['--host=localhost', '--port=5432', '--username=postgres', '--dbname=example', '--file=/Users/yisong.yu/test_sql_file.txt']);
// result.stdout.on('data', (data) => console.log('normal: ', data.toString()));
// result.stderr.on('data', (data) => console.log('error: ', data.toString()));
// result.on('close', (code) => console.log('code status: ', code));
// console.log(result.stderr.toString());
// import {randomBytes} from 'crypto';
// import {writeFileSync, unlink} from 'fs';
// import { PostgreSQLConnectionStringBuilder } from "./utils/postgreSQL-connection-string-builder";
// console.log(PostgreSQLConnectionStringBuilder.build({
//     host: "localhost",
//     port: 5432,
//     dbname: "example",
//     username: "postgres",
//     password: "password"
// }));

// console.log(writeFileSync(__dirname + '/test.txt', 'hahahaha'));
// unlink(__dirname + '/test.txt', (err) => {});

// console.log(['--port', 2].join('='));
// myPool
//     .query({text: 'EXPLAIN ANALYZE SELECT * FROM items NATURAL JOIN warehouses NATURAL JOIN stocks;'}, QueryType.ADMIN_QUERY)
//     .then(console.log);
// setInterval(() => pool.query({text: 'EXPLAIN ANALYZE SELECT * FROM items', rowMode: 'array'}, (err, res) => {
//     console.log(res);
// }), 2000);
// import process from 'process';
// process.on("SIGINT", () => {
//     console.log('received signal');
//     process.exit();
// });
// process.on("exit", (x) => {
//     console.log('haha' + x);
//     pool.end();
// });

//   pool.query({text: 'EXPLAIN ANALYZE SELECT * FROM items', rowMode: 'array'}, (err, res) => {
//     console.log(res)
//     pool.end()
//   });
// (async () => {
//     console.log('starting async query')
//     const result = await pool.query('SELECT NOW()')
//     console.log('async query finished')
//     console.log('starting callback query')
//     pool.query('SELECT NOW()', (err, res) => {
//       console.log('callback query finished')
//     })
//     console.log('calling end')
//     await pool.end()
//     console.log('pool has drained')
//   })();

// const client = new Client({
//     user: 'postgres',
//     host: 'localhost',
//     database: 'example',
//     password: 'password',
//     port: 5432,
// });

// import {spawn} from "child_process";


// const schema_name = "testhaha";
// client.connect();
// client.query("select now();", (err, res) => {
//     console.log(res.rows);
// })
// client.query(`CREATE SCHEMA IF NOT EXISTS ${schema_name}`, (err, res) => {
//     console.log(res.rows)
//     client.query(`SET search_path TO ${schema_name}`, (err, res) => {
//         console.log(res.rows)
//         client.query("CREATE TABLE IF NOT EXISTS lol (i int);", (err, res) => {
//             console.log(res.rows);
//             client.end();
//         })
//     });
// });


// const client = new Client()
// client.connect()
// client.query('SELECT $1::text as message', ['Hello world!'], (err, res) => {
//   console.log(err ? err.stack : res.rows[0].message) // Hello World!
//   client.end()
// })

