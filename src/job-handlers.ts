import { KafkaMessage } from "kafkajs";
import { 
    KAFKA_JOB_INIT_CONSUMER_HEARTBEAT_INTERVAL, KAFKA_JOB_INIT_COMPLETION_TOPIC, 
    KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_COMPLETED, KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_FAILED, 
    KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC, KAFKA_JOB_ATTEMPT_COMPLETION_EVENT_STATUS_FAILED, 
    KAFKA_JOB_ATTEMPT_COMPLETION_EVENT_STATUS_WRONG, KAFKA_JOB_ATTEMPT_COMPLETION_EVENT_STATUS_CORRECT
} from "./constants/kafka";
import { PostgreSQLQueryType } from "./constants/postgreSQL";
import { KafkaClientService } from "./services/kafka-client-service";
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

interface JobAttemptEventValue {
    attempt_id: number,
    user_id: number,
    challenge_id: number,
    query: string    
}

export async function kafkaJobInitEventHandler(
    postgreSQLAdapter: PostgreSQLAdapter,
    postgreSQLQueryType: PostgreSQLQueryType,
    kafkaClientService: KafkaClientService,
    producerIndex: number,
    message: KafkaMessage, 
    heartbeat: () => Promise<void>
): Promise<void> {
    const heartbeatTimer: NodeJS.Timer = setInterval(heartbeat, KAFKA_JOB_INIT_CONSUMER_HEARTBEAT_INTERVAL);
    const jobInitEventValue: JobInitEventValue = JSON.parse(message.value!.toString());
    console.log(`[Job Handler - JobInitEventHandler] message: ${JSON.stringify(jobInitEventValue)}`);
    try {
        const { rows }: { rows: boolean[][] } = (await postgreSQLAdapter.schematizedQuery("public", {
            text: `SELECT EXISTS (SELECT * FROM ${process.env.DB_CHALLENGE_TABLE_NAME!} WHERE challenge_id = $1)`,
            values: [jobInitEventValue.challenge_id.toString()],
            rowMode: 'array'
        }, postgreSQLQueryType))!;
        console.log(`[Job Handler - JobIniitEventHandler] rows: ${JSON.stringify(rows)}`);
        if (rows[0][0]) {
            await postgreSQLAdapter.schematizedQuery("public", {
                text: `UPDATE ${process.env.DB_CHALLENGE_TABLE_NAME!} SET expires_at = $1 WHERE challenge_id = $2`,
                values: [new Date(jobInitEventValue.expires_at), jobInitEventValue.challenge_id],
            }, postgreSQLQueryType);
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
        } else {
            const createStatements: { text: string }[] = 
                jobInitEventValue.init!.split(";").filter(statement => statement).map(statement => ({ text: statement }));
            console.log(`[Job Handler - JobInitEventHandler] createStatements: ${JSON.stringify(createStatements)}`);
            jobInitEventValue.challenge_name = jobInitEventValue.challenge_name!.split(/(\s+)/).filter((word: string) => word.trim().length > 0).join("_");
            console.log(`[Job Handler - JobInitEventHandler] cleaned jobInitEventValue.challenge_name: ${jobInitEventValue.challenge_name!}`);
            jobInitEventValue.solution = jobInitEventValue.solution!.trim();
            jobInitEventValue.solution = 
                jobInitEventValue.solution![jobInitEventValue.solution!.length - 1] == ";" ? 
                    jobInitEventValue.solution!.slice(0, jobInitEventValue.solution!.length - 1) : jobInitEventValue.solution!;
            console.log(`[Job Handler - JobInitEventHandler] cleaned jobInitEventValue.solution: ${jobInitEventValue.solution!}`);
            try {
                await postgreSQLAdapter.transaction(
                    jobInitEventValue.test_cases!.flatMap((testCase: { id: number, data: string }) => {
                        const schemaName = `${jobInitEventValue.challenge_name!}_${jobInitEventValue.challenge_id}_${testCase.id}`;
                        return [
                            { text: "SET search_path TO public" },
                            { text: `DROP SCHEMA IF EXISTS ${schemaName} CASCADE` },
                            { text: `CREATE SCHEMA ${schemaName}` },
                            { text: `SET search_path TO ${schemaName}` }
                        ].concat(createStatements)
                    }),
                    postgreSQLQueryType
                );
                const [_, stderrData] = await PostgreSQLAdapter.execute(
                    jobInitEventValue.test_cases!.map((testCase: { id: number, data: string }) => {
                        const trimedStatements = testCase.data.trim();
                        return `SET search_path TO ${jobInitEventValue.challenge_name!}_${jobInitEventValue.challenge_id}_${testCase.id};` + 
                                (trimedStatements[trimedStatements.length - 1] == ";" ? trimedStatements : (trimedStatements + ";"));
                    }).join(""), 
                    {
                        host: process.env.DB_HOST,
                        port: parseInt(process.env.DB_PORT!),
                        dbname: process.env.DB_NAME,
                        username: process.env.DB_ADMIN_USERNAME,
                        password: process.env.DB_ADMIN_PASSWORD,
                    }
                );
                if (stderrData) {
                    throw new Error(stderrData);
                }
                await postgreSQLAdapter.schematizedQuery(
                    "public",
                    { 
                        text: 
                            `INSERT INTO ${process.env.DB_CHALLENGE_TABLE_NAME!} 
                             VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (challenge_id) DO UPDATE 
                             SET challenge_name = $2, expires_at = $3, init = $4, test_cases = $5, solution = $6, times_to_run = $7`,
                        values: [
                            jobInitEventValue.challenge_id, jobInitEventValue.challenge_name!, 
                            new Date(jobInitEventValue.expires_at!), jobInitEventValue.init!,
                            JSON.stringify(jobInitEventValue.test_cases!), 
                            jobInitEventValue.solution!, jobInitEventValue.times_to_run!
                        ]
                    },
                    postgreSQLQueryType
                );
                const expected_results: { id: number, expected_result: any[] }[] = 
                    await Promise.all(
                        jobInitEventValue.test_cases!.map(async (testCase: { id: number, data: string }) => {
                            const schemaName = 
                                `${jobInitEventValue.challenge_name}_${jobInitEventValue.challenge_id}_${testCase.id}`;
                            return {
                                id: testCase.id,
                                expected_result: (await postgreSQLAdapter.schematizedQuery(
                                    schemaName, 
                                    { text: jobInitEventValue.solution! }, 
                                    postgreSQLQueryType
                                ))!.rows
                            };
                        })
                    );
                console.log(`[Kafka Client Service - JobInitEventHandler] expected_results: ${JSON.stringify(expected_results)}`);
                await kafkaClientService.producerSend({
                    topic: KAFKA_JOB_INIT_COMPLETION_TOPIC,
                    messages: [{
                        key: jobInitEventValue.challenge_id.toString(),
                        value: JSON.stringify({
                            challenge_id: jobInitEventValue.challenge_id,
                            status: KAFKA_JOB_INIT_COMPLETION_EVENT_STATUS_COMPLETED,
                            expected_results: expected_results
                        })
                    }]
                }, producerIndex);
            } catch (error) {
                await Promise.all(
                    jobInitEventValue.test_cases!.map((testCase: { id: number, data: string }) => {
                        const schemaName = 
                            `${jobInitEventValue.challenge_name}_${jobInitEventValue.challenge_id}_${testCase.id}`;
                        return postgreSQLAdapter.schematizedQuery(
                            "public", 
                            { text: `DROP SCHEMA IF EXISTS ${schemaName} CASCADE` }, 
                            postgreSQLQueryType
                        );
                    })
                );
                await postgreSQLAdapter.schematizedQuery(
                    "public",
                    { 
                        text: `DELETE FROM ${process.env.DB_CHALLENGE_TABLE_NAME!} WHERE challenge_id = $1`,
                        values: [jobInitEventValue.challenge_id]
                    },
                    postgreSQLQueryType
                );
                throw error;
            }
        }
    } catch (error) {
        console.log(`[Kafka Client Service - JobInitEventHandler] Error: ${(error as Error).message}`);
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

export async function kafkaJobAttemptEventHandler(
    postgreSQLAdapter: PostgreSQLAdapter,
    postgreSQLQueryType: PostgreSQLQueryType,
    kafkaClientService: KafkaClientService,
    producerIndex: number,
    message: KafkaMessage, 
    heartbeat: () => Promise<void>
) {
    const heartbeatTimer: NodeJS.Timer = setInterval(heartbeat, KAFKA_JOB_INIT_CONSUMER_HEARTBEAT_INTERVAL);
    const jobAttemptEventValue: JobAttemptEventValue = JSON.parse(message.value!.toString());
    console.log(`[Job Handler - JobAttemptEventHandler] message: ${JSON.stringify(jobAttemptEventValue)}`);
    try {
        const { challenge_name, test_cases, solution, times_to_run }: 
            { challenge_name: string, test_cases: string, solution: string, times_to_run: number } = 
            (await postgreSQLAdapter.schematizedQuery(
                "public",
                { 
                    text: 
                        `SELECT challenge_name, test_cases, solution, times_to_run FROM 
                        ${process.env.DB_CHALLENGE_TABLE_NAME!} WHERE challenge_id = $1`,
                    values: [jobAttemptEventValue.challenge_id] 
                },
                PostgreSQLQueryType.ADMIN_QUERY
            ))!.rows[0];
        console.log(`[Job Handler - JobAttemptEventHandler] challenge_name: ${challenge_name}, test_cases: ${test_cases}, solution: ${solution}, times_to_run: ${times_to_run}`);
        const parsedTestCases: { id: number, data: string }[] = JSON.parse(test_cases);
        parsedTestCases.forEach(async (testCase: { id: number, data: string }) => {
            try {
                const schemaName: string = `${challenge_name}_${jobAttemptEventValue.challenge_id}_${testCase.id}`;
                const queryResult: any[] = (await postgreSQLAdapter.schematizedQuery(
                    schemaName,
                    { text: `${solution} EXCEPT ${jobAttemptEventValue.query}` },
                    postgreSQLQueryType
                ))!.rows;
                console.log(`[Job Handler - JobAttemptEventHandler] queryResult(correctness): ${JSON.stringify(queryResult)}`);
                if (queryResult.length != 0) {
                    await kafkaClientService.producerSend({
                        topic: KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC,
                        messages: [{
                            key: jobAttemptEventValue.attempt_id.toString(),
                            value: JSON.stringify({
                                attempt_id: jobAttemptEventValue.attempt_id,
                                user_id: jobAttemptEventValue.user_id,
                                challenge_id: jobAttemptEventValue.challenge_id,
                                test_case_id: testCase.id,
                                status: KAFKA_JOB_ATTEMPT_COMPLETION_EVENT_STATUS_WRONG,
                                actual_result: queryResult
                            })
                        }]
                    }, producerIndex);
                } else {
                    let overallExecutionTimes = 0;
                    for (let i = 0; i < times_to_run; i++) {
                        const queryResult: string[][] = (await postgreSQLAdapter.schematizedQuery(
                            schemaName,
                            { 
                                text: `EXPLAIN ANALYZE ${jobAttemptEventValue.query}`,
                                rowMode: "array"
                            },
                            postgreSQLQueryType
                        ))!.rows;
                        console.log(`[Job Handler - JobAttemptEventHandler] queryResult(efficiency-iteration_${i}): ${JSON.stringify(queryResult)}`);
                        overallExecutionTimes += 
                            parseFloat(queryResult[queryResult.length - 1][0].match(/^Execution Time: (\d+\.\d+) ms$/)![1]);
                    }
                    await kafkaClientService.producerSend({
                        topic: KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC,
                        messages: [{
                            key: jobAttemptEventValue.attempt_id.toString(),
                            value: JSON.stringify({
                                attempt_id: jobAttemptEventValue.attempt_id,
                                user_id: jobAttemptEventValue.user_id,
                                challenge_id: jobAttemptEventValue.challenge_id,
                                test_case_id: testCase.id,
                                status: KAFKA_JOB_ATTEMPT_COMPLETION_EVENT_STATUS_CORRECT,
                                execution_ms: overallExecutionTimes / times_to_run
                            })
                        }]
                    }, producerIndex);
                }
            } catch (error) {
                console.log(`[Job Handler - JobAttemptEventHandler] Error(test_case_${testCase.id}): ${(error as Error).message}`);
                await kafkaClientService.producerSend({
                    topic: KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC,
                    messages: [{
                        key: jobAttemptEventValue.attempt_id.toString(),
                        value: JSON.stringify({
                            attempt_id: jobAttemptEventValue.attempt_id,
                            user_id: jobAttemptEventValue.user_id,
                            challenge_id: jobAttemptEventValue.challenge_id,
                            test_case_id: testCase.id,
                            status: KAFKA_JOB_ATTEMPT_COMPLETION_EVENT_STATUS_FAILED,
                            error: (error as Error).toString()
                        })
                    }]
                }, producerIndex);
            }
        })
    } catch (error) {
        console.log(`[Job Handler - JobAttemptEventHandler] Error: ${(error as Error).message}`);
        await kafkaClientService.producerSend({
            topic: KAFKA_JOB_ATTEMPT_COMPLETION_TOPIC,
            messages: [{
                key: jobAttemptEventValue.attempt_id.toString(),
                value: JSON.stringify({
                    attempt_id: jobAttemptEventValue.attempt_id,
                    user_id: jobAttemptEventValue.user_id,
                    challenge_id: jobAttemptEventValue.challenge_id,
                    status: KAFKA_JOB_ATTEMPT_COMPLETION_EVENT_STATUS_FAILED,
                    error: (error as Error).toString()
                })
            }]
        }, producerIndex);
    } finally {
        clearInterval(heartbeatTimer);
    }
}