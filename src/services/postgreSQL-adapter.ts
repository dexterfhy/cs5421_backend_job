import { spawn } from "child_process"
import { randomBytes } from "crypto";
import { unlink, writeFile } from "fs/promises";
import { join } from "path";
import { Pool, PoolClient, QueryResult } from "pg"
import { PostgreSQLQueryType } from "../constants/postgreSQL";

export interface PostgreSQLAdapterConfig {
    host?: string,
    port?: number,
    database?: string,
    adminUsername?: string,
    adminPassword?: string,
    ordinaryUsername?: string,
    ordinaryPassword?: string,
    adminStatementTimeout?: number,
    fastStatementTimeout?: number,
    slowStatementTimeout?: number,
    adminQueryTimeout?: number,
    fastQueryTimeout?: number,
    slowQueryTimeout?: number,
    adminQueryPoolMaxConnections?: number,
    fastQueryPoolMaxConnections?: number,
    slowQueryPoolMaxConnections?: number,
    applicationName?: string
}

export interface PostgreSQLQuery {
    text: string,
    values?: any[],
    rowMode?: string,
    name?: string // for prepared statement
}

export interface PostgreSQLStatementsExecutionConfig {
    host?: string,
    port?: number,
    dbname?: string,
    username?: string,
    password?: string
}

export class PostgreSQLAdapter {
    private static postgreSQLAdapter: PostgreSQLAdapter | undefined;
    private adminQueryPool: Pool;
    private fastQueryPool: Pool;
    private slowQueryPool: Pool;

    public static initialize(adapterConfig: PostgreSQLAdapterConfig): PostgreSQLAdapter {
        if (!PostgreSQLAdapter.postgreSQLAdapter) {
            PostgreSQLAdapter.postgreSQLAdapter = new PostgreSQLAdapter(adapterConfig);
        }
        console.log(`[PostgreSQL Adapter - Initialize] PostgreSQLAdapter singleton instance has been initialized successfully`);
        return PostgreSQLAdapter.postgreSQLAdapter;
    }

    private constructor(adapterConfig: PostgreSQLAdapterConfig) {
        this.adminQueryPool = new Pool({
            host: adapterConfig.host,
            port: adapterConfig.port,
            database: adapterConfig.database,
            user: adapterConfig.adminUsername,
            password: adapterConfig.adminPassword,
            statement_timeout: adapterConfig.adminStatementTimeout,
            query_timeout: adapterConfig.adminQueryTimeout,
            max: adapterConfig.adminQueryPoolMaxConnections,
            application_name: adapterConfig.applicationName
        });
        this.fastQueryPool = new Pool({
            host: adapterConfig.host,
            port: adapterConfig.port,
            database: adapterConfig.database,
            user: adapterConfig.ordinaryUsername,
            password: adapterConfig.ordinaryPassword,
            statement_timeout: adapterConfig.fastStatementTimeout,
            query_timeout: adapterConfig.fastQueryTimeout,
            max: adapterConfig.fastQueryPoolMaxConnections,
            application_name: adapterConfig.applicationName
        });
        this.slowQueryPool = new Pool({
            host: adapterConfig.host,
            port: adapterConfig.port,
            database: adapterConfig.database,
            user: adapterConfig.ordinaryUsername,
            password: adapterConfig.ordinaryPassword,
            statement_timeout: adapterConfig.slowStatementTimeout,
            query_timeout: adapterConfig.slowQueryTimeout,
            max: adapterConfig.slowQueryPoolMaxConnections,
            application_name: adapterConfig.applicationName
        });
    }

    public static async terminate(): Promise<void> {
        if (PostgreSQLAdapter.postgreSQLAdapter) {
            await PostgreSQLAdapter.postgreSQLAdapter.adminQueryPool.end();
            await PostgreSQLAdapter.postgreSQLAdapter.fastQueryPool.end();
            await PostgreSQLAdapter.postgreSQLAdapter.slowQueryPool.end();
            PostgreSQLAdapter.postgreSQLAdapter = undefined;
            console.log(`[PostgreSQL Adapter - Terminate] PostgreSQLAdapter singleton instance has been terminated gracefully`);
        }
    }

    public async query(query: PostgreSQLQuery, queryType: PostgreSQLQueryType): Promise<void | QueryResult<any>> {
        console.log(`[PostgreSQL Adapter - Query] query: ${JSON.stringify(query)}, queryType: ${queryType}`);
        switch (queryType) {
            case PostgreSQLQueryType.ADMIN_QUERY:
                return this.adminQueryPool.query(query);
            case PostgreSQLQueryType.FAST_QUERY:
                return this.fastQueryPool.query(query);
            case PostgreSQLQueryType.SLOW_QUERY:
                return this.slowQueryPool.query(query);
            default:
                return Promise.resolve();
        }
    }

    public async schematizedQuery(schemaName: string, query: PostgreSQLQuery, queryType: PostgreSQLQueryType): Promise<void | QueryResult<any>> {
        console.log(`[PostgreSQL Adapter - Schematized Query] schemaName: ${schemaName}, query: ${JSON.stringify(query)}, queryType: ${queryType}`);
        let client: PoolClient | undefined;
        let queryResult: QueryResult<any> | undefined;
        switch (queryType) {
            case PostgreSQLQueryType.ADMIN_QUERY:
                client = await this.adminQueryPool.connect();
                break;
            case PostgreSQLQueryType.FAST_QUERY:
                client = await this.fastQueryPool.connect();
                break;
            case PostgreSQLQueryType.SLOW_QUERY:
                client = await this.slowQueryPool.connect();
            default:
                return;
        }
        try {
            await client.query(`SET search_path TO ${schemaName}`);
            queryResult = await client.query(query);
        } finally {
            client.release();
        }
        console.log(`[PostgreSQL Adapter - Schematized Query] queryResult: ${JSON.stringify(queryResult)}`);
        return queryResult;
    }

    public async transaction(queries: PostgreSQLQuery[], transactionType: PostgreSQLQueryType): Promise<void> {
        console.log(`[PostgreSQL Adapter - Transaction] queries: ${JSON.stringify(queries)}, transactionType: ${transactionType}`);
        let client: PoolClient | undefined;
        switch (transactionType) {
            case PostgreSQLQueryType.ADMIN_QUERY:
                client = await this.adminQueryPool.connect();
                break;
            case PostgreSQLQueryType.FAST_QUERY:
                client = await this.fastQueryPool.connect();
                break;
            case PostgreSQLQueryType.SLOW_QUERY:
                client = await this.slowQueryPool.connect();
            default:
                return;
        }
        try {
            await client.query("BEGIN");
            console.log(`[PostgreSQL Adapter - Transaction] BEGIN`);
            for (const query of queries) {
                await client.query(query);
            }
            await client.query("COMMIT");
            console.log(`[PostgreSQL Adapter - Transaction] COMMIT`);
        } catch (error) {
            await client.query("ROLLBACK");
            console.log(`[PostgreSQL Adapter - Transaction] ROLLBACK`);
            console.log(`[PostgreSQL Adapter - Transaction] Error: ${(error as Error).message}`);
            throw error;
        } finally {
            client.release()
        }
    }

    public static async execute(
        statements: string, 
        statementsExecutionConfig: PostgreSQLStatementsExecutionConfig
    ): Promise<[string, string]> {
        console.log(`[PostgreSQL Adapter - Execute] statements: ${statements}, statementsExecutionConfig: ${JSON.stringify(statementsExecutionConfig)}`);
        return new Promise(async (resolve, reject) => {
            try {
                const filename = randomBytes(20).toString("hex");
                const filePath = join(__dirname, filename);
                await writeFile(filePath, statements);
                const psqlChildProcess = spawn(
                    "psql", 
                    Object.entries(statementsExecutionConfig)
                          .filter(([key, _]) => key !== "password")
                          .map(([key, value]) => `--${key}=${value}`)
                          .concat(`--file=${filePath}`),
                    {
                        env: {
                            ...process.env,
                            PGPASSWORD: statementsExecutionConfig.password
                        }
                    }
                );
                const stdoutDataChunks: string[] = [];
                const stderrDataChunks: string[] = [];
                psqlChildProcess.stdout.on("data", stdoutDataChunks.push.bind(stdoutDataChunks));
                psqlChildProcess.stderr.on("data", stderrDataChunks.push.bind(stdoutDataChunks));
                psqlChildProcess.on("error", async (error) => {
                    try {
                        await unlink(filePath);
                    } catch (fileDeletionError) {
                        console.error(fileDeletionError);
                    } finally {
                        console.log(`[PostgreSQL Adapter - Execute] onError: ${error.message}`);
                        reject("Process error: " + error);
                    }
                });
                psqlChildProcess.on("close", async (code) => {
                    try {
                        await unlink(filePath);
                    } catch (fileDeletionError) {
                        console.error(fileDeletionError);
                    } finally {
                        console.log(`[PostgreSQL Adapter - Execute] onClose(stdout): ${stdoutDataChunks.join("")}, onClose(stderr): ${stderrDataChunks.join("")}`);
                        code !== 0 ? reject(code) : resolve([stdoutDataChunks.join(""), stderrDataChunks.join("")]);
                    }
                });
            } catch (error) {
                console.log(`[PostgreSQL Adapter - Execute] Error: ${(error as Error).message}`);
                reject(error);
            }
        });
    }
}
