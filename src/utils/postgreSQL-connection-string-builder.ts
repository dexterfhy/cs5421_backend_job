export interface PostgreSQLConnectionStringComponents {
    host?: string,
    port?: number,
    dbname?: string,
    username?: string,
    password?: string
}

export class PostgreSQLConnectionStringBuilder {
    public static build(components: PostgreSQLConnectionStringComponents) {
        const {host, port, dbname, username, password} = components;
        const hostSpec = `${host ? host : ""}${port ? ":" + port : ""}`;
        const userSpec = `${username ? username : ""}${password ? ":" + password : ""}`
        const connectionString = `postgresql://${userSpec ? userSpec + "@" : ""}${hostSpec ? hostSpec : ""}${dbname ? "/" + dbname : ""}`;
        return connectionString;
    }
}
