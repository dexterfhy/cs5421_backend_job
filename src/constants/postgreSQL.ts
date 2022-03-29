export enum PostgreSQLQueryType {
    FAST_QUERY = 1,
    SLOW_QUERY = 2,
    ADMIN_QUERY = 3
}

export const POSTGRESQL_ADMIN_STATEMENT_TIMEOUT = 3600000;
export const POSTGRESQL_ADMIN_QUERY_TIMEOUT = 3600000;
export const POSTGRESQL_FAST_STATEMENT_TIMEOUT = 3600000;
export const POSTGRESQL_FAST_QUERY_TIMEOUT = 3600000;
export const POSTGRESQL_SLOW_STATEMENT_TIMEOUT = 7200000;
export const POSTGRESQL_SLOW_QUERY_TIMEOUT = 7200000;

/*
    *    *    *    *    *    *
    ┬    ┬    ┬    ┬    ┬    ┬
    │    │    │    │    │    │
    │    │    │    │    │    └ day of week (0 - 7) (0 or 7 is Sun)
    │    │    │    │    └───── month (1 - 12)
    │    │    │    └────────── day of month (1 - 31)
    │    │    └─────────────── hour (0 - 23)
    │    └──────────────────── minute (0 - 59)
    └───────────────────────── second (0 - 59, OPTIONAL)
*/
export const POSTGRESQL_DAILY_CLEANUP_TIME = "0 0 23 * * *";