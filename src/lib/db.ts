export interface QueryResultRow {
    [column: string]: unknown;
}

export interface QueryResult<T extends QueryResultRow = QueryResultRow> {
    rows: T[];
    rowCount: number;
}

export interface DatabaseClient {
    query<T extends QueryResultRow = QueryResultRow>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
}

export const db: DatabaseClient = {
    async query<T extends QueryResultRow = QueryResultRow>(_sql: string, _params: unknown[] = []): Promise<QueryResult<T>> {
        // TODO: Replace with a real pg Pool / client implementation.
        throw new Error('Database client not implemented');
    }
};

