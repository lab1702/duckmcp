import { DuckDBConnection } from '../database/connection.js';
import { TableInfo, TableSchema, ColumnInfo } from '../types.js';

export class MetadataTools {
  constructor(private connection: DuckDBConnection) {}

  async getTables(): Promise<TableInfo[]> {
    const sql = `
      SELECT 
        table_name as name,
        table_schema as schema,
        table_type as type
      FROM information_schema.tables
      WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
      ORDER BY table_schema, table_name
    `;

    const rows = await this.connection.query<{
      name: string;
      schema: string;
      type: string;
    }>(sql);

    return rows.map(row => ({
      name: row.name,
      schema: row.schema,
      type: row.type === 'BASE TABLE' ? 'TABLE' : 'VIEW',
    }));
  }

  async getTableSchema(tableName: string): Promise<TableSchema> {
    const sql = `
      SELECT 
        column_name as name,
        data_type as type,
        is_nullable as nullable,
        column_default as default_value
      FROM information_schema.columns
      WHERE table_name = ?
      ORDER BY ordinal_position
    `;

    const rows = await this.connection.query<{
      name: string;
      type: string;
      nullable: string;
      default_value: string | null;
    }>(sql, [tableName]);

    const columns: ColumnInfo[] = rows.map(row => ({
      name: row.name,
      type: row.type,
      nullable: row.nullable === 'YES',
      default_value: row.default_value || undefined,
    }));

    return {
      table_name: tableName,
      columns,
    };
  }

  async describeTable(tableName: string): Promise<{
    schema: TableSchema;
    rowCount: number;
    tableSize?: string;
  }> {
    // Get schema
    const schema = await this.getTableSchema(tableName);

    // Get row count
    const countSql = `SELECT COUNT(*) as count FROM "${tableName}"`;
    const countResult = await this.connection.query<{ count: number }>(countSql);
    const rowCount = countResult[0]?.count || 0;

    return {
      schema,
      rowCount,
    };
  }

  async getDatabaseInfo(): Promise<{
    version: string;
    tables: TableInfo[];
    totalTables: number;
    readonly: boolean;
  }> {
    // Get DuckDB version
    const versionSql = 'SELECT version() as version';
    const versionResult = await this.connection.query<{ version: string }>(versionSql);
    const version = versionResult[0]?.version || 'Unknown';

    // Get all tables
    const tables = await this.getTables();

    return {
      version,
      tables,
      totalTables: tables.length,
      readonly: this.connection.getConfig().readonly,
    };
  }
}