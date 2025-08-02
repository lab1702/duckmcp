import { DuckDBConnection } from '../database/connection.js';
import { SummaryResult } from '../types.js';

export class SummarizeTools {
  constructor(private connection: DuckDBConnection) {}

  async summarizeTable(tableName: string): Promise<SummaryResult[]> {
    // Use DuckDB's SUMMARIZE function
    const sql = `SUMMARIZE "${tableName}"`;

    try {
      const rows = await this.connection.query<{
        column_name: string;
        column_type: string;
        min?: any;
        max?: any;
        approx_unique?: number;
        avg?: number;
        std?: number;
        q25?: any;
        q50?: any;
        q75?: any;
        count?: number;
        null_percentage?: number;
      }>(sql);

      return rows.map(row => ({
        column_name: row.column_name,
        column_type: row.column_type,
        min: row.min,
        max: row.max,
        approx_unique: row.approx_unique,
        avg: row.avg,
        std: row.std,
        q25: row.q25,
        q50: row.q50,
        q75: row.q75,
        count: row.count,
        null_percentage: row.null_percentage,
      }));
    } catch (error) {
      throw new Error(`Failed to summarize table "${tableName}": ${(error as Error).message}`);
    }
  }

  async summarizeColumn(tableName: string, columnName: string): Promise<SummaryResult> {
    // Get detailed statistics for a specific column
    const sql = `
      SELECT 
        '${columnName}' as column_name,
        (SELECT typeof("${columnName}") FROM "${tableName}" LIMIT 1) as column_type,
        MIN("${columnName}") as min,
        MAX("${columnName}") as max,
        COUNT(DISTINCT "${columnName}") as approx_unique,
        AVG(TRY_CAST("${columnName}" AS DOUBLE)) as avg,
        STDDEV(TRY_CAST("${columnName}" AS DOUBLE)) as std,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY TRY_CAST("${columnName}" AS DOUBLE)) as q25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TRY_CAST("${columnName}" AS DOUBLE)) as q50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY TRY_CAST("${columnName}" AS DOUBLE)) as q75,
        COUNT("${columnName}") as count,
        (COUNT(*) - COUNT("${columnName}")) * 100.0 / COUNT(*) as null_percentage
      FROM "${tableName}"
    `;

    try {
      const rows = await this.connection.query<SummaryResult>(sql);
      if (rows.length === 0) {
        throw new Error(`Column "${columnName}" not found in table "${tableName}"`);
      }
      return rows[0];
    } catch (error) {
      throw new Error(`Failed to summarize column "${columnName}" in table "${tableName}": ${(error as Error).message}`);
    }
  }

  formatSummary(summary: SummaryResult[]): string {
    if (summary.length === 0) {
      return 'No columns to summarize.';
    }

    let output = 'Table Summary\n';
    output += '='.repeat(50) + '\n\n';

    for (const col of summary) {
      output += `Column: ${col.column_name} (${col.column_type})\n`;
      output += '-'.repeat(30) + '\n';

      if (col.count !== undefined) {
        output += `Count: ${col.count}\n`;
      }

      if (col.null_percentage !== undefined) {
        output += `Null %: ${col.null_percentage.toFixed(2)}%\n`;
      }

      if (col.approx_unique !== undefined) {
        output += `Unique: ${col.approx_unique}\n`;
      }

      if (col.min !== undefined && col.min !== null) {
        output += `Min: ${col.min}\n`;
      }

      if (col.max !== undefined && col.max !== null) {
        output += `Max: ${col.max}\n`;
      }

      if (col.avg !== undefined && col.avg !== null) {
        output += `Avg: ${col.avg.toFixed(2)}\n`;
      }

      if (col.std !== undefined && col.std !== null) {
        output += `Std: ${col.std.toFixed(2)}\n`;
      }

      if (col.q25 !== undefined && col.q25 !== null) {
        output += `Q25: ${col.q25}\n`;
      }

      if (col.q50 !== undefined && col.q50 !== null) {
        output += `Median: ${col.q50}\n`;
      }

      if (col.q75 !== undefined && col.q75 !== null) {
        output += `Q75: ${col.q75}\n`;
      }

      output += '\n';
    }

    return output;
  }
}