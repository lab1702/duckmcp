import { DuckDBConnection } from '../database/connection.js';
import { QueryResult } from '../types.js';

export class QueryTools {
  constructor(private connection: DuckDBConnection) {}

  async executeQuery(sql: string): Promise<QueryResult> {
    const startTime = Date.now();

    try {
      const rows = await this.connection.query(sql);
      const executionTime = Date.now() - startTime;

      if (!rows || rows.length === 0) {
        return {
          columns: [],
          rows: [],
          rowCount: 0,
          executionTime,
        };
      }

      // Extract column names from the first row
      const columns = Object.keys(rows[0]);

      // Convert rows to array format
      const arrayRows = rows.map(row =>
        columns.map(col => row[col]),
      );

      return {
        columns,
        rows: arrayRows,
        rowCount: rows.length,
        executionTime,
      };
    } catch (error) {
      // Re-throw with execution time for consistency
      const executionTime = Date.now() - startTime;
      throw new Error(`Query execution failed (${executionTime}ms): ${(error as Error).message}`);
    }
  }

  async explainQuery(sql: string): Promise<QueryResult> {
    const explainSql = `EXPLAIN ${sql}`;
    return this.executeQuery(explainSql);
  }

  async validateQuery(sql: string): Promise<{ valid: boolean; error?: string }> {
    try {
      // Use EXPLAIN to validate without executing
      await this.explainQuery(sql);
      return { valid: true };
    } catch (error) {
      return {
        valid: false,
        error: (error as Error).message,
      };
    }
  }

  formatResults(result: QueryResult, limit?: number): string {
    if (result.rowCount === 0) {
      return 'No results returned.';
    }

    const displayRows = limit ? result.rows.slice(0, limit) : result.rows;
    const hasMore = limit && result.rows.length > limit;

    // Calculate column widths
    const colWidths = result.columns.map((col, i) => {
      const dataWidth = Math.max(
        ...displayRows.map(row => String(row[i] || '').length),
      );
      return Math.max(col.length, dataWidth, 3); // minimum width of 3
    });

    // Build table
    let output = '';

    // Header
    const header = result.columns
      .map((col, i) => col.padEnd(colWidths[i]))
      .join(' | ');
    output += header + '\n';

    // Separator
    const separator = colWidths
      .map(width => '-'.repeat(width))
      .join('-|-');
    output += separator + '\n';

    // Data rows
    for (const row of displayRows) {
      const rowStr = row
        .map((cell, i) => String(cell || '').padEnd(colWidths[i]))
        .join(' | ');
      output += rowStr + '\n';
    }

    // Footer info
    if (hasMore) {
      output += `\n... and ${result.rowCount - limit} more rows\n`;
    }

    output += `\n${result.rowCount} rows`;
    if (result.executionTime) {
      output += ` (${result.executionTime}ms)`;
    }

    return output;
  }
}