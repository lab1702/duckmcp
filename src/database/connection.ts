import { Database } from 'duckdb';
import { DatabaseConfig } from '../types.js';

export class DuckDBConnection {
  private db: Database | null = null;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      // Configure DuckDB in readonly mode for safety
      const dbPath = this.config.isDirectory ? ':memory:' : this.config.path;
      
      this.db = new Database(dbPath, {
        access_mode: 'read_only',
        threads: '4'
      }, (err) => {
        if (err) {
          reject(new Error(`Failed to connect to DuckDB: ${err.message}`));
        } else {
          resolve();
        }
      });
    });
  }

  async query<T = any>(sql: string, params: any[] = []): Promise<T[]> {
    if (!this.db) {
      throw new Error('Database not connected');
    }

    return new Promise((resolve, reject) => {
      this.db!.all(sql, params, (err, rows) => {
        if (err) {
          reject(new Error(`Query failed: ${err.message}`));
        } else {
          resolve(rows as T[]);
        }
      });
    });
  }

  async execute(sql: string, params: any[] = []): Promise<void> {
    if (!this.db) {
      throw new Error('Database not connected');
    }

    return new Promise((resolve, reject) => {
      this.db!.run(sql, params, (err) => {
        if (err) {
          reject(new Error(`Execution failed: ${err.message}`));
        } else {
          resolve();
        }
      });
    });
  }

  async close(): Promise<void> {
    if (!this.db) return;

    return new Promise((resolve, reject) => {
      this.db!.close((err) => {
        if (err) {
          reject(new Error(`Failed to close database: ${err.message}`));
        } else {
          this.db = null;
          resolve();
        }
      });
    });
  }

  isConnected(): boolean {
    return this.db !== null;
  }

  getConfig(): DatabaseConfig {
    return { ...this.config };
  }
}