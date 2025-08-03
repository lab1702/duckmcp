import pkg from 'duckdb';
const { Database } = pkg;
import { DatabaseConfig } from '../types.js';

export class DuckDBConnection {
  private db: InstanceType<typeof Database> | null = null;
  private config: DatabaseConfig;

  constructor(config: DatabaseConfig) {
    this.config = config;
  }

  async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      // Configure DuckDB
      const dbPath = this.config.isDirectory ? ':memory:' : this.config.path;
      
      // For directory mode, we need read-write access to create tables/views
      // For database file mode, use read-only for safety
      const accessMode = this.config.isDirectory ? 'read_write' : 'read_only';

      this.db = new Database(dbPath, {
        access_mode: accessMode,
      }, (err: any) => {
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
      if (params.length === 0) {
        // For queries without parameters, use simplified call
        this.db!.all(sql, (err: any, rows: any) => {
          if (err) {
            reject(new Error(`Query failed: ${err.message}`));
          } else {
            resolve(rows as T[]);
          }
        });
      } else {
        this.db!.all(sql, params, (err: any, rows: any) => {
          if (err) {
            reject(new Error(`Query failed: ${err.message}`));
          } else {
            resolve(rows as T[]);
          }
        });
      }
    });
  }

  async execute(sql: string, params: any[] = []): Promise<void> {
    if (!this.db) {
      throw new Error('Database not connected');
    }

    return new Promise((resolve, reject) => {
      if (params.length === 0) {
        // For execution without parameters, use simplified call
        this.db!.run(sql, (err: any) => {
          if (err) {
            reject(new Error(`Execution failed: ${err.message}`));
          } else {
            resolve();
          }
        });
      } else {
        this.db!.run(sql, params, (err: any) => {
          if (err) {
            reject(new Error(`Execution failed: ${err.message}`));
          } else {
            resolve();
          }
        });
      }
    });
  }

  async close(): Promise<void> {
    if (!this.db) {return;}

    return new Promise((resolve, reject) => {
      this.db!.close((err: any) => {
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