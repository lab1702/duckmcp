import { glob } from 'glob';
import * as path from 'path';
import * as fs from 'fs';
import { DatabaseConfig } from '../types.js';
import { DuckDBConnection } from './connection.js';

export class DataLoader {
  private connection: DuckDBConnection;
  private config: DatabaseConfig;

  constructor(connection: DuckDBConnection, config: DatabaseConfig) {
    this.connection = connection;
    this.config = config;
  }

  async loadData(): Promise<void> {
    if (!this.config.isDirectory) {
      // Single database file - already handled by connection
      return;
    }

    // Directory mode - scan for data files and create views
    await this.loadDirectoryData();
  }

  private async loadDirectoryData(): Promise<void> {
    const supportedExtensions = ['csv', 'parquet', 'json', 'jsonl'];
    const dataPath = this.config.path;

    for (const ext of supportedExtensions) {
      try {
        // Use HIVE-style pattern to support partitioned data
        const pattern = path.join(dataPath, `**/*.${ext}`);
        const files = await glob(pattern);

        if (files.length > 0) {
          await this.createTableForFileType(ext, dataPath, files);
        }
      } catch (error) {
        console.warn(`Warning: Failed to load ${ext} files:`, error);
      }
    }
  }

  private async createTableForFileType(
    fileType: string, 
    basePath: string, 
    files: string[]
  ): Promise<void> {
    const tableName = this.generateTableName(basePath, fileType);
    
    // Use glob pattern for DuckDB to handle all files at once
    // This supports HIVE partitioning automatically
    const globPattern = path.join(basePath, `**/*.${fileType}`);
    
    let sql: string;
    
    switch (fileType) {
      case 'csv':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_csv('${globPattern}', auto_detect=true, hive_partitioning=true)`;
        break;
      case 'parquet':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_parquet('${globPattern}', hive_partitioning=true)`;
        break;
      case 'json':
      case 'jsonl':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_json('${globPattern}', auto_detect=true, hive_partitioning=true)`;
        break;
      default:
        throw new Error(`Unsupported file type: ${fileType}`);
    }

    try {
      await this.connection.execute(sql);
      console.log(`Created table '${tableName}' for ${files.length} ${fileType} files`);
    } catch (error) {
      console.warn(`Warning: Failed to create table '${tableName}':`, error);
    }
  }

  private generateTableName(basePath: string, fileType: string): string {
    const baseName = path.basename(basePath);
    // Sanitize table name to be SQL-safe
    const safeName = baseName.replace(/[^a-zA-Z0-9_]/g, '_');
    return `${safeName}_${fileType}`;
  }

  static async detectPathType(inputPath: string): Promise<DatabaseConfig> {
    try {
      const stats = await fs.promises.stat(inputPath);
      
      if (stats.isDirectory()) {
        return {
          path: inputPath,
          isDirectory: true,
          readonly: true
        };
      } else if (stats.isFile()) {
        // Check if it's a DuckDB database file
        const ext = path.extname(inputPath).toLowerCase();
        if (ext === '.db' || ext === '.duckdb' || ext === '.sqlite') {
          return {
            path: inputPath,
            isDirectory: false,
            readonly: true
          };
        } else {
          throw new Error(`Unsupported file type: ${ext}. Use .db, .duckdb, or specify a directory.`);
        }
      }
    } catch (error) {
      if ((error as any).code === 'ENOENT') {
        throw new Error(`Path does not exist: ${inputPath}`);
      }
      throw error;
    }

    throw new Error(`Invalid path: ${inputPath}`);
  }
}