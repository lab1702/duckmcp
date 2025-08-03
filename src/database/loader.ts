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
        // Use both recursive and non-recursive patterns
        const patterns = [
          path.join(dataPath, `*.${ext}`),
          path.join(dataPath, `**/*.${ext}`)
        ];
        
        let files: string[] = [];
        for (const pattern of patterns) {
          try {
            const foundFiles = await glob(pattern.replace(/\\/g, '/'));
            files = files.concat(foundFiles);
          } catch (err) {
            console.warn(`Pattern ${pattern} failed:`, err);
          }
        }
        
        // Remove duplicates
        files = [...new Set(files)];
        
        console.log(`Found ${files.length} ${ext} files:`, files);

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
    files: string[],
  ): Promise<void> {
    const tableName = this.generateTableName(basePath, fileType);

    // Use glob pattern for DuckDB to handle all files at once
    // This supports HIVE partitioning automatically
    // Convert Windows paths to forward slashes for DuckDB
    const globPattern = path.join(basePath, `**/*.${fileType}`).replace(/\\/g, '/');

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
      console.log(`Attempting to create table '${tableName}' with SQL: ${sql}`);
      await this.connection.execute(sql);
      console.log(`Created table '${tableName}' for ${files.length} ${fileType} files`);
    } catch (error) {
      console.warn(`Warning: Failed to create table '${tableName}':`, error);
      // Try fallback approach - create table for individual files
      await this.createTableForIndividualFiles(tableName, fileType, files);
    }
  }

  private async createTableForIndividualFiles(
    tableName: string,
    fileType: string,
    files: string[],
  ): Promise<void> {
    console.log(`Trying fallback approach for ${tableName} with individual files`);
    
    // Convert Windows paths to forward slashes for DuckDB
    const normalizedFiles = files.map(f => f.replace(/\\/g, '/'));
    
    let sql: string;
    
    if (normalizedFiles.length === 1) {
      // Single file approach
      const filePath = normalizedFiles[0];
      switch (fileType) {
      case 'csv':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_csv('${filePath}', auto_detect=true)`;
        break;
      case 'parquet':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_parquet('${filePath}')`;
        break;
      case 'json':
      case 'jsonl':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_json('${filePath}', auto_detect=true)`;
        break;
      default:
        throw new Error(`Unsupported file type: ${fileType}`);
      }
    } else {
      // Multiple files - use array syntax
      const fileList = normalizedFiles.map(f => `'${f}'`).join(', ');
      switch (fileType) {
      case 'csv':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_csv([${fileList}], auto_detect=true)`;
        break;
      case 'parquet':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_parquet([${fileList}])`;
        break;
      case 'json':
      case 'jsonl':
        sql = `CREATE VIEW ${tableName} AS SELECT * FROM read_json([${fileList}], auto_detect=true)`;
        break;
      default:
        throw new Error(`Unsupported file type: ${fileType}`);
      }
    }
    
    try {
      console.log(`Fallback SQL: ${sql}`);
      await this.connection.execute(sql);
      console.log(`Successfully created table '${tableName}' using fallback approach`);
    } catch (error) {
      console.error(`Failed to create table '${tableName}' even with fallback:`, error);
      throw error;
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
          readonly: true,
        };
      } else if (stats.isFile()) {
        // Check if it's a DuckDB database file
        const ext = path.extname(inputPath).toLowerCase();
        if (ext === '.db' || ext === '.duckdb' || ext === '.sqlite') {
          return {
            path: inputPath,
            isDirectory: false,
            readonly: true,
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