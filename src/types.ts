export interface DatabaseConfig {
  path: string;
  isDirectory: boolean;
  readonly: boolean;
}

export interface TableInfo {
  name: string;
  schema: string;
  type: 'TABLE' | 'VIEW';
  source?: string;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable: boolean;
  default_value?: string;
}

export interface TableSchema {
  table_name: string;
  columns: ColumnInfo[];
}

export interface QueryResult {
  columns: string[];
  rows: any[][];
  rowCount: number;
  executionTime?: number;
}

export interface SummaryResult {
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
}