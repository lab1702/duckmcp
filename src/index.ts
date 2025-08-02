#!/usr/bin/env node

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';
import { Command } from 'commander';
import { DuckDBConnection } from './database/connection.js';
import { DataLoader } from './database/loader.js';
import { MetadataTools, QueryTools, SummarizeTools } from './tools/index.js';

class DuckDBMCPServer {
  private server: Server;
  private connection: DuckDBConnection;
  private loader: DataLoader;
  private metadataTools: MetadataTools;
  private queryTools: QueryTools;
  private summarizeTools: SummarizeTools;

  constructor() {
    this.server = new Server(
      {
        name: 'duckmcp',
        version: '1.0.0',
      },
      {
        capabilities: {
          tools: {},
        },
      }
    );

    this.setupToolHandlers();
  }

  async initialize(dataPath: string): Promise<void> {
    try {
      // Detect if path is directory or database file
      const config = await DataLoader.detectPathType(dataPath);
      
      // Initialize connection
      this.connection = new DuckDBConnection(config);
      await this.connection.connect();

      // Initialize loader and load data
      this.loader = new DataLoader(this.connection, config);
      await this.loader.loadData();

      // Initialize tools
      this.metadataTools = new MetadataTools(this.connection);
      this.queryTools = new QueryTools(this.connection);
      this.summarizeTools = new SummarizeTools(this.connection);

      console.error(`DuckDB MCP Server initialized with ${config.isDirectory ? 'directory' : 'database file'}: ${dataPath}`);
    } catch (error) {
      console.error('Failed to initialize server:', error);
      process.exit(1);
    }
  }

  private setupToolHandlers(): void {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: 'get_tables',
            description: 'List all available tables and views in the database',
            inputSchema: {
              type: 'object',
              properties: {},
            },
          },
          {
            name: 'get_schema',
            description: 'Get the schema (columns and types) for a specific table',
            inputSchema: {
              type: 'object',
              properties: {
                table_name: {
                  type: 'string',
                  description: 'Name of the table to get schema for',
                },
              },
              required: ['table_name'],
            },
          },
          {
            name: 'describe_table',
            description: 'Get detailed information about a table including schema and row count',
            inputSchema: {
              type: 'object',
              properties: {
                table_name: {
                  type: 'string',
                  description: 'Name of the table to describe',
                },
              },
              required: ['table_name'],
            },
          },
          {
            name: 'execute_query',
            description: 'Execute a SQL query against the database (readonly mode)',
            inputSchema: {
              type: 'object',
              properties: {
                sql: {
                  type: 'string',
                  description: 'SQL query to execute',
                },
                limit: {
                  type: 'number',
                  description: 'Maximum number of rows to return in formatted output (default: 100)',
                },
              },
              required: ['sql'],
            },
          },
          {
            name: 'summarize_table',
            description: 'Generate statistical summary of all columns in a table using DuckDB SUMMARIZE',
            inputSchema: {
              type: 'object',
              properties: {
                table_name: {
                  type: 'string',
                  description: 'Name of the table to summarize',
                },
              },
              required: ['table_name'],
            },
          },
          {
            name: 'get_database_info',
            description: 'Get general information about the database and connection',
            inputSchema: {
              type: 'object',
              properties: {},
            },
          },
        ],
      };
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      const { name, arguments: args } = request.params;

      try {
        switch (name) {
          case 'get_tables': {
            const tables = await this.metadataTools.getTables();
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(tables, null, 2),
                },
              ],
            };
          }

          case 'get_schema': {
            const { table_name } = args as { table_name: string };
            const schema = await this.metadataTools.getTableSchema(table_name);
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(schema, null, 2),
                },
              ],
            };
          }

          case 'describe_table': {
            const { table_name } = args as { table_name: string };
            const description = await this.metadataTools.describeTable(table_name);
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(description, null, 2),
                },
              ],
            };
          }

          case 'execute_query': {
            const { sql, limit = 100 } = args as { sql: string; limit?: number };
            const result = await this.queryTools.executeQuery(sql);
            const formatted = this.queryTools.formatResults(result, limit);
            return {
              content: [
                {
                  type: 'text',
                  text: formatted,
                },
              ],
            };
          }

          case 'summarize_table': {
            const { table_name } = args as { table_name: string };
            const summary = await this.summarizeTools.summarizeTable(table_name);
            const formatted = this.summarizeTools.formatSummary(summary);
            return {
              content: [
                {
                  type: 'text',
                  text: formatted,
                },
              ],
            };
          }

          case 'get_database_info': {
            const info = await this.metadataTools.getDatabaseInfo();
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify(info, null, 2),
                },
              ],
            };
          }

          default:
            throw new Error(`Unknown tool: ${name}`);
        }
      } catch (error) {
        return {
          content: [
            {
              type: 'text',
              text: `Error: ${(error as Error).message}`,
            },
          ],
          isError: true,
        };
      }
    });
  }

  async run(): Promise<void> {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
    console.error('DuckDB MCP Server running on stdio');
  }

  async cleanup(): Promise<void> {
    if (this.connection) {
      await this.connection.close();
    }
  }
}

async function main(): Promise<void> {
  const program = new Command();
  
  program
    .name('duckmcp')
    .description('DuckDB MCP Server - Query databases and data files via MCP')
    .version('1.0.0')
    .argument('<path>', 'Path to DuckDB database file or directory containing data files')
    .action(async (path: string) => {
      const server = new DuckDBMCPServer();
      
      // Handle cleanup on exit
      process.on('SIGINT', async () => {
        await server.cleanup();
        process.exit(0);
      });
      
      process.on('SIGTERM', async () => {
        await server.cleanup();
        process.exit(0);
      });

      await server.initialize(path);
      await server.run();
    });

  await program.parseAsync();
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error('Server failed:', error);
    process.exit(1);
  });
}