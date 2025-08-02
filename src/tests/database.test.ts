import { DataLoader } from '../database/loader';
import { DuckDBConnection } from '../database/connection';
import * as path from 'path';
import * as fs from 'fs';

describe('DataLoader', () => {
  beforeAll(() => {
    // Create test data directory
    const testDir = path.join(__dirname, 'test-data');
    if (!fs.existsSync(testDir)) {
      fs.mkdirSync(testDir, { recursive: true });
    }
    
    // Create test CSV file
    const csvContent = 'id,name,value\\n1,test,100\\n2,test2,200';
    fs.writeFileSync(path.join(testDir, 'test.csv'), csvContent);
  });

  afterAll(() => {
    // Clean up test data
    const testDir = path.join(__dirname, 'test-data');
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true, force: true });
    }
  });

  test('should detect directory path type', async () => {
    const testDir = path.join(__dirname, 'test-data');
    const config = await DataLoader.detectPathType(testDir);
    
    expect(config.isDirectory).toBe(true);
    expect(config.readonly).toBe(true);
    expect(config.path).toBe(testDir);
  });

  test('should reject non-existent path', async () => {
    await expect(DataLoader.detectPathType('/non/existent/path'))
      .rejects
      .toThrow('Path does not exist');
  });
});

describe('DuckDBConnection', () => {
  test('should create connection with readonly config', () => {
    const config = {
      path: ':memory:',
      isDirectory: true,
      readonly: true
    };
    
    const connection = new DuckDBConnection(config);
    expect(connection.getConfig()).toEqual(config);
    expect(connection.isConnected()).toBe(false);
  });
});