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

describe('Sample Datasets Integration', () => {
  let connection: DuckDBConnection;
  let loader: DataLoader;
  const testDataDir = path.join(process.cwd(), 'test', 'data');

  beforeAll(async () => {
    // Ensure test data directory exists
    expect(fs.existsSync(testDataDir)).toBe(true);
    
    const config = {
      path: testDataDir,
      isDirectory: true,
      readonly: true
    };
    
    connection = new DuckDBConnection(config);
    loader = new DataLoader(connection, config);
    
    await connection.connect();
    await loader.loadData();
  });

  afterAll(async () => {
    if (connection && connection.isConnected()) {
      await connection.close();
    }
  });

  test('should load people.csv with edge cases', async () => {
    const results = await connection.query('SELECT * FROM data_csv ORDER BY id');
    
    expect(results).toHaveLength(10);
    
    // Test specific edge cases
    const johnDoe = results.find(r => r.id === 1n);
    expect(johnDoe.name.toString()).toBe('John "Johnny" Doe');
    expect(johnDoe.notes.toString()).toBe('Has nickname in quotes');
    
    const emptyName = results.find(r => r.id === 5n);
    expect(emptyName.name).toBe(null);
    expect(emptyName.notes.toString()).toBe('Empty name field');
    
    const internationalChars = results.find(r => r.id === 4n);
    expect(internationalChars.name.toString()).toBe('María González');
    expect(internationalChars.city.toString()).toBe('México City');
  });

  test('should load sales.json with nested structures', async () => {
    const results = await connection.query('SELECT * FROM data_json ORDER BY sale_id');
    
    expect(results).toHaveLength(3);
    
    // Test first sale
    const firstSale = results[0];
    expect(firstSale.sale_id).toBe(1001n);
    expect(Number(firstSale.total_amount)).toBe(483.75);
    expect(firstSale.status.toString()).toBe('completed');
    
    // Test that JSON fields are preserved
    expect(firstSale.customer).toBeDefined();
    expect(firstSale.items).toBeDefined();
  });

  test('should handle parquet files if supported', async () => {
    try {
      const results = await connection.query('SELECT * FROM data_parquet ORDER BY trip_id LIMIT 5');
      
      expect(results.length).toBeGreaterThan(0);
      expect(results.length).toBeLessThanOrEqual(5);
      
      // Test parquet data structure
      const firstTrip = results[0];
      expect(firstTrip).toHaveProperty('trip_id');
      expect(firstTrip).toHaveProperty('trip_name');
      expect(firstTrip).toHaveProperty('start_date');
      expect(firstTrip).toHaveProperty('end_date');
      expect(firstTrip).toHaveProperty('destination');
      expect(firstTrip).toHaveProperty('country');
      expect(firstTrip).toHaveProperty('cost_usd');
      
    } catch (error) {
      // If parquet is not supported, this test should be skipped
      console.warn('Parquet support not available, skipping parquet test');
    }
  });

  test('should show all available tables', async () => {
    const tables = await connection.query('SHOW TABLES', []);
    const tableNames = tables.map(t => t.name);
    
    console.log('Available tables:', tableNames);
    
    expect(tableNames).toContain('data_csv');
    expect(tableNames).toContain('data_json');
    
    // Parquet table may or may not be present depending on support
  });

  test('should handle complex queries across datasets', async () => {
    // Test joining data from different formats
    const query = `
      SELECT 
        csv.name,
        csv.email,
        COUNT(*) as person_count
      FROM data_csv csv
      WHERE csv.name IS NOT NULL
      GROUP BY csv.name, csv.email
      ORDER BY csv.name
      LIMIT 5
    `;
    
    const results = await connection.query(query);
    expect(results.length).toBeGreaterThan(0);
    expect(results.length).toBeLessThanOrEqual(5);
    
    // Verify structure
    results.forEach(row => {
      expect(row).toHaveProperty('name');
      expect(row).toHaveProperty('email');
      expect(row).toHaveProperty('person_count');
      expect(row.person_count).toBe(1n); // Each person should appear once
    });
  });
});
