import { spawn } from 'child_process';

let serverProcess: any;

beforeAll((done) => {
  const isWindows = process.platform === 'win32';
  const command = isWindows ? 'cross-env' : 'cross-env';
  const args = ['node', 'dist/index.js']; // Ensure the path matches your setup

  serverProcess = spawn(command, args, { stdio: 'pipe' });

  serverProcess.stdout.on('data', (data) => {
    if (data.toString().includes('DuckDB MCP Server running on stdio')) {
      done();
    }
  });

  serverProcess.stderr.on('data', (data) => {
    console.error(`stderr: ${data}`);
  });

  serverProcess.on('close', (code) => {
    console.log(`Server process exited with code ${code}`);
  });
});

afterAll(() => {
  if (serverProcess) {
    serverProcess.kill();
  }
});

