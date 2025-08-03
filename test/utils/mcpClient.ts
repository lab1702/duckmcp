import net from 'net';

export class MCPClient {
    private client: net.Socket;
    private host: string;
    private port: number;

    constructor(host: string, port: number) {
        this.host = host;
        this.port = port;
        this.client = new net.Socket();
    }

    connect(): Promise<void> {
        return new Promise((resolve, reject) => {
            this.client.connect(this.port, this.host, () => {
                console.log('Connected to MCP server');
                resolve();
            });

            this.client.on('error', (err) => {
                console.error('Connection error:', err);
                reject(err);
            });
        });
    }

    sendMessage(message: object): Promise<object> {
        return new Promise((resolve, reject) => {
            const jsonMessage = JSON.stringify(message);
            const framedMessage = jsonMessage + '\n';  // Framing with newline

            this.client.write(framedMessage, 'utf-8', () => {
                console.log('Message sent:', jsonMessage);
            });

            this.client.once('data', (data) => {
                console.log('Received data:', data.toString());
                try {
                    const response = JSON.parse(data.toString());
                    resolve(response);
                } catch (error) {
                    reject('Failed to parse response: ' + error);
                }
            });

            this.client.on('error', (err) => {
                reject('Connection error: ' + err);
            });
        });
    }

    disconnect() {
        this.client.end(() => {
            console.log('Disconnected from MCP server');
        });
    }
}

// Convenience wrapper examples
export const mcpToolOne = async (host: string, port: number, params: object) => {
    const client = new MCPClient(host, port);
    await client.connect();
    const response = await client.sendMessage({ tool: 'toolOne', ...params });
    client.disconnect();
    return response;
};

export const mcpToolTwo = async (host: string, port: number, params: object) => {
    const client = new MCPClient(host, port);
    await client.connect();
    const response = await client.sendMessage({ tool: 'toolTwo', ...params });
    client.disconnect();
    return response;
};
