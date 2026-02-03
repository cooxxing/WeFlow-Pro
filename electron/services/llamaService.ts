import fs from "fs";
import { app, BrowserWindow } from "electron";
import path from "path";
import { ConfigService } from './config';

// Define interfaces locally to avoid static import of types that might not be available or cause issues
type LlamaModel = any;
type LlamaContext = any;
type LlamaChatSession = any;

export class LlamaService {
    private _model: LlamaModel | null = null;
    private _context: LlamaContext | null = null;
    private _sequence: any = null;
    private _session: LlamaChatSession | null = null;
    private _llama: any = null;
    private _nodeLlamaCpp: any = null;
    private configService = new ConfigService();
    private _initialized = false;

    constructor() {
        // 延迟初始化，只在需要时初始化
    }

    public async init() {
        if (this._initialized) return;
        
        try {
            // Dynamic import to handle ESM module in CJS context
            this._nodeLlamaCpp = await import("node-llama-cpp");
            this._llama = await this._nodeLlamaCpp.getLlama();
            this._initialized = true;
            console.log("[LlamaService] Llama initialized");
        } catch (error) {
            console.error("[LlamaService] Failed to initialize Llama:", error);
        }
    }

    public async loadModel(modelPath: string) {
        if (!this._llama) await this.init();

        try {
            console.log("[LlamaService] Loading model from:", modelPath);
            if (!this._llama) {
                throw new Error("Llama not initialized");
            }
            this._model = await this._llama.loadModel({
                modelPath: modelPath,
                gpuLayers: 'max', // Offload all layers to GPU if possible
                useMlock: false   // Disable mlock to avoid "VirtualLock" errors (common on Windows)
            });

            if (!this._model) throw new Error("Failed to load model");

            this._context = await this._model.createContext({
                contextSize: 8192, // Balanced context size for better performance
                batchSize: 2048    // Increase batch size for better prompt processing speed
            });

            if (!this._context) throw new Error("Failed to create context");

            this._sequence = this._context.getSequence();

            const { LlamaChatSession } = this._nodeLlamaCpp;
            this._session = new LlamaChatSession({
                contextSequence: this._sequence
            });

            console.log("[LlamaService] Model loaded successfully");
            return true;
        } catch (error) {
            console.error("[LlamaService] Failed to load model:", error);
            throw error;
        }
    }

    public async createSession(systemPrompt?: string) {
        if (!this._context) throw new Error("Model not loaded");
        if (!this._nodeLlamaCpp) await this.init();

        const { LlamaChatSession } = this._nodeLlamaCpp;

        if (!this._sequence) {
            this._sequence = this._context.getSequence();
        }

        this._session = new LlamaChatSession({
            contextSequence: this._sequence,
            systemPrompt: systemPrompt
        });

        return true;
    }

    public async chat(message: string, options: { thinking?: boolean } = {}, onToken: (token: string) => void) {
        if (!this._session) throw new Error("Session not initialized");

        const thinking = options.thinking ?? false;

        // Sampling parameters based on mode
        const samplingParams = thinking ? {
            temperature: 0.6,
            topP: 0.95,
            topK: 20,
            repeatPenalty: 1.5 // PresencePenalty=1.5
        } : {
            temperature: 0.7,
            topP: 0.8,
            topK: 20,
            repeatPenalty: 1.5
        };

        try {
            const response = await this._session.prompt(message, {
                ...samplingParams,
                onTextChunk: (chunk: string) => {
                    onToken(chunk);
                }
            });
            return response;
        } catch (error) {
            console.error("[LlamaService] Chat error:", error);
            throw error;
        }
    }

    public async getModelStatus(modelPath: string) {
        try {
            const exists = fs.existsSync(modelPath);
            if (!exists) {
                return { exists: false, path: modelPath };
            }
            const stats = fs.statSync(modelPath);
            return {
                exists: true,
                path: modelPath,
                size: stats.size
            };
        } catch (error) {
            return { exists: false, error: String(error) };
        }
    }

    private resolveModelDir(): string {
        const configured = this.configService.get('whisperModelDir') as string | undefined;
        if (configured) return configured;
        return path.join(app.getPath('documents'), 'WeFlow', 'models');
    }

    public async downloadModel(url: string, savePath: string, onProgress: (payload: { downloaded: number; total: number; speed: number }) => void): Promise<void> {
        // Ensure directory exists
        const dir = path.dirname(savePath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        console.info(`[LlamaService] Multi-threaded download check for: ${savePath}`);

        if (fs.existsSync(savePath)) {
            fs.unlinkSync(savePath);
        }

        // 1. Get total size and check range support
        let probeResult;
        try {
            probeResult = await this.probeUrl(url);
        } catch (err) {
            console.warn("[LlamaService] Probe failed, falling back to single-thread.", err);
            return this.downloadSingleThread(url, savePath, onProgress);
        }

        const { totalSize, acceptRanges, finalUrl } = probeResult;
        console.log(`[LlamaService] Total size: ${totalSize}, Accept-Ranges: ${acceptRanges}`);

        if (totalSize <= 0 || !acceptRanges) {
            console.warn("[LlamaService] Ranges not supported or size unknown, falling back to single-thread.");
            return this.downloadSingleThread(finalUrl, savePath, onProgress);
        }

        const threadCount = 4;
        const chunkSize = Math.ceil(totalSize / threadCount);
        const fd = fs.openSync(savePath, 'w');

        let downloadedLength = 0;
        let lastDownloadedLength = 0;
        let lastTime = Date.now();
        let speed = 0;

        const speedInterval = setInterval(() => {
            const now = Date.now();
            const duration = (now - lastTime) / 1000;
            if (duration > 0) {
                speed = (downloadedLength - lastDownloadedLength) / duration;
                lastDownloadedLength = downloadedLength;
                lastTime = now;
                onProgress({ downloaded: downloadedLength, total: totalSize, speed });
            }
        }, 1000);

        try {
            const promises = [];
            for (let i = 0; i < threadCount; i++) {
                const start = i * chunkSize;
                const end = i === threadCount - 1 ? totalSize - 1 : (i + 1) * chunkSize - 1;

                promises.push(this.downloadChunk(finalUrl, fd, start, end, (bytes) => {
                    downloadedLength += bytes;
                }));
            }

            await Promise.all(promises);
            console.log("[LlamaService] Multi-threaded download complete");

            // Final progress update
            onProgress({ downloaded: totalSize, total: totalSize, speed: 0 });
        } catch (err) {
            console.error("[LlamaService] Multi-threaded download failed:", err);
            throw err;
        } finally {
            clearInterval(speedInterval);
            fs.closeSync(fd);
        }
    }

    private async probeUrl(url: string): Promise<{ totalSize: number, acceptRanges: boolean, finalUrl: string }> {
        const protocol = url.startsWith('https') ? require('https') : require('http');
        const options = {
            method: 'GET',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.modelscope.cn/',
                'Range': 'bytes=0-0'
            }
        };

        return new Promise((resolve, reject) => {
            const req = protocol.get(url, options, (res: any) => {
                if ([301, 302, 307, 308].includes(res.statusCode)) {
                    const location = res.headers.location;
                    const nextUrl = new URL(location, url).href;
                    this.probeUrl(nextUrl).then(resolve).catch(reject);
                    return;
                }

                if (res.statusCode !== 206 && res.statusCode !== 200) {
                    reject(new Error(`Probe failed: HTTP ${res.statusCode}`));
                    return;
                }

                const contentRange = res.headers['content-range'];
                let totalSize = 0;
                if (contentRange) {
                    const parts = contentRange.split('/');
                    totalSize = parseInt(parts[parts.length - 1], 10);
                } else {
                    totalSize = parseInt(res.headers['content-length'] || '0', 10);
                }

                const acceptRanges = res.headers['accept-ranges'] === 'bytes' || !!contentRange;
                resolve({ totalSize, acceptRanges, finalUrl: url });
                res.destroy();
            });
            req.on('error', reject);
        });
    }

    private async downloadChunk(url: string, fd: number, start: number, end: number, onData: (bytes: number) => void): Promise<void> {
        const protocol = url.startsWith('https') ? require('https') : require('http');
        const options = {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Referer': 'https://www.modelscope.cn/',
                'Range': `bytes=${start}-${end}`
            }
        };

        return new Promise((resolve, reject) => {
            const req = protocol.get(url, options, (res: any) => {
                if (res.statusCode !== 206) {
                    reject(new Error(`Chunk download failed: HTTP ${res.statusCode}`));
                    return;
                }

                let currentOffset = start;
                res.on('data', (chunk: Buffer) => {
                    try {
                        fs.writeSync(fd, chunk, 0, chunk.length, currentOffset);
                        currentOffset += chunk.length;
                        onData(chunk.length);
                    } catch (err) {
                        reject(err);
                        res.destroy();
                    }
                });

                res.on('end', () => resolve());
                res.on('error', reject);
            });
            req.on('error', reject);
        });
    }

    private async downloadSingleThread(url: string, savePath: string, onProgress: (payload: { downloaded: number; total: number; speed: number }) => void): Promise<void> {
        return new Promise((resolve, reject) => {
            const protocol = url.startsWith('https') ? require('https') : require('http');
            const options = {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Referer': 'https://www.modelscope.cn/'
                }
            };

            const request = protocol.get(url, options, (response: any) => {
                if ([301, 302, 307, 308].includes(response.statusCode)) {
                    const location = response.headers.location;
                    const nextUrl = new URL(location, url).href;
                    this.downloadSingleThread(nextUrl, savePath, onProgress).then(resolve).catch(reject);
                    return;
                }

                if (response.statusCode !== 200) {
                    reject(new Error(`Fallback download failed: HTTP ${response.statusCode}`));
                    return;
                }

                const totalLength = parseInt(response.headers['content-length'] || '0', 10);
                let downloadedLength = 0;
                let lastDownloadedLength = 0;
                let lastTime = Date.now();
                let speed = 0;

                const fileStream = fs.createWriteStream(savePath);
                response.pipe(fileStream);

                const speedInterval = setInterval(() => {
                    const now = Date.now();
                    const duration = (now - lastTime) / 1000;
                    if (duration > 0) {
                        speed = (downloadedLength - lastDownloadedLength) / duration;
                        lastDownloadedLength = downloadedLength;
                        lastTime = now;
                        onProgress({ downloaded: downloadedLength, total: totalLength, speed });
                    }
                }, 1000);

                response.on('data', (chunk: any) => {
                    downloadedLength += chunk.length;
                });

                fileStream.on('finish', () => {
                    clearInterval(speedInterval);
                    fileStream.close();
                    resolve();
                });

                fileStream.on('error', (err: any) => {
                    clearInterval(speedInterval);
                    fs.unlink(savePath, () => { });
                    reject(err);
                });
            });
            request.on('error', reject);
        });
    }

    public getModelsPath() {
        return this.resolveModelDir();
    }
}

export const llamaService = new LlamaService();
