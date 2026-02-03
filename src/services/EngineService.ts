
export interface ModelInfo {
    name: string;
    path: string;
    downloadUrl?: string; // If it's a known preset
    size?: number;
    downloaded: boolean;
}

export const PRESET_MODELS: ModelInfo[] = [
    {
        name: "Qwen3 4B (Preset)",
        path: "Qwen3-4B-Q4_K_M.gguf",
        downloadUrl: "https://www.modelscope.cn/models/Qwen/Qwen3-4B-GGUF/resolve/master/Qwen3-4B-Q4_K_M.gguf",
        downloaded: false
    }
];

class EngineService {
    private onTokenCallback: ((token: string) => void) | null = null;
    private onProgressCallback: ((percent: number) => void) | null = null;
    private _removeTokenListener: (() => void) | null = null;
    private _removeProgressListener: (() => void) | null = null;

    constructor() {
        // Initialize listeners
        this._removeTokenListener = window.electronAPI.llama.onToken((token: string) => {
            if (this.onTokenCallback) {
                this.onTokenCallback(token);
            }
        });

        this._removeProgressListener = window.electronAPI.llama.onDownloadProgress((percent: number) => {
            if (this.onProgressCallback) {
                this.onProgressCallback(percent);
            }
        });
    }

    public async checkModelExists(filename: string): Promise<boolean> {
        const modelsPath = await window.electronAPI.llama.getModelsPath();
        const fullPath = `${modelsPath}\\${filename}`; // Windows path separator
        // We might need to handle path separator properly or let main process handle it
        // Updated preload to take full path or handling in main?
        // Let's rely on main process exposing join or just checking relative to models dir if implemented
        // Actually main process `checkFileExists` takes a path.
        // Let's assume we construct path here or Main helps. 
        // Better: getModelsPath returns the directory.
        return await window.electronAPI.llama.checkFileExists(fullPath);
    }

    public async getModelsPath(): Promise<string> {
        return await window.electronAPI.llama.getModelsPath();
    }

    public async loadModel(filename: string) {
        const modelsPath = await this.getModelsPath();
        const fullPath = `${modelsPath}\\${filename}`;
        console.log("Loading model:", fullPath);
        return await window.electronAPI.llama.loadModel(fullPath);
    }

    public async createSession(systemPrompt?: string) {
        return await window.electronAPI.llama.createSession(systemPrompt);
    }

    public async chat(message: string, onToken: (token: string) => void, options?: { thinking?: boolean }) {
        this.onTokenCallback = onToken;
        return await window.electronAPI.llama.chat(message, options);
    }

    public async downloadModel(url: string, filename: string, onProgress: (percent: number) => void) {
        const modelsPath = await this.getModelsPath();
        const fullPath = `${modelsPath}\\${filename}`;
        this.onProgressCallback = onProgress;
        return await window.electronAPI.llama.downloadModel(url, fullPath);
    }

    /**
     * 清除当前的回调函数引用
     * 用于避免内存泄漏
     */
    public clearCallbacks() {
        this.onTokenCallback = null;
        this.onProgressCallback = null;
    }

    /**
     * 释放所有资源
     * 包括事件监听器和回调引用
     */
    public dispose() {
        // 清除回调
        this.clearCallbacks();

        // 移除事件监听器
        if (this._removeTokenListener) {
            this._removeTokenListener();
            this._removeTokenListener = null;
        }
        if (this._removeProgressListener) {
            this._removeProgressListener();
            this._removeProgressListener = null;
        }
    }
}

export const engineService = new EngineService();
