import { Worker } from 'worker_threads'
import { join } from 'path'
import { app } from 'electron'
import { existsSync, mkdirSync } from 'fs'
import { readFile, writeFile } from 'fs/promises'
import { ConfigService } from './config'
import { chatService } from './chatService'
import { wcdbService } from './wcdbService'
import { mapRowToCloneMessage, CloneMessage, CloneRole } from './cloneMessageUtils'

interface IndexOptions {
  reset?: boolean
  batchSize?: number
  chunkGapSeconds?: number
  maxChunkChars?: number
  maxChunkMessages?: number
}

interface QueryOptions {
  topK?: number
  roleFilter?: CloneRole
}

interface ToneGuide {
  sessionId: string
  createdAt: string
  model: string
  sampleSize: number
  summary: string
  details?: Record<string, any>
}

interface ChatRequest {
  sessionId: string
  message: string
  topK?: number
}

type WorkerRequest =
  | { id: string; type: 'index'; payload: any }
  | { id: string; type: 'query'; payload: any }

type PendingRequest = {
  resolve: (value: any) => void
  reject: (err: any) => void
  onProgress?: (payload: any) => void
}

class CloneService {
  private configService = new ConfigService()
  private worker: Worker | null = null
  private pending: Map<string, PendingRequest> = new Map()
  private requestId = 0
  private llmModel: any = null
  private llmContext: any = null
  private llmModelPath: string | null = null
  
  private resolveResourcesPath(): string {
    const candidate = app.isPackaged
      ? join(process.resourcesPath, 'resources')
      : join(app.getAppPath(), 'resources')
    if (existsSync(candidate)) return candidate
    const fallback = join(process.cwd(), 'resources')
    if (existsSync(fallback)) return fallback
    return candidate
  }

  private getBaseStoragePath(): string {
    const cachePath = this.configService.get('cachePath')
    if (cachePath && cachePath.length > 0) {
      return cachePath
    }
    const documents = app.getPath('documents')
    const defaultDir = join(documents, 'WeFlow')
    if (!existsSync(defaultDir)) {
      mkdirSync(defaultDir, { recursive: true })
    }
    return defaultDir
  }

  private getSessionDir(sessionId: string): string {
    const safeId = sessionId.replace(/[\\/:"*?<>|]+/g, '_')
    const base = this.getBaseStoragePath()
    const dir = join(base, 'clone_memory', safeId)
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true })
    return dir
  }

  private getToneGuidePath(sessionId: string): string {
    return join(this.getSessionDir(sessionId), 'tone_guide.json')
  }

  private ensureWorker(): Worker {
    if (this.worker) return this.worker
    const workerPath = join(__dirname, 'cloneEmbeddingWorker.js')
    const worker = new Worker(workerPath, {
      workerData: {
        resourcesPath: this.resolveResourcesPath(),
        userDataPath: this.getBaseStoragePath(),
        logEnabled: this.configService.get('logEnabled'),
        embeddingModel: 'Xenova/bge-small-zh-v1.5'
      }
    })

    worker.on('message', (msg: any) => {
      if (msg?.type === 'event' && msg.event === 'clone:indexProgress') {
        const entry = this.pending.get(msg.data?.requestId)
        if (entry?.onProgress) entry.onProgress(msg.data)
        return
      }
      if (msg?.type === 'response' && msg.id) {
        const entry = this.pending.get(msg.id)
        if (!entry) return
        this.pending.delete(msg.id)
        if (msg.ok) {
          entry.resolve(msg.data)
        } else {
          entry.reject(new Error(msg.error || 'worker error'))
        }
      }
    })

    worker.on('exit', () => {
      this.worker = null
      this.pending.clear()
    })

    this.worker = worker
    return worker
  }

  private callWorker(type: WorkerRequest['type'], payload: any, onProgress?: (payload: any) => void) {
    const worker = this.ensureWorker()
    const id = String(++this.requestId)
    const request: WorkerRequest = { id, type, payload }
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject, onProgress })
      worker.postMessage(request)
    })
  }

  private validatePrivateSession(sessionId: string): { ok: boolean; error?: string } {
    if (!sessionId) return { ok: false, error: 'sessionId 不能为空' }
    if (sessionId.includes('@chatroom')) {
      return { ok: false, error: '当前仅支持私聊' }
    }
    return { ok: true }
  }

  async indexSession(sessionId: string, options: IndexOptions = {}, onProgress?: (payload: any) => void) {
    const check = this.validatePrivateSession(sessionId)
    if (!check.ok) return { success: false, error: check.error }

    const dbPath = this.configService.get('dbPath')
    const decryptKey = this.configService.get('decryptKey')
    const myWxid = this.configService.get('myWxid')
    if (!dbPath || !decryptKey || !myWxid) {
      return { success: false, error: '数据库配置不完整' }
    }

    try {
      const result = await this.callWorker(
        'index',
        { sessionId, dbPath, decryptKey, myWxid, ...options },
        onProgress
      )
      return result
    } catch (err) {
      return { success: false, error: String(err) }
    }
  }

  async queryMemory(sessionId: string, keyword: string, options: QueryOptions = {}) {
    const check = this.validatePrivateSession(sessionId)
    if (!check.ok) return { success: false, error: check.error }
    if (!keyword) return { success: false, error: 'keyword 不能为空' }
    try {
      const result = await this.callWorker('query', { sessionId, keyword, ...options })
      return result
    } catch (err) {
      return { success: false, error: String(err) }
    }
  }

  async getToneGuide(sessionId: string) {
    const check = this.validatePrivateSession(sessionId)
    if (!check.ok) return { success: false, error: check.error }
    const filePath = this.getToneGuidePath(sessionId)
    if (!existsSync(filePath)) {
      return { success: false, error: '未找到性格说明书' }
    }
    const raw = await readFile(filePath, 'utf8')
    return { success: true, data: JSON.parse(raw) as ToneGuide }
  }

  async generateToneGuide(sessionId: string, sampleSize = 500) {
    const check = this.validatePrivateSession(sessionId)
    if (!check.ok) return { success: false, error: check.error }
    const connectResult = await chatService.connect()
    if (!connectResult.success) return { success: false, error: connectResult.error || '数据库未连接' }

    const myWxid = this.configService.get('myWxid')
    if (!myWxid) return { success: false, error: '缺少 myWxid 配置' }

    const cursorResult = await wcdbService.openMessageCursorLite(sessionId, 300, true, 0, 0)
    if (!cursorResult.success || !cursorResult.cursor) {
      return { success: false, error: cursorResult.error || '创建游标失败' }
    }

    const samples: CloneMessage[] = []
    let seen = 0
    let hasMore = true
    let cursor = cursorResult.cursor

    while (hasMore) {
      const batchResult = await wcdbService.fetchMessageBatch(cursor)
      if (!batchResult.success || !batchResult.rows) {
        await wcdbService.closeMessageCursor(cursor)
        return { success: false, error: batchResult.error || '读取消息失败' }
      }

      for (const row of batchResult.rows) {
        const msg = mapRowToCloneMessage(row, myWxid)
        if (!msg || msg.role !== 'target') continue
        seen += 1
        if (samples.length < sampleSize) {
          samples.push(msg)
        } else {
          const idx = Math.floor(Math.random() * seen)
          if (idx < sampleSize) samples[idx] = msg
        }
      }

      hasMore = batchResult.hasMore === true
    }

    await wcdbService.closeMessageCursor(cursor)

    if (samples.length === 0) {
      return { success: false, error: '样本为空，无法生成说明书' }
    }

    const toneResult = await this.runToneGuideLlm(sessionId, samples)
    if (!toneResult.success) return toneResult

    const filePath = this.getToneGuidePath(sessionId)
    await writeFile(filePath, JSON.stringify(toneResult.data, null, 2), 'utf8')
    return toneResult
  }

  async chat(request: ChatRequest) {
    const { sessionId, message, topK = 5 } = request
    const check = this.validatePrivateSession(sessionId)
    if (!check.ok) return { success: false, error: check.error }
    if (!message) return { success: false, error: '消息不能为空' }

    const toneGuide = await this.getToneGuide(sessionId)
    const toneText = toneGuide.success ? JSON.stringify(toneGuide.data) : '未找到说明书'

    const toolPrompt = [
      '你是一个微信好友的私聊分身，只能基于已知事实回答。',
      '如果需要查询过去的对话事实，请用工具。',
      '请严格输出 JSON，不要输出多余文本。',
      '当需要工具时输出：{"tool":"query_chat_history","parameters":{"keyword":"关键词"}}',
      '当无需工具时输出：{"tool":"none","response":"直接回复"}',
      `性格说明书: ${toneText}`,
      `用户: ${message}`
    ].join('\n')

    const decision = await this.runLlm(toolPrompt)
    const parsed = parseToolJson(decision)
    if (!parsed || parsed.tool === 'none') {
      return { success: true, response: parsed?.response || decision }
    }

    if (parsed.tool === 'query_chat_history') {
      const keyword = parsed.parameters?.keyword
      if (!keyword) return { success: true, response: decision }
      const memory = await this.queryMemory(sessionId, keyword, { topK, roleFilter: 'target' })
      const finalPrompt = [
        '你是一个微信好友的私聊分身，请根据工具返回的历史记录回答。',
        `性格说明书: ${toneText}`,
        `用户: ${message}`,
        `工具结果: ${JSON.stringify(memory)}`,
        '请直接回复用户，不要提及工具调用。'
      ].join('\n')
      const finalAnswer = await this.runLlm(finalPrompt)
      return { success: true, response: finalAnswer }
    }

    if (parsed.tool === 'get_tone_guide') {
      const finalPrompt = [
        '你是一个微信好友的私聊分身。',
        `性格说明书: ${toneText}`,
        `用户: ${message}`,
        '请直接回复用户。'
      ].join('\n')
      const finalAnswer = await this.runLlm(finalPrompt)
      return { success: true, response: finalAnswer }
    }

    return { success: true, response: decision }
  }

  private async runToneGuideLlm(sessionId: string, samples: CloneMessage[]) {
    const prompt = [
      '你是对话风格分析助手，请根据聊天样本总结性格说明书。',
      '输出 JSON：{"summary":"一句话概括","details":{"口癖":[],"情绪价值":"","回复速度":"","表情偏好":"","风格要点":[]}}',
      '以下是聊天样本（仅该好友的发言）：',
      samples.map((msg) => msg.content).join('\n')
    ].join('\n')

    const response = await this.runLlm(prompt)
    const parsed = parseToolJson(response)
    const toneGuide: ToneGuide = {
      sessionId,
      createdAt: new Date().toISOString(),
      model: this.configService.get('llmModelPath') || 'node-llama-cpp',
      sampleSize: samples.length,
      summary: parsed?.summary || response,
      details: parsed?.details || parsed?.data
    }
    return { success: true, data: toneGuide }
  }

  private async ensureLlmModel(): Promise<{ model: any; context: any; error?: string }> {
    const modelPath = this.configService.get('llmModelPath')
    if (!modelPath) {
      return { model: null, context: null, error: 'LLM 未配置，请设置 llmModelPath' }
    }

    // 如果模型路径改变，需要重新加载
    if (this.llmModelPath !== modelPath) {
      await this.releaseLlmModel()
      this.llmModelPath = modelPath
    }

    // 如果模型已加载，直接返回
    if (this.llmModel && this.llmContext) {
      return { model: this.llmModel, context: this.llmContext }
    }

    // 加载模型
    const llama = await import('node-llama-cpp').catch(() => null)
    if (!llama) {
      return { model: null, context: null, error: 'node-llama-cpp 未安装' }
    }

    try {
      const { LlamaModel, LlamaContext } = llama as any
      this.llmModel = new LlamaModel({ modelPath })
      this.llmContext = new LlamaContext({ model: this.llmModel })
      return { model: this.llmModel, context: this.llmContext }
    } catch (err) {
      await this.releaseLlmModel()
      return { model: null, context: null, error: `模型加载失败: ${String(err)}` }
    }
  }

  private async releaseLlmModel(): Promise<void> {
    try {
      if (this.llmContext) {
        // LlamaContext 可能需要显式释放
        if (typeof this.llmContext.dispose === 'function') {
          this.llmContext.dispose()
        }
        this.llmContext = null
      }
      if (this.llmModel) {
        // LlamaModel 可能需要显式释放
        if (typeof this.llmModel.dispose === 'function') {
          this.llmModel.dispose()
        }
        this.llmModel = null
      }
      this.llmModelPath = null
    } catch (err) {
      // 忽略释放错误
      console.error('释放 LLM 模型时出错:', err)
    }
  }

  private async runLlm(prompt: string): Promise<string> {
    const { model, context, error } = await this.ensureLlmModel()
    if (error || !model || !context) {
      return error || '模型加载失败'
    }

    try {
      const llama = await import('node-llama-cpp').catch(() => null)
      if (!llama) {
        return 'node-llama-cpp 未安装'
      }

      const { LlamaChatSession } = llama as any
      // 每次对话创建新的 session，但复用 model 和 context
      const session = new LlamaChatSession({ context })
      const result = await session.prompt(prompt)
      return typeof result === 'string' ? result : String(result)
    } catch (err) {
      return `LLM 推理失败: ${String(err)}`
    }
  }
}

function parseToolJson(raw: string): any | null {
  if (!raw) return null
  const trimmed = raw.trim()
  const start = trimmed.indexOf('{')
  const end = trimmed.lastIndexOf('}')
  if (start === -1 || end === -1 || end <= start) return null
  try {
    return JSON.parse(trimmed.slice(start, end + 1))
  } catch {
    return null
  }
}

export const cloneService = new CloneService()
