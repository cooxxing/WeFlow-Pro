import { useState, useEffect, useRef, useCallback } from 'react'
import { Send, Bot, User, Cpu, ChevronDown, Loader2 } from 'lucide-react'
import { Virtuoso, VirtuosoHandle } from 'react-virtuoso'
import { engineService, PRESET_MODELS, ModelInfo } from '../services/EngineService'
import { MessageBubble } from '../components/MessageBubble'
import './AIChatPage.scss'

interface ChatMessage {
    id: string;
    role: 'user' | 'ai';
    content: string;
    timestamp: number;
}

// 消息数量限制，避免内存过载
const MAX_MESSAGES = 200

export default function AIChatPage() {
    const [input, setInput] = useState('')
    const [messages, setMessages] = useState<ChatMessage[]>([])
    const [isTyping, setIsTyping] = useState(false)
    const [models, setModels] = useState<ModelInfo[]>([...PRESET_MODELS])
    const [selectedModel, setSelectedModel] = useState<string | null>(null)
    const [modelLoaded, setModelLoaded] = useState(false)
    const [loadingModel, setLoadingModel] = useState(false)
    const [isThinkingMode, setIsThinkingMode] = useState(true)
    const [showModelDropdown, setShowModelDropdown] = useState(false)

    const textareaRef = useRef<HTMLTextAreaElement>(null)
    const virtuosoRef = useRef<VirtuosoHandle>(null)
    const dropdownRef = useRef<HTMLDivElement>(null)

    // 流式渲染优化：使用 ref 缓存内容，使用 RAF 批量更新
    const streamingContentRef = useRef('')
    const streamingMessageIdRef = useRef<string | null>(null)
    const rafIdRef = useRef<number | null>(null)

    useEffect(() => {
        checkModelsStatus()

        // 初始化Llama服务（延迟初始化，用户进入此页面时启动）
        const initLlama = async () => {
            try {
                await window.electronAPI.llama?.init()
                console.log('[AIChatPage] Llama service initialized')
            } catch (e) {
                console.error('[AIChatPage] Failed to initialize Llama:', e)
            }
        }
        initLlama()

        // 清理函数：组件卸载时释放所有资源
        return () => {
            // 取消未完成的 RAF
            if (rafIdRef.current !== null) {
                cancelAnimationFrame(rafIdRef.current)
                rafIdRef.current = null
            }

            // 清理 engine service 的回调引用
            engineService.clearCallbacks()
        }
    }, [])

    // 监听页面卸载事件，确保资源释放
    useEffect(() => {
        const handleBeforeUnload = () => {
            // 清理回调和监听器
            engineService.dispose()
        }

        window.addEventListener('beforeunload', handleBeforeUnload)
        return () => window.removeEventListener('beforeunload', handleBeforeUnload)
    }, [])

    // 点击外部关闭下拉框
    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setShowModelDropdown(false)
            }
        }

        document.addEventListener('mousedown', handleClickOutside)
        return () => document.removeEventListener('mousedown', handleClickOutside)
    }, [])

    const scrollToBottom = useCallback(() => {
        // 使用 virtuoso 的 scrollToIndex 方法滚动到底部
        if (virtuosoRef.current && messages.length > 0) {
            virtuosoRef.current.scrollToIndex({
                index: messages.length - 1,
                behavior: 'smooth'
            })
        }
    }, [messages.length])

    const checkModelsStatus = async () => {
        const updatedModels = await Promise.all(models.map(async (m) => {
            const exists = await engineService.checkModelExists(m.path)
            return { ...m, downloaded: exists }
        }))
        setModels(updatedModels)

        // Auto-select first available model
        if (!selectedModel) {
            const available = updatedModels.find(m => m.downloaded)
            if (available) {
                setSelectedModel(available.path)
            }
        }
    }

    // 自动加载模型
    const handleLoadModel = async (modelPath?: string) => {
        const pathToLoad = modelPath || selectedModel
        if (!pathToLoad) return false

        setLoadingModel(true)
        try {
            await engineService.loadModel(pathToLoad)
            // Initialize session with system prompt
            await engineService.createSession("You are a helpful AI assistant.")
            setModelLoaded(true)
            return true
        } catch (e) {
            console.error("Load failed", e)
            alert("模型加载失败: " + String(e))
            return false
        } finally {
            setLoadingModel(false)
        }
    }

    // 选择模型（如果有多个）
    const handleSelectModel = (modelPath: string) => {
        setSelectedModel(modelPath)
        setShowModelDropdown(false)
    }

    // 获取可用的已下载模型
    const availableModels = models.filter(m => m.downloaded)
    const selectedModelInfo = models.find(m => m.path === selectedModel)

    // 优化的流式更新函数：使用 RAF 批量更新
    const updateStreamingMessage = useCallback(() => {
        if (!streamingMessageIdRef.current) return

        setMessages(prev => prev.map(msg =>
            msg.id === streamingMessageIdRef.current
                ? { ...msg, content: streamingContentRef.current }
                : msg
        ))

        rafIdRef.current = null
    }, [])

    // Token 回调：使用 RAF 批量更新 UI
    const handleToken = useCallback((token: string) => {
        streamingContentRef.current += token

        // 使用 requestAnimationFrame 批量更新，避免频繁渲染
        if (rafIdRef.current === null) {
            rafIdRef.current = requestAnimationFrame(updateStreamingMessage)
        }
    }, [updateStreamingMessage])

    const handleSend = async () => {
        if (!input.trim() || isTyping) return

        // 如果模型未加载，先自动加载
        if (!modelLoaded) {
            if (!selectedModel) {
                alert("请先下载模型（设置页面）")
                return
            }
            const loaded = await handleLoadModel()
            if (!loaded) return
        }

        const userMsg: ChatMessage = {
            id: Date.now().toString(),
            role: 'user',
            content: input,
            timestamp: Date.now()
        }

        setMessages(prev => {
            const newMessages = [...prev, userMsg]
            // 限制消息数量，避免内存过载
            return newMessages.length > MAX_MESSAGES
                ? newMessages.slice(-MAX_MESSAGES)
                : newMessages
        })
        setInput('')
        setIsTyping(true)

        // Reset textarea height
        if (textareaRef.current) {
            textareaRef.current.style.height = 'auto'
        }

        const aiMsgId = (Date.now() + 1).toString()
        streamingContentRef.current = ''
        streamingMessageIdRef.current = aiMsgId

        // Optimistic update for AI message start
        setMessages(prev => {
            const newMessages = [...prev, {
                id: aiMsgId,
                role: 'ai' as const,
                content: '',
                timestamp: Date.now()
            }]
            return newMessages.length > MAX_MESSAGES
                ? newMessages.slice(-MAX_MESSAGES)
                : newMessages
        })

        // Append thinking command based on mode
        const msgWithSuffix = input + (isThinkingMode ? " /think" : " /no_think")

        try {
            await engineService.chat(msgWithSuffix, handleToken, { thinking: isThinkingMode })
        } catch (e) {
            console.error("Chat failed", e)
            setMessages(prev => [...prev, {
                id: Date.now().toString(),
                role: 'ai',
                content: "❌ Error: Failed to get response from AI.",
                timestamp: Date.now()
            }])
        } finally {
            setIsTyping(false)
            streamingMessageIdRef.current = null

            // 确保最终状态同步
            if (rafIdRef.current !== null) {
                cancelAnimationFrame(rafIdRef.current)
                updateStreamingMessage()
            }
        }
    }

    // 渲染模型选择按钮（集成在输入框作为下拉项）
    const renderModelSelector = () => {
        // 没有可用模型
        if (availableModels.length === 0) {
            return (
                <button
                    className="model-btn disabled"
                    title="请先在设置页面下载模型"
                >
                    <Bot size={16} />
                    <span>无模型</span>
                </button>
            )
        }

        // 只有一个模型，直接显示
        if (availableModels.length === 1) {
            return (
                <button
                    className={`model-btn ${modelLoaded ? 'loaded' : ''} ${loadingModel ? 'loading' : ''}`}
                    title={modelLoaded ? "模型已就绪" : "发送消息时自动加载"}
                >
                    {loadingModel ? (
                        <Loader2 size={16} className="spin" />
                    ) : (
                        <Bot size={16} />
                    )}
                    <span>{loadingModel ? '加载中' : selectedModelInfo?.name || '模型'}</span>
                </button>
            )
        }

        // 多个模型，显示下拉选择
        return (
            <div className="model-selector" ref={dropdownRef}>
                <button
                    className={`model-btn ${modelLoaded ? 'loaded' : ''} ${loadingModel ? 'loading' : ''}`}
                    onClick={() => !loadingModel && setShowModelDropdown(!showModelDropdown)}
                    title="点击选择模型"
                >
                    {loadingModel ? (
                        <Loader2 size={16} className="spin" />
                    ) : (
                        <Bot size={16} />
                    )}
                    <span>{loadingModel ? '加载中' : selectedModelInfo?.name || '选择模型'}</span>
                    <ChevronDown size={13} className={showModelDropdown ? 'rotate' : ''} />
                </button>

                {showModelDropdown && (
                    <div className="model-dropdown">
                        {availableModels.map(model => (
                            <div
                                key={model.path}
                                className={`model-option ${selectedModel === model.path ? 'active' : ''}`}
                                onClick={() => handleSelectModel(model.path)}
                            >
                                <span>{model.name}</span>
                                {selectedModel === model.path && (
                                    <span className="check">✓</span>
                                )}
                            </div>
                        ))}
                    </div>
                )}
            </div>
        )
    }

    return (
        <div className="ai-chat-page">
            <div className="chat-main">
                {messages.length === 0 ? (
                    <div className="empty-state">
                        <div className="icon">
                            <Bot size={40} />
                        </div>
                        <h2>AI 为你服务</h2>
                        <p>
                            {availableModels.length === 0
                                ? "请先在设置页面下载模型"
                                : "输入消息开始对话，模型将自动加载"
                            }
                        </p>
                    </div>
                ) : (
                    <Virtuoso
                        ref={virtuosoRef}
                        data={messages}
                        className="messages-list"
                        initialTopMostItemIndex={messages.length - 1}
                        followOutput="smooth"
                        itemContent={(index, message) => (
                            <MessageBubble key={message.id} message={message} />
                        )}
                        components={{
                            Footer: () => <div className="list-spacer" />
                        }}
                    />
                )}

                <div className="input-area">
                    <div className="input-wrapper">
                        <textarea
                            ref={textareaRef}
                            value={input}
                            onChange={e => {
                                setInput(e.target.value)
                                e.target.style.height = 'auto'
                                e.target.style.height = `${Math.min(e.target.scrollHeight, 120)}px`
                            }}
                            onKeyDown={e => {
                                if (e.key === 'Enter' && !e.shiftKey) {
                                    e.preventDefault()
                                    handleSend()
                                    // Reset height after send
                                    if (textareaRef.current) textareaRef.current.style.height = 'auto'
                                }
                            }}
                            placeholder={availableModels.length === 0 ? "请先下载模型..." : "输入消息..."}
                            disabled={availableModels.length === 0 || loadingModel}
                            rows={1}
                        />
                        <div className="input-actions">
                            {renderModelSelector()}
                            <button
                                className={`mode-toggle ${isThinkingMode ? 'active' : ''}`}
                                onClick={() => setIsThinkingMode(!isThinkingMode)}
                                title={isThinkingMode ? "深度思考模式已开启" : "深度思考模式已关闭"}
                                disabled={availableModels.length === 0}
                            >
                                <Cpu size={18} />
                            </button>
                            <button
                                className="send-btn"
                                onClick={handleSend}
                                disabled={!input.trim() || availableModels.length === 0 || isTyping || loadingModel}
                            >
                                <Send size={18} />
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}
