
import { useState, useEffect, useRef, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { ZoomIn, ZoomOut, RotateCw, RotateCcw } from 'lucide-react'
import './ImageWindow.scss'

export default function ImageWindow() {
    const [searchParams] = useSearchParams()
    const imagePath = searchParams.get('imagePath')
    const [scale, setScale] = useState(1)
    const [rotation, setRotation] = useState(0)
    const [position, setPosition] = useState({ x: 0, y: 0 })
    const [initialScale, setInitialScale] = useState(1)
    const viewportRef = useRef<HTMLDivElement>(null)
    
    // 使用 ref 存储拖动状态，避免闭包问题
    const dragStateRef = useRef({
        isDragging: false,
        startX: 0,
        startY: 0,
        startPosX: 0,
        startPosY: 0
    })

    const handleZoomIn = () => setScale(prev => Math.min(prev + 0.25, 10))
    const handleZoomOut = () => setScale(prev => Math.max(prev - 0.25, 0.1))
    const handleRotate = () => setRotation(prev => (prev + 90) % 360)
    const handleRotateCcw = () => setRotation(prev => (prev - 90 + 360) % 360)
    
    // 重置视图
    const handleReset = useCallback(() => {
        setScale(1)
        setRotation(0)
        setPosition({ x: 0, y: 0 })
    }, [])

    // 图片加载完成后计算初始缩放
    const handleImageLoad = useCallback((e: React.SyntheticEvent<HTMLImageElement>) => {
        const img = e.currentTarget
        const naturalWidth = img.naturalWidth
        const naturalHeight = img.naturalHeight
        
        if (viewportRef.current) {
            const viewportWidth = viewportRef.current.clientWidth * 0.9
            const viewportHeight = viewportRef.current.clientHeight * 0.9
            const scaleX = viewportWidth / naturalWidth
            const scaleY = viewportHeight / naturalHeight
            const fitScale = Math.min(scaleX, scaleY, 1)
            setInitialScale(fitScale)
            setScale(1)
        }
    }, [])

    // 使用原生事件监听器处理拖动
    useEffect(() => {
        const handleMouseMove = (e: MouseEvent) => {
            if (!dragStateRef.current.isDragging) return
            
            const dx = e.clientX - dragStateRef.current.startX
            const dy = e.clientY - dragStateRef.current.startY
            
            setPosition({
                x: dragStateRef.current.startPosX + dx,
                y: dragStateRef.current.startPosY + dy
            })
        }

        const handleMouseUp = () => {
            dragStateRef.current.isDragging = false
            document.body.style.cursor = ''
        }

        document.addEventListener('mousemove', handleMouseMove)
        document.addEventListener('mouseup', handleMouseUp)

        return () => {
            document.removeEventListener('mousemove', handleMouseMove)
            document.removeEventListener('mouseup', handleMouseUp)
        }
    }, [])

    const handleMouseDown = (e: React.MouseEvent) => {
        if (e.button !== 0) return
        e.preventDefault()
        
        dragStateRef.current = {
            isDragging: true,
            startX: e.clientX,
            startY: e.clientY,
            startPosX: position.x,
            startPosY: position.y
        }
        document.body.style.cursor = 'grabbing'
    }

    const handleWheel = useCallback((e: React.WheelEvent) => {
        const delta = -Math.sign(e.deltaY) * 0.15
        setScale(prev => Math.min(Math.max(prev + delta, 0.1), 10))
    }, [])

    // 双击重置
    const handleDoubleClick = useCallback(() => {
        handleReset()
    }, [handleReset])

    // 快捷键支持
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') window.electronAPI.window.close()
            if (e.key === '=' || e.key === '+') handleZoomIn()
            if (e.key === '-') handleZoomOut()
            if (e.key === 'r' || e.key === 'R') handleRotate()
            if (e.key === '0') handleReset()
        }
        window.addEventListener('keydown', handleKeyDown)
        return () => window.removeEventListener('keydown', handleKeyDown)
    }, [handleReset])

    if (!imagePath) {
        return (
            <div className="image-window-empty">
                <span>无效的图片路径</span>
            </div>
        )
    }

    const displayScale = initialScale * scale

    return (
        <div className="image-window-container">
            <div className="title-bar">
                <div className="window-drag-area"></div>
                <div className="title-bar-controls">
                    <button onClick={handleZoomOut} title="缩小 (-)"><ZoomOut size={16} /></button>
                    <span className="scale-text">{Math.round(displayScale * 100)}%</span>
                    <button onClick={handleZoomIn} title="放大 (+)"><ZoomIn size={16} /></button>
                    <div className="divider"></div>
                    <button onClick={handleRotateCcw} title="逆时针旋转"><RotateCcw size={16} /></button>
                    <button onClick={handleRotate} title="顺时针旋转 (R)"><RotateCw size={16} /></button>
                </div>
            </div>

            <div 
                className="image-viewport" 
                ref={viewportRef}
                onWheel={handleWheel}
                onDoubleClick={handleDoubleClick}
                onMouseDown={handleMouseDown}
            >
                <img
                    src={imagePath}
                    alt="Preview"
                    style={{
                        transform: `translate(${position.x}px, ${position.y}px) scale(${displayScale}) rotate(${rotation}deg)`
                    }}
                    onLoad={handleImageLoad}
                    draggable={false}
                />
            </div>
        </div>
    )
}
