import { useState, useEffect, useRef } from 'react'
import { Loader2, Download, Image, Check, X, SlidersHorizontal } from 'lucide-react'
import html2canvas from 'html2canvas'
import { useThemeStore } from '../stores/themeStore'
import './AnnualReportWindow.scss'

// SVG 背景图案 (用于导出)
const PATTERN_LIGHT_SVG = `<svg xmlns='http://www.w3.org/2000/svg' width='400' height='400' viewBox='0 0 400 400'><defs><style>.a{fill:none;stroke:#000;stroke-width:1.2;opacity:0.045}.b{fill:none;stroke:#000;stroke-width:1;opacity:0.035}.c{fill:none;stroke:#000;stroke-width:0.8;opacity:0.04}</style></defs><g transform='translate(45,35) rotate(-8)'><circle class='a' cx='0' cy='0' r='16'/><circle class='a' cx='-5' cy='-4' r='2.5'/><circle class='a' cx='5' cy='-4' r='2.5'/><path class='a' d='M-8 4 Q0 12 8 4'/></g><g transform='translate(320,28) rotate(15) scale(0.7)'><path class='b' d='M0 -12 l3 9 9 0 -7 5 3 9 -8 -6 -8 6 3 -9 -7 -5 9 0z'/></g><g transform='translate(180,55) rotate(12)'><path class='a' d='M0 -8 C0 -14 8 -17 12 -10 C16 -17 24 -14 24 -8 C24 4 12 14 12 14 C12 14 0 4 0 -8'/></g><g transform='translate(95,120) rotate(-5) scale(1.1)'><path class='b' d='M0 10 Q-8 10 -8 3 Q-8 -4 0 -4 Q0 -12 10 -12 Q22 -12 22 -2 Q30 -2 30 5 Q30 12 22 12 Z'/></g><g transform='translate(355,95) rotate(8)'><path class='c' d='M0 0 L0 18 M0 0 L18 -4 L18 14'/><ellipse class='c' cx='-4' cy='20' rx='6' ry='4'/><ellipse class='c' cx='14' cy='16' rx='6' ry='4'/></g><g transform='translate(250,110) rotate(-12) scale(0.9)'><rect class='b' x='0' y='0' width='26' height='18' rx='2'/><path class='b' d='M0 2 L13 11 L26 2'/></g><g transform='translate(28,195) rotate(6)'><circle class='a' cx='0' cy='0' r='11'/><path class='a' d='M-5 11 L5 11 M-4 14 L4 14'/><path class='c' d='M-3 -2 L0 -6 L3 -2'/></g><g transform='translate(155,175) rotate(-3) scale(0.85)'><path class='b' d='M0 0 L0 28 Q14 22 28 28 L28 0 Q14 6 0 0'/><path class='b' d='M28 0 L28 28 Q42 22 56 28 L56 0 Q42 6 28 0'/></g><g transform='translate(340,185) rotate(-20) scale(1.2)'><path class='a' d='M0 8 L20 0 L5 6 L8 14 L5 6 L-12 12 Z'/></g><g transform='translate(70,280) rotate(5)'><rect class='b' x='0' y='5' width='30' height='22' rx='4'/><circle class='b' cx='15' cy='16' r='7'/><rect class='b' x='8' y='0' width='14' height='6' rx='2'/></g><g transform='translate(230,250) rotate(-8) scale(1.1)'><rect class='a' x='0' y='6' width='22' height='18' rx='2'/><rect class='a' x='-3' y='0' width='28' height='7' rx='2'/><path class='a' d='M11 0 L11 24 M-3 13 L25 13'/></g><g transform='translate(365,280) rotate(10)'><ellipse class='b' cx='0' cy='0' rx='10' ry='14'/><path class='b' d='M0 14 Q-3 20 0 28 Q2 24 -1 20'/></g><g transform='translate(145,310) rotate(-6)'><path class='c' d='M0 0 L4 28 L24 28 L28 0 Z'/><path class='c' d='M28 6 Q40 6 40 16 Q40 24 28 24'/><path class='c' d='M8 8 Q10 4 12 8'/></g><g transform='translate(310,340) rotate(5) scale(0.9)'><path class='a' d='M0 8 L8 0 L24 0 L32 8 L16 28 Z'/><path class='a' d='M8 0 L12 8 L0 8 M24 0 L20 8 L32 8 M12 8 L16 28 L20 8'/></g><g transform='translate(55,365) rotate(25) scale(1.15)'><path class='a' d='M8 0 Q12 -14 16 0 L14 6 L18 12 L12 9 L6 12 L10 6 Z'/><circle class='c' cx='12' cy='-2' r='2'/></g><g transform='translate(200,375) rotate(-4)'><path class='b' d='M0 12 Q0 -8 24 -8 Q48 -8 48 12'/><path class='c' d='M6 12 Q6 -2 24 -2 Q42 -2 42 12'/><path class='c' d='M12 12 Q12 4 24 4 Q36 4 36 12'/></g><g transform='translate(380,375) rotate(-10)'><circle class='a' cx='0' cy='0' r='8'/><path class='c' d='M0 -14 L0 -10 M0 10 L0 14 M-14 0 L-10 0 M10 0 L14 0 M-10 -10 L-7 -7 M7 7 L10 10 M-10 10 L-7 7 M7 -7 L10 -10'/></g></svg>`

const PATTERN_DARK_SVG = `<svg xmlns='http://www.w3.org/2000/svg' width='400' height='400' viewBox='0 0 400 400'><defs><style>.a{fill:none;stroke:#fff;stroke-width:1.2;opacity:0.055}.b{fill:none;stroke:#fff;stroke-width:1;opacity:0.045}.c{fill:none;stroke:#fff;stroke-width:0.8;opacity:0.05}</style></defs><g transform='translate(45,35) rotate(-8)'><circle class='a' cx='0' cy='0' r='16'/><circle class='a' cx='-5' cy='-4' r='2.5'/><circle class='a' cx='5' cy='-4' r='2.5'/><path class='a' d='M-8 4 Q0 12 8 4'/></g><g transform='translate(320,28) rotate(15) scale(0.7)'><path class='b' d='M0 -12 l3 9 9 0 -7 5 3 9 -8 -6 -8 6 3 -9 -7 -5 9 0z'/></g><g transform='translate(180,55) rotate(12)'><path class='a' d='M0 -8 C0 -14 8 -17 12 -10 C16 -17 24 -14 24 -8 C24 4 12 14 12 14 C12 14 0 4 0 -8'/></g><g transform='translate(95,120) rotate(-5) scale(1.1)'><path class='b' d='M0 10 Q-8 10 -8 3 Q-8 -4 0 -4 Q0 -12 10 -12 Q22 -12 22 -2 Q30 -2 30 5 Q30 12 22 12 Z'/></g><g transform='translate(355,95) rotate(8)'><path class='c' d='M0 0 L0 18 M0 0 L18 -4 L18 14'/><ellipse class='c' cx='-4' cy='20' rx='6' ry='4'/><ellipse class='c' cx='14' cy='16' rx='6' ry='4'/></g><g transform='translate(250,110) rotate(-12) scale(0.9)'><rect class='b' x='0' y='0' width='26' height='18' rx='2'/><path class='b' d='M0 2 L13 11 L26 2'/></g><g transform='translate(28,195) rotate(6)'><circle class='a' cx='0' cy='0' r='11'/><path class='a' d='M-5 11 L5 11 M-4 14 L4 14'/><path class='c' d='M-3 -2 L0 -6 L3 -2'/></g><g transform='translate(155,175) rotate(-3) scale(0.85)'><path class='b' d='M0 0 L0 28 Q14 22 28 28 L28 0 Q14 6 0 0'/><path class='b' d='M28 0 L28 28 Q42 22 56 28 L56 0 Q42 6 28 0'/></g><g transform='translate(340,185) rotate(-20) scale(1.2)'><path class='a' d='M0 8 L20 0 L5 6 L8 14 L5 6 L-12 12 Z'/></g><g transform='translate(70,280) rotate(5)'><rect class='b' x='0' y='5' width='30' height='22' rx='4'/><circle class='b' cx='15' cy='16' r='7'/><rect class='b' x='8' y='0' width='14' height='6' rx='2'/></g><g transform='translate(230,250) rotate(-8) scale(1.1)'><rect class='a' x='0' y='6' width='22' height='18' rx='2'/><rect class='a' x='-3' y='0' width='28' height='7' rx='2'/><path class='a' d='M11 0 L11 24 M-3 13 L25 13'/></g><g transform='translate(365,280) rotate(10)'><ellipse class='b' cx='0' cy='0' rx='10' ry='14'/><path class='b' d='M0 14 Q-3 20 0 28 Q2 24 -1 20'/></g><g transform='translate(145,310) rotate(-6)'><path class='c' d='M0 0 L4 28 L24 28 L28 0 Z'/><path class='c' d='M28 6 Q40 6 40 16 Q40 24 28 24'/><path class='c' d='M8 8 Q10 4 12 8'/></g><g transform='translate(310,340) rotate(5) scale(0.9)'><path class='a' d='M0 8 L8 0 L24 0 L32 8 L16 28 Z'/><path class='a' d='M8 0 L12 8 L0 8 M24 0 L20 8 L32 8 M12 8 L16 28 L20 8'/></g><g transform='translate(55,365) rotate(25) scale(1.15)'><path class='a' d='M8 0 Q12 -14 16 0 L14 6 L18 12 L12 9 L6 12 L10 6 Z'/><circle class='c' cx='12' cy='-2' r='2'/></g><g transform='translate(200,375) rotate(-4)'><path class='b' d='M0 12 Q0 -8 24 -8 Q48 -8 48 12'/><path class='c' d='M6 12 Q6 -2 24 -2 Q42 -2 42 12'/><path class='c' d='M12 12 Q12 4 24 4 Q36 4 36 12'/></g><g transform='translate(380,375) rotate(-10)'><circle class='a' cx='0' cy='0' r='8'/><path class='c' d='M0 -14 L0 -10 M0 10 L0 14 M-14 0 L-10 0 M10 0 L14 0 M-10 -10 L-7 -7 M7 7 L10 10 M-10 10 L-7 7 M7 -7 L10 -10'/></g></svg>`

// 绘制 SVG 图案背景到 canvas
const drawPatternBackground = async (ctx: CanvasRenderingContext2D, width: number, height: number, bgColor: string, isDark: boolean) => {
  // 先填充背景色
  ctx.fillStyle = bgColor
  ctx.fillRect(0, 0, width, height)

  // 加载 SVG 图案
  const svgString = isDark ? PATTERN_DARK_SVG : PATTERN_LIGHT_SVG
  const blob = new Blob([svgString], { type: 'image/svg+xml' })
  const url = URL.createObjectURL(blob)

  return new Promise<void>((resolve) => {
    const img = new window.Image()
    img.onload = () => {
      // 平铺绘制图案
      const pattern = ctx.createPattern(img, 'repeat')
      if (pattern) {
        ctx.fillStyle = pattern
        ctx.fillRect(0, 0, width, height)
      }
      URL.revokeObjectURL(url)
      resolve()
    }
    img.onerror = () => {
      URL.revokeObjectURL(url)
      resolve()
    }
    img.src = url
  })
}

interface TopContact {
  username: string
  displayName: string
  avatarUrl?: string
  messageCount: number
  sentCount: number
  receivedCount: number
}

interface MonthlyTopFriend {
  month: number
  displayName: string
  avatarUrl?: string
  messageCount: number
}

interface AnnualReportData {
  year: number
  totalMessages: number
  totalFriends: number
  coreFriends: TopContact[]
  monthlyTopFriends: MonthlyTopFriend[]
  peakDay: { date: string; messageCount: number; topFriend?: string; topFriendCount?: number } | null
  longestStreak: { friendName: string; days: number; startDate: string; endDate: string } | null
  activityHeatmap: { data: number[][] }
  midnightKing: { displayName: string; count: number; percentage: number } | null
  selfAvatarUrl?: string
  mutualFriend?: { displayName: string; avatarUrl?: string; sentCount: number; receivedCount: number; ratio: number } | null
  socialInitiative?: { initiatedChats: number; receivedChats: number; initiativeRate: number } | null
  responseSpeed?: { avgResponseTime: number; fastestFriend: string; fastestTime: number } | null
  topPhrases?: { phrase: string; count: number }[]
  snsStats?: {
    totalPosts: number
    typeCounts?: Record<string, number>
    topLikers: { username: string; displayName: string; avatarUrl?: string; count: number }[]
    topLiked: { username: string; displayName: string; avatarUrl?: string; count: number }[]
  }
  lostFriend: {
    username: string
    displayName: string
    avatarUrl?: string
    earlyCount: number
    lateCount: number
    periodDesc: string
  } | null
}

interface SectionInfo {
  id: string
  name: string
  ref: React.RefObject<HTMLElement | null>
}

// 头像组件
const Avatar = ({ url, name, size = 'md' }: { url?: string; name: string; size?: 'sm' | 'md' | 'lg' }) => {
  const [imgError, setImgError] = useState(false)
  const initial = name?.[0] || '友'

  return (
    <div className={`avatar ${size}`}>
      {url && !imgError ? (
        <img src={url} alt="" onError={() => setImgError(true)} crossOrigin="anonymous" />
      ) : (
        <span>{initial}</span>
      )}
    </div>
  )
}

// 热力图组件
const Heatmap = ({ data }: { data: number[][] }) => {
  const maxHeat = Math.max(...data.flat())
  const weekLabels = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']

  return (
    <div className="heatmap-wrapper">
      <div className="heatmap-header">
        <div></div>
        <div className="time-labels">
          {[0, 6, 12, 18].map(h => (
            <span key={h} style={{ gridColumn: h + 1 }}>{h}</span>
          ))}
        </div>
      </div>
      <div className="heatmap">
        <div className="heatmap-week-col">
          {weekLabels.map(w => <div key={w} className="week-label">{w}</div>)}
        </div>
        <div className="heatmap-grid">
          {data.map((row, wi) =>
            row.map((val, hi) => {
              const alpha = maxHeat > 0 ? (val / maxHeat * 0.85 + 0.1).toFixed(2) : '0.1'
              return (
                <div
                  key={`${wi}-${hi}`}
                  className="h-cell"
                  style={{ background: `rgba(7, 193, 96, ${alpha})` }}
                  title={`${weekLabels[wi]} ${hi}:00 - ${val}条`}
                />
              )
            })
          )}
        </div>
      </div>
    </div>
  )
}

// 词云组件
const WordCloud = ({ words }: { words: { phrase: string; count: number }[] }) => {
  const maxCount = words.length > 0 ? words[0].count : 1
  const topWords = words.slice(0, 32)
  const baseSize = 520

  // 使用确定性随机数生成器
  const seededRandom = (seed: number) => {
    const x = Math.sin(seed) * 10000
    return x - Math.floor(x)
  }

  // 计算词云位置
  const placedItems: { x: number; y: number; w: number; h: number }[] = []

  const canPlace = (x: number, y: number, w: number, h: number): boolean => {
    const halfW = w / 2
    const halfH = h / 2
    const dx = x - 50
    const dy = y - 50
    const dist = Math.sqrt(dx * dx + dy * dy)
    const maxR = 49 - Math.max(halfW, halfH)
    if (dist > maxR) return false

    const pad = 1.8
    for (const p of placedItems) {
      if ((x - halfW - pad) < (p.x + p.w / 2) &&
        (x + halfW + pad) > (p.x - p.w / 2) &&
        (y - halfH - pad) < (p.y + p.h / 2) &&
        (y + halfH + pad) > (p.y - p.h / 2)) {
        return false
      }
    }
    return true
  }

  const wordItems = topWords.map((item, i) => {
    const ratio = item.count / maxCount
    const fontSize = Math.round(12 + Math.pow(ratio, 0.65) * 20)
    const opacity = Math.min(1, Math.max(0.35, 0.35 + ratio * 0.65))
    const delay = (i * 0.04).toFixed(2)

    // 计算词语宽度
    const charCount = Math.max(1, item.phrase.length)
    const hasCjk = /[\u4e00-\u9fff]/.test(item.phrase)
    const hasLatin = /[A-Za-z0-9]/.test(item.phrase)
    const widthFactor = hasCjk && hasLatin ? 0.85 : hasCjk ? 0.98 : 0.6
    const widthPx = fontSize * (charCount * widthFactor)
    const heightPx = fontSize * 1.1
    const widthPct = (widthPx / baseSize) * 100
    const heightPct = (heightPx / baseSize) * 100

    // 寻找位置
    let x = 50, y = 50
    let placedOk = false
    const tries = i === 0 ? 1 : 420

    for (let t = 0; t < tries; t++) {
      if (i === 0) {
        x = 50
        y = 50
      } else {
        const idx = i + t * 0.28
        const radius = Math.sqrt(idx) * 7.6 + (seededRandom(i * 1000 + t) * 1.2 - 0.6)
        const angle = idx * 2.399963 + seededRandom(i * 2000 + t) * 0.35
        x = 50 + radius * Math.cos(angle)
        y = 50 + radius * Math.sin(angle)
      }
      if (canPlace(x, y, widthPct, heightPct)) {
        placedOk = true
        break
      }
    }

    if (!placedOk) return null
    placedItems.push({ x, y, w: widthPct, h: heightPct })

    return (
      <span
        key={i}
        className="word-tag"
        style={{
          '--final-opacity': opacity,
          left: `${x.toFixed(2)}%`,
          top: `${y.toFixed(2)}%`,
          fontSize: `${fontSize}px`,
          animationDelay: `${delay}s`,
        } as React.CSSProperties}
        title={`${item.phrase} (出现 ${item.count} 次)`}
      >
        {item.phrase}
      </span>
    )
  }).filter(Boolean)

  return (
    <div className="word-cloud-wrapper">
      <div className="word-cloud-inner">
        {wordItems}
      </div>
    </div>
  )
}

function AnnualReportWindow() {
  const [reportData, setReportData] = useState<AnnualReportData | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [isExporting, setIsExporting] = useState(false)
  const [exportProgress, setExportProgress] = useState('')
  const [showExportModal, setShowExportModal] = useState(false)
  const [selectedSections, setSelectedSections] = useState<Set<string>>(new Set())
  const [fabOpen, setFabOpen] = useState(false)
  const [loadingProgress, setLoadingProgress] = useState(0)
  const [loadingStage, setLoadingStage] = useState('正在初始化...')
  const [exportMode, setExportMode] = useState<'separate' | 'long'>('separate')

  const { currentTheme, themeMode } = useThemeStore()

  // 应用主题到独立窗口
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', currentTheme)
    document.documentElement.setAttribute('data-mode', themeMode)
  }, [currentTheme, themeMode])

  // Section refs
  const sectionRefs = {
    cover: useRef<HTMLElement>(null),
    overview: useRef<HTMLElement>(null),
    bestFriend: useRef<HTMLElement>(null),
    monthlyFriends: useRef<HTMLElement>(null),
    mutualFriend: useRef<HTMLElement>(null),
    socialInitiative: useRef<HTMLElement>(null),
    peakDay: useRef<HTMLElement>(null),
    streak: useRef<HTMLElement>(null),
    heatmap: useRef<HTMLElement>(null),
    midnightKing: useRef<HTMLElement>(null),
    responseSpeed: useRef<HTMLElement>(null),
    topPhrases: useRef<HTMLElement>(null),
    ranking: useRef<HTMLElement>(null),
    sns: useRef<HTMLElement>(null),
    lostFriend: useRef<HTMLElement>(null),
    ending: useRef<HTMLElement>(null),
  }

  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const params = new URLSearchParams(window.location.hash.split('?')[1] || '')
    const yearParam = params.get('year')
    const parsedYear = yearParam ? parseInt(yearParam, 10) : new Date().getFullYear()
    const year = Number.isNaN(parsedYear) ? new Date().getFullYear() : parsedYear
    generateReport(year)
  }, [])

  const generateReport = async (year: number) => {
    setIsLoading(true)
    setError(null)
    setLoadingProgress(0)

    const removeProgressListener = window.electronAPI.annualReport.onProgress?.((payload: { status: string; progress: number }) => {
      setLoadingProgress(payload.progress)
      setLoadingStage(payload.status)
    })

    try {
      const result = await window.electronAPI.annualReport.generateReport(year)
      removeProgressListener?.()
      setLoadingProgress(100)
      setLoadingStage('完成')

      if (result.success && result.data) {
        setTimeout(() => {
          setReportData(result.data!)
          setIsLoading(false)
        }, 300)
      } else {
        setError(result.error || '生成报告失败')
        setIsLoading(false)
      }
    } catch (e) {
      removeProgressListener?.()
      setError(String(e))
      setIsLoading(false)
    }
  }

  const formatNumber = (num: number) => num.toLocaleString()

  const getMostActiveTime = (data: number[][]) => {
    let maxHour = 0, maxWeekday = 0, maxVal = 0
    data.forEach((row, w) => {
      row.forEach((val, h) => {
        if (val > maxVal) { maxVal = val; maxHour = h; maxWeekday = w }
      })
    })
    const weekdayNames = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
    return { weekday: weekdayNames[maxWeekday], hour: maxHour }
  }

  const formatTime = (seconds: number) => {
    if (seconds < 60) return `${seconds}秒`
    if (seconds < 3600) return `${Math.round(seconds / 60)}分钟`
    return `${Math.round(seconds / 3600)}小时`
  }

  const formatYearLabel = (value: number, withSuffix: boolean = true) => {
    if (value === 0) return '历史以来'
    return withSuffix ? `${value}年` : `${value}`
  }

  // 获取可用的板块列表
  const getAvailableSections = (): SectionInfo[] => {
    if (!reportData) return []
    const sections: SectionInfo[] = [
      { id: 'cover', name: '封面', ref: sectionRefs.cover },
      { id: 'overview', name: '年度概览', ref: sectionRefs.overview },
    ]
    if (reportData.coreFriends[0]) {
      sections.push({ id: 'bestFriend', name: '年度挚友', ref: sectionRefs.bestFriend })
    }
    sections.push({ id: 'monthlyFriends', name: '月度好友', ref: sectionRefs.monthlyFriends })
    if (reportData.mutualFriend) {
      sections.push({ id: 'mutualFriend', name: '双向奔赴', ref: sectionRefs.mutualFriend })
    }
    if (reportData.socialInitiative) {
      sections.push({ id: 'socialInitiative', name: '社交主动性', ref: sectionRefs.socialInitiative })
    }
    if (reportData.peakDay) {
      sections.push({ id: 'peakDay', name: '巅峰时刻', ref: sectionRefs.peakDay })
    }
    if (reportData.longestStreak) {
      sections.push({ id: 'streak', name: '聊天火花', ref: sectionRefs.streak })
    }
    sections.push({ id: 'heatmap', name: '作息规律', ref: sectionRefs.heatmap })
    if (reportData.midnightKing) {
      sections.push({ id: 'midnightKing', name: '深夜好友', ref: sectionRefs.midnightKing })
    }
    if (reportData.responseSpeed) {
      sections.push({ id: 'responseSpeed', name: '回应速度', ref: sectionRefs.responseSpeed })
    }
    if (reportData.lostFriend) {
      sections.push({ id: 'lostFriend', name: '曾经的好朋友', ref: sectionRefs.lostFriend })
    }
    if (reportData.topPhrases && reportData.topPhrases.length > 0) {
      sections.push({ id: 'topPhrases', name: '年度常用语', ref: sectionRefs.topPhrases })
    }
    sections.push({ id: 'ranking', name: '好友排行', ref: sectionRefs.ranking })
    if (reportData.snsStats && reportData.snsStats.totalPosts > 0) {
      sections.push({ id: 'sns', name: '朋友圈', ref: sectionRefs.sns })
    }
    sections.push({ id: 'ending', name: '尾声', ref: sectionRefs.ending })
    return sections
  }

  // 导出单个板块 - 统一 16:9 尺寸
  const exportSection = async (section: SectionInfo): Promise<{ name: string; data: string } | null> => {
    const element = section.ref.current
    if (!element) {
      return null
    }

    // 固定输出尺寸 1920x1080 (16:9)
    const OUTPUT_WIDTH = 1920
    const OUTPUT_HEIGHT = 1080

    try {
      const selection = window.getSelection()
      if (selection && selection.rangeCount > 0) selection.removeAllRanges()
      const activeEl = document.activeElement as HTMLElement | null
      activeEl?.blur?.()
      document.body.classList.add('exporting-snapshot')
      document.documentElement.classList.add('exporting-snapshot')

      const originalStyle = element.style.cssText
      element.style.minHeight = 'auto'
      element.style.padding = '40px 20px'
      element.style.background = 'transparent'
      element.style.backgroundColor = 'transparent'
      element.style.boxShadow = 'none'

      // 修复词云
      const wordCloudInner = element.querySelector('.word-cloud-inner') as HTMLElement
      const wordTags = element.querySelectorAll('.word-tag') as NodeListOf<HTMLElement>
      let wordCloudOriginalStyle = ''
      const wordTagOriginalStyles: string[] = []

      if (wordCloudInner) {
        wordCloudOriginalStyle = wordCloudInner.style.cssText
        wordCloudInner.style.transform = 'none'
      }

      wordTags.forEach((tag, i) => {
        wordTagOriginalStyles[i] = tag.style.cssText
        tag.style.opacity = String(tag.style.getPropertyValue('--final-opacity') || '1')
        tag.style.animation = 'none'
      })

      await new Promise(r => setTimeout(r, 50))

      const computedStyle = getComputedStyle(document.documentElement)
      const bgColor = computedStyle.getPropertyValue('--bg-primary').trim() || '#F9F8F6'

      const canvas = await html2canvas(element, {
        backgroundColor: 'transparent', // 透明背景，让 SVG 图案显示
        scale: 2,
        useCORS: true,
        allowTaint: true,
        logging: false,
        onclone: (clonedDoc) => {
          clonedDoc.body.classList.add('exporting-snapshot')
          clonedDoc.documentElement.classList.add('exporting-snapshot')
          clonedDoc.getSelection?.()?.removeAllRanges()
        },
      })

      // 恢复样式
      element.style.cssText = originalStyle
      if (wordCloudInner) {
        wordCloudInner.style.cssText = wordCloudOriginalStyle
      }
      wordTags.forEach((tag, i) => {
        tag.style.cssText = wordTagOriginalStyles[i]
      })
      document.body.classList.remove('exporting-snapshot')
      document.documentElement.classList.remove('exporting-snapshot')

      // 创建固定 16:9 尺寸的画布
      const outputCanvas = document.createElement('canvas')
      outputCanvas.width = OUTPUT_WIDTH
      outputCanvas.height = OUTPUT_HEIGHT
      const ctx = outputCanvas.getContext('2d')!

      // 绘制带 SVG 图案的背景
      const isDark = themeMode === 'dark'
      await drawPatternBackground(ctx, OUTPUT_WIDTH, OUTPUT_HEIGHT, bgColor, isDark)

      // 边距 (留出更多空白)
      const PADDING = 80
      const contentWidth = OUTPUT_WIDTH - PADDING * 2
      const contentHeight = OUTPUT_HEIGHT - PADDING * 2

      // 计算缩放和居中位置
      const srcRatio = canvas.width / canvas.height
      const dstRatio = contentWidth / contentHeight
      let drawWidth: number, drawHeight: number, drawX: number, drawY: number

      if (srcRatio > dstRatio) {
        // 源图更宽，以宽度为准
        drawWidth = contentWidth
        drawHeight = contentWidth / srcRatio
        drawX = PADDING
        drawY = PADDING + (contentHeight - drawHeight) / 2
      } else {
        // 源图更高，以高度为准
        drawHeight = contentHeight
        drawWidth = contentHeight * srcRatio
        drawX = PADDING + (contentWidth - drawWidth) / 2
        drawY = PADDING
      }

      ctx.drawImage(canvas, drawX, drawY, drawWidth, drawHeight)

      return { name: section.name, data: outputCanvas.toDataURL('image/png') }
    } catch (e) {
      document.body.classList.remove('exporting-snapshot')
      return null
    }
  }

  // 导出整个报告为长图
  const exportFullReport = async (filterIds?: Set<string>) => {
    if (!containerRef.current) {
      return
    }
    setIsExporting(true)
    setExportProgress('正在生成长图...')

    try {
      const selection = window.getSelection()
      if (selection && selection.rangeCount > 0) selection.removeAllRanges()
      const activeEl = document.activeElement as HTMLElement | null
      activeEl?.blur?.()
      document.body.classList.add('exporting-snapshot')
      document.documentElement.classList.add('exporting-snapshot')

      const container = containerRef.current
      const sections = container.querySelectorAll('.section')
      const originalStyles: string[] = []

      sections.forEach((section, i) => {
        const el = section as HTMLElement
        originalStyles[i] = el.style.cssText
        el.style.minHeight = 'auto'
        el.style.padding = '40px 0'
      })

      // 如果有筛选，隐藏未选中的板块
      if (filterIds) {
        const available = getAvailableSections()
        available.forEach(s => {
          if (!filterIds.has(s.id) && s.ref.current) {
            s.ref.current.style.display = 'none'
          }
        })
      }

      // 修复词云导出问题
      const wordCloudInner = container.querySelector('.word-cloud-inner') as HTMLElement
      const wordTags = container.querySelectorAll('.word-tag') as NodeListOf<HTMLElement>
      let wordCloudOriginalStyle = ''
      const wordTagOriginalStyles: string[] = []

      if (wordCloudInner) {
        wordCloudOriginalStyle = wordCloudInner.style.cssText
        wordCloudInner.style.transform = 'none'
      }

      wordTags.forEach((tag, i) => {
        wordTagOriginalStyles[i] = tag.style.cssText
        tag.style.opacity = String(tag.style.getPropertyValue('--final-opacity') || '1')
        tag.style.animation = 'none'
      })

      // 等待样式生效
      await new Promise(r => setTimeout(r, 100))

      // 获取计算后的背景色
      const computedStyle = getComputedStyle(document.documentElement)
      const bgColor = computedStyle.getPropertyValue('--bg-primary').trim() || '#F9F8F6'

      const canvas = await html2canvas(container, {
        backgroundColor: 'transparent', // 透明背景
        scale: 2,
        useCORS: true,
        allowTaint: true,
        logging: false,
        onclone: (clonedDoc) => {
          clonedDoc.body.classList.add('exporting-snapshot')
          clonedDoc.documentElement.classList.add('exporting-snapshot')
          clonedDoc.getSelection?.()?.removeAllRanges()
        },
      })

      // 恢复原始样式
      sections.forEach((section, i) => {
        const el = section as HTMLElement
        el.style.cssText = originalStyles[i]
      })

      if (wordCloudInner) {
        wordCloudInner.style.cssText = wordCloudOriginalStyle
      }

      wordTags.forEach((tag, i) => {
        tag.style.cssText = wordTagOriginalStyles[i]
      })
      document.body.classList.remove('exporting-snapshot')
      document.documentElement.classList.remove('exporting-snapshot')

      // 创建带 SVG 图案背景的输出画布
      const outputCanvas = document.createElement('canvas')
      outputCanvas.width = canvas.width
      outputCanvas.height = canvas.height
      const ctx = outputCanvas.getContext('2d')!

      // 绘制 SVG 图案背景
      const isDark = themeMode === 'dark'
      await drawPatternBackground(ctx, canvas.width, canvas.height, bgColor, isDark)

      // 绘制内容
      ctx.drawImage(canvas, 0, 0)

      const dataUrl = outputCanvas.toDataURL('image/png')
      const link = document.createElement('a')
      const yearFilePrefix = reportData ? formatYearLabel(reportData.year, false) : ''
      link.download = `${yearFilePrefix}年度报告${filterIds ? '_自定义' : ''}.png`
      link.href = dataUrl
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
    } catch (e) {
      alert('导出失败: ' + String(e))
    } finally {
      document.body.classList.remove('exporting-snapshot')
      document.documentElement.classList.remove('exporting-snapshot')
      setIsExporting(false)
      setExportProgress('')
    }
  }

  // 导出选中的板块
  const exportSelectedSections = async () => {
    const sections = getAvailableSections().filter(s => selectedSections.has(s.id))
    if (sections.length === 0) {
      alert('请至少选择一个板块')
      return
    }

    if (exportMode === 'long') {
      setShowExportModal(false)
      await exportFullReport(selectedSections)
      setSelectedSections(new Set())
      return
    }

    setIsExporting(true)
    setShowExportModal(false)

    const exportedImages: { name: string; data: string }[] = []

    for (let i = 0; i < sections.length; i++) {
      const section = sections[i]
      setExportProgress(`正在导出: ${section.name} (${i + 1}/${sections.length})`)

      const result = await exportSection(section)
      if (result) {
        exportedImages.push(result)
      }
    }

    if (exportedImages.length === 0) {
      alert('导出失败')
      setIsExporting(false)
      setExportProgress('')
      return
    }

    const dirResult = await window.electronAPI.dialog.openDirectory({
      title: '选择导出文件夹',
      properties: ['openDirectory', 'createDirectory']
    })
    if (dirResult.canceled || !dirResult.filePaths?.[0]) {
      setIsExporting(false)
      setExportProgress('')
      return
    }

    setExportProgress('正在写入文件...')
    const yearFilePrefix = reportData ? formatYearLabel(reportData.year, false) : ''
    const exportResult = await window.electronAPI.annualReport.exportImages({
      baseDir: dirResult.filePaths[0],
      folderName: `${yearFilePrefix}年度报告_分模块`,
      images: exportedImages.map((img) => ({
        name: `${yearFilePrefix}年度报告_${img.name}.png`,
        dataUrl: img.data
      }))
    })

    if (!exportResult.success) {
      alert('导出失败: ' + (exportResult.error || '未知错误'))
    }

    setIsExporting(false)
    setExportProgress('')
    setSelectedSections(new Set())
  }

  // 切换板块选择
  const toggleSection = (id: string) => {
    const newSet = new Set(selectedSections)
    if (newSet.has(id)) {
      newSet.delete(id)
    } else {
      newSet.add(id)
    }
    setSelectedSections(newSet)
  }

  // 全选/取消全选
  const toggleAll = () => {
    const sections = getAvailableSections()
    if (selectedSections.size === sections.length) {
      setSelectedSections(new Set())
    } else {
      setSelectedSections(new Set(sections.map(s => s.id)))
    }
  }

  if (isLoading) {
    return (
      <div className="annual-report-window loading">
        <div className="loading-ring">
          <svg viewBox="0 0 100 100">
            <circle className="ring-bg" cx="50" cy="50" r="42" />
            <circle
              className="ring-progress"
              cx="50" cy="50" r="42"
              style={{ strokeDashoffset: 264 - (264 * loadingProgress / 100) }}
            />
          </svg>
          <span className="ring-text">{loadingProgress}%</span>
        </div>
        <p className="loading-stage">{loadingStage}</p>
        <p className="loading-hint">进行中</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="annual-report-window error">
        <p>生成报告失败: {error}</p>
      </div>
    )
  }

  if (!reportData) {
    return (
      <div className="annual-report-window error">
        <p>暂无数据</p>
      </div>
    )
  }

  const { year, totalMessages, totalFriends, coreFriends, monthlyTopFriends, peakDay, longestStreak, activityHeatmap, midnightKing, selfAvatarUrl, mutualFriend, socialInitiative, responseSpeed, topPhrases, lostFriend } = reportData
  const topFriend = coreFriends[0]
  const mostActive = getMostActiveTime(activityHeatmap.data)
  const socialStoryName = topFriend?.displayName || '好友'
  const yearTitle = formatYearLabel(year, true)
  const yearTitleShort = formatYearLabel(year, false)
  const monthlyTitle = year === 0 ? '历史以来月度好友' : `${year}年月度好友`
  const phrasesTitle = year === 0 ? '你在历史以来的常用语' : `你在${year}年的年度常用语`

  return (
    <div className="annual-report-window">
      <div className="drag-region" />

      {/* 背景装饰 */}
      <div className="bg-decoration">
        <div className="deco-circle c1" />
        <div className="deco-circle c2" />
        <div className="deco-circle c3" />
        <div className="deco-circle c4" />
        <div className="deco-circle c5" />
      </div>

      {/* 浮动操作按钮 */}
      <div className={`fab-container ${fabOpen ? 'open' : ''}`}>
        <button className="fab-item" onClick={() => { setFabOpen(false); setExportMode('separate'); setShowExportModal(true) }} title="分模块导出">
          <Image size={18} />
        </button>
        <button className="fab-item" onClick={() => { setFabOpen(false); setExportMode('long'); setShowExportModal(true) }} title="自定义导出长图">
          <SlidersHorizontal size={18} />
        </button>
        <button className="fab-item" onClick={() => { setFabOpen(false); exportFullReport() }} title="导出长图">
          <Download size={18} />
        </button>
        <button className="fab-main" onClick={() => setFabOpen(!fabOpen)}>
          {fabOpen ? <X size={22} /> : <Download size={22} />}
        </button>
      </div>

      {/* 导出进度 */}
      {isExporting && (
        <div className="export-overlay">
          <div className="export-progress-modal">
            <div className="export-spinner">
              <div className="spinner-ring"></div>
              <Download size={24} className="spinner-icon" />
            </div>
            <p className="export-title">正在导出</p>
            <p className="export-status">{exportProgress}</p>
          </div>
        </div>
      )}

      {/* 模块选择弹窗 */}
      {showExportModal && (
        <div className="export-overlay" onClick={() => setShowExportModal(false)}>
          <div className="export-modal section-selector" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h3>{exportMode === 'long' ? '自定义导出长图' : '选择要导出的板块'}</h3>
              <button className="close-btn" onClick={() => setShowExportModal(false)}>
                <X size={20} />
              </button>
            </div>
            <div className="section-grid">
              {getAvailableSections().map(section => (
                <div
                  key={section.id}
                  className={`section-card ${selectedSections.has(section.id) ? 'selected' : ''}`}
                  onClick={() => toggleSection(section.id)}
                >
                  <div className="card-check">
                    {selectedSections.has(section.id) && <Check size={14} />}
                  </div>
                  <span>{section.name}</span>
                </div>
              ))}
            </div>
            <div className="modal-footer">
              <button className="select-all-btn" onClick={toggleAll}>
                {selectedSections.size === getAvailableSections().length ? '取消全选' : '全选'}
              </button>
              <button
                className="confirm-btn"
                onClick={exportSelectedSections}
                disabled={selectedSections.size === 0}
              >
                {exportMode === 'long' ? '生成长图' : '导出'} {selectedSections.size > 0 ? `(${selectedSections.size})` : ''}
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="report-scroll-view">
        <div className="report-container" ref={containerRef}>

          {/* 封面 */}
          <section className="section" ref={sectionRefs.cover}>
            <div className="label-text">WEFLOW · ANNUAL REPORT</div>
            <h1 className="hero-title">{yearTitle}<br />微信聊天报告</h1>
            <hr className="divider" />
            <p className="hero-desc">每一条消息背后<br />都藏着一段独特的故事</p>
          </section>

          {/* 年度概览 */}
          <section className="section" ref={sectionRefs.overview}>
            <div className="label-text">年度概览</div>
            <h2 className="hero-title">你和你的朋友们<br />互相发过</h2>
            <div className="big-stat">
              <span className="stat-num">{formatNumber(totalMessages)}</span>
              <span className="stat-unit">条消息</span>
            </div>
            <p className="hero-desc">
              在这段时光里，你与 <span className="hl">{formatNumber(totalFriends)}</span> 位好友交换过喜怒哀乐。
              <br />每一个对话，都是一段故事的开始。
            </p>
          </section>

          {/* 年度挚友 */}
          {topFriend && (
            <section className="section" ref={sectionRefs.bestFriend}>
              <div className="label-text">年度挚友</div>
              <h2 className="hero-title">{topFriend.displayName}</h2>
              <div className="big-stat">
                <span className="stat-num">{formatNumber(topFriend.messageCount)}</span>
                <span className="stat-unit">条消息</span>
              </div>
              <p className="hero-desc">
                你发出 <span className="hl">{formatNumber(topFriend.sentCount)}</span> 条 ·
                TA发来 <span className="hl">{formatNumber(topFriend.receivedCount)}</span> 条
              </p>
              <br />
              <p className="hero-desc">
                在一起，就可以
              </p>
            </section>
          )}

          {/* 月度好友 */}
          <section className="section" ref={sectionRefs.monthlyFriends}>
            <div className="label-text">月度好友</div>
            <h2 className="hero-title">{monthlyTitle}</h2>
            <p className="hero-desc">根据12个月的聊天习惯</p>
            <div className="monthly-orbit">
              {monthlyTopFriends.map((m, i) => (
                <div key={m.month} className="monthly-item" style={{ '--i': i } as React.CSSProperties}>
                  <div className="month-label">{m.month}月</div>
                  <Avatar url={m.avatarUrl} name={m.displayName} size="sm" />
                  <div className="month-name">{m.displayName}</div>
                </div>
              ))}
              <div className="monthly-center">
                <Avatar url={selfAvatarUrl} name="我" size="lg" />
              </div>
            </div>
            <p className="hero-desc">你只管说<br />我一直在</p>
          </section>

          {/* 双向奔赴 */}
          {mutualFriend && (
            <section className="section" ref={sectionRefs.mutualFriend}>
              <div className="label-text">双向奔赴</div>
              <h2 className="hero-title">默契与平衡</h2>
              <div className="mutual-visual">
                <div className="mutual-side you">
                  <Avatar url={selfAvatarUrl} name="我" size="lg" />
                  <div className="mutual-arrow">
                    <span className="arrow-count">{formatNumber(mutualFriend.sentCount)}</span>
                    <div className="arrow-line">→</div>
                  </div>
                </div>
                <div className="mutual-center">
                  <div className="mutual-icon">🤝</div>
                  <div className="mutual-ratio">{mutualFriend.ratio}</div>
                </div>
                <div className="mutual-side friend">
                  <div className="mutual-arrow reverse">
                    <span className="arrow-count">{formatNumber(mutualFriend.receivedCount)}</span>
                    <div className="arrow-line">←</div>
                  </div>
                  <Avatar url={mutualFriend.avatarUrl} name={mutualFriend.displayName} size="lg" />
                </div>
              </div>
              <div className="mutual-name-tag">{mutualFriend.displayName}</div>
              <p className="hero-desc">
                你们的互动比例接近 <span className="hl">{mutualFriend.ratio}</span>。
                <br />你来我往，势均力敌。
              </p>
            </section>
          )}

          {/* 社交主动性 */}
          {socialInitiative && (
            <section className="section" ref={sectionRefs.socialInitiative}>
              <div className="label-text">社交主动性</div>
              <h2 className="hero-title">主动才有故事</h2>
              <div className="big-stat">
                <span className="stat-num">{socialInitiative.initiativeRate}%</span>
                <span className="stat-unit">的对话由你发起</span>
              </div>
              <p className="hero-desc">
                面对 <span className="hl">{socialStoryName}</span> 的时候，你总是那个先开口的人。
              </p>
            </section>
          )}

          {/* 巅峰时刻 */}
          {peakDay && (
            <section className="section" ref={sectionRefs.peakDay}>
              <div className="label-text">巅峰时刻</div>
              <h2 className="hero-title">{peakDay.date}</h2>
              <p className="hero-desc">一天里你一共发了</p>
              <div className="big-stat">
                <span className="stat-num">{formatNumber(peakDay.messageCount)}</span>
                <span className="stat-unit">条消息</span>
              </div>
              <p className="hero-desc">
                在这个快节奏的世界，有人正陪在你身边听你慢慢地讲
                <br />那天，你和 <span className="hl">{peakDay.topFriend || '好友'}</span> 的 {formatNumber(peakDay.topFriendCount || 0)} 条消息见证着这一切
                <br />有些话，只想对你说
              </p>
            </section>
          )}

          {/* 聊天火花 */}
          {longestStreak && (
            <section className="section" ref={sectionRefs.streak}>
              <div className="label-text">持之以恒</div>
              <h2 className="hero-title">聊天火花</h2>
              <p className="hero-desc">与 <span className="hl">{longestStreak.friendName}</span> 持续了</p>
              <div className="big-stat">
                <span className="stat-num">{longestStreak.days}</span>
                <span className="stat-unit">天</span>
              </div>
              <p className="hero-desc">
                从 {longestStreak.startDate} 到 {longestStreak.endDate}
              </p>
              <p className="hero-desc">陪伴，是最长情的告白</p>
            </section>
          )}

          {/* 作息规律 */}
          <section className="section" ref={sectionRefs.heatmap}>
            <div className="label-text">作息规律</div>
            <h2 className="hero-title">时间的痕迹</h2>
            <p className="hero-desc active-time">
              在 <span className="hl">{mostActive.weekday} {String(mostActive.hour).padStart(2, '0')}:00</span> 最活跃
            </p>
            <Heatmap data={activityHeatmap.data} />
          </section>

          {/* 深夜好友 */}
          {midnightKing && (
            <section className="section" ref={sectionRefs.midnightKing}>
              <div className="label-text">深夜好友</div>
              <h2 className="hero-title">月光下的你</h2>
              <p className="hero-desc">在这一年你留下了</p>
              <div className="big-stat">
                <span className="stat-num">{midnightKing.count}</span>
                <span className="stat-unit">条深夜的消息</span>
              </div>
              <p className="hero-desc">
                其中 <span className="hl">{midnightKing.displayName}</span> 常常在深夜中陪着你胡思乱想。
                <br />你和Ta的对话占你深夜期间聊天的 <span className="gold">{midnightKing.percentage}%</span>。
              </p>
            </section>
          )}

          {/* 回应速度 */}
          {responseSpeed && (
            <section className="section" ref={sectionRefs.responseSpeed}>
              <div className="label-text">回应速度</div>
              <h2 className="hero-title">念念不忘，必有回响</h2>
              <div className="big-stat">
                <span className="stat-num">{formatTime(responseSpeed.avgResponseTime)}</span>
                <span className="stat-unit">是你的平均回复时间</span>
              </div>
              <p className="hero-desc">
                你回复 <span className="hl">{responseSpeed.fastestFriend}</span> 最快
                <br />平均只需 <span className="gold">{formatTime(responseSpeed.fastestTime)}</span>
              </p>
            </section>
          )}

          {/* 曾经的好朋友 */}
          {lostFriend && (
            <section className="section" ref={sectionRefs.lostFriend}>
              <div className="label-text">曾经的好朋友</div>
              <h2 className="hero-title">{lostFriend.displayName}</h2>
              <div className="big-stat">
                <span className="stat-num">{formatNumber(lostFriend.earlyCount)}</span>
                <span className="stat-unit">条消息</span>
              </div>
              <p className="hero-desc">
                在 <span className="hl">{lostFriend.periodDesc}</span>
                <br />你们曾有聊不完的话题
              </p>
              <div className="lost-friend-visual">
                <div className="avatar-group sender">
                  <Avatar url={lostFriend.avatarUrl} name={lostFriend.displayName} size="lg" />
                  <span className="avatar-label">TA</span>
                </div>
                <div className="fading-line">
                  <div className="line-path" />
                  <div className="line-glow" />
                  <div className="flow-particle" />
                </div>
                <div className="avatar-group receiver">
                  <Avatar url={selfAvatarUrl} name="我" size="lg" />
                  <span className="avatar-label">我</span>
                </div>
              </div>
              <p className="hero-desc fading">
                人类发明后悔
                <br />来证明拥有的珍贵
              </p>
            </section>
          )}

          {/* 年度常用语 - 词云 */}
          {topPhrases && topPhrases.length > 0 && (
            <section className="section" ref={sectionRefs.topPhrases}>
              <div className="label-text">年度常用语</div>
              <h2 className="hero-title">{phrasesTitle}</h2>
              <p className="hero-desc">
                这一年，你说得最多的是：
                <br />
                <span className="hl" style={{ fontSize: '20px' }}>
                  {topPhrases.slice(0, 3).map(p => p.phrase).join('、')}
                </span>
              </p>
              <WordCloud words={topPhrases} />
              <p className="hero-desc word-cloud-note">颜色越深代表出现频率越高</p>
            </section>
          )}

          {/* 朋友圈 */}
          {reportData.snsStats && reportData.snsStats.totalPosts > 0 && (
            <section className="section" ref={sectionRefs.sns}>
              <div className="label-text">朋友圈</div>
              <h2 className="hero-title">记录生活时刻</h2>
              <p className="hero-desc">
                这一年，你发布了
              </p>
              <div className="big-stat">
                <span className="stat-num">{reportData.snsStats.totalPosts}</span>
                <span className="stat-unit">条朋友圈</span>
              </div>

              <div className="sns-stats-container" style={{ display: 'flex', gap: '60px', marginTop: '40px', justifyContent: 'center' }}>
                {reportData.snsStats.topLikers.length > 0 && (
                  <div className="sns-sub-stat" style={{ textAlign: 'left' }}>
                    <h3 className="sub-title" style={{ fontSize: '18px', marginBottom: '16px', opacity: 0.8, borderBottom: '1px solid currentColor', paddingBottom: '8px' }}>更关心你的Ta</h3>
                    <div className="mini-ranking">
                      {reportData.snsStats.topLikers.slice(0, 3).map((u, i) => (
                        <div key={i} className="mini-rank-item" style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '14px' }}>
                          <Avatar url={u.avatarUrl} name={u.displayName} size="sm" />
                          <div style={{ display: 'flex', flexDirection: 'column' }}>
                            <span className="name" style={{ fontSize: '15px', fontWeight: 500, maxWidth: '120px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{u.displayName}</span>
                          </div>
                          <span className="count hl" style={{ fontSize: '14px', marginLeft: 'auto' }}>{u.count}赞</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {reportData.snsStats.topLiked.length > 0 && (
                  <div className="sns-sub-stat" style={{ textAlign: 'left' }}>
                    <h3 className="sub-title" style={{ fontSize: '18px', marginBottom: '16px', opacity: 0.8, borderBottom: '1px solid currentColor', paddingBottom: '8px' }}>你最关心的Ta</h3>
                    <div className="mini-ranking">
                      {reportData.snsStats.topLiked.slice(0, 3).map((u, i) => (
                        <div key={i} className="mini-rank-item" style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '14px' }}>
                          <Avatar url={u.avatarUrl} name={u.displayName} size="sm" />
                          <div style={{ display: 'flex', flexDirection: 'column' }}>
                            <span className="name" style={{ fontSize: '15px', fontWeight: 500, maxWidth: '120px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{u.displayName}</span>
                          </div>
                          <span className="count hl" style={{ fontSize: '14px', marginLeft: 'auto' }}>{u.count}赞</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </section>
          )}

          {/* 好友排行 */}
          <section className="section" ref={sectionRefs.ranking}>
            <div className="label-text">好友排行</div>
            <h2 className="hero-title">聊得最多的人</h2>

            {/* 领奖台 - 前三名 */}
            <div className="podium">
              {/* 第二名 - 左边 */}
              {coreFriends[1] && (
                <div className="podium-item second">
                  <Avatar url={coreFriends[1].avatarUrl} name={coreFriends[1].displayName} size="lg" />
                  <div className="podium-name">{coreFriends[1].displayName}</div>
                  <div className="podium-count">{formatNumber(coreFriends[1].messageCount)} 条</div>
                  <div className="podium-stand">
                    <span className="podium-rank">2</span>
                  </div>
                </div>
              )}

              {/* 第一名 - 中间最高 */}
              {coreFriends[0] && (
                <div className="podium-item first">
                  <div className="crown">👑</div>
                  <Avatar url={coreFriends[0].avatarUrl} name={coreFriends[0].displayName} size="lg" />
                  <div className="podium-name">{coreFriends[0].displayName}</div>
                  <div className="podium-count">{formatNumber(coreFriends[0].messageCount)} 条</div>
                  <div className="podium-stand">
                    <span className="podium-rank">1</span>
                  </div>
                </div>
              )}

              {/* 第三名 - 右边 */}
              {coreFriends[2] && (
                <div className="podium-item third">
                  <Avatar url={coreFriends[2].avatarUrl} name={coreFriends[2].displayName} size="lg" />
                  <div className="podium-name">{coreFriends[2].displayName}</div>
                  <div className="podium-count">{formatNumber(coreFriends[2].messageCount)} 条</div>
                  <div className="podium-stand">
                    <span className="podium-rank">3</span>
                  </div>
                </div>
              )}
            </div>
          </section>

          {/* 结尾 */}
          <section className="section ending" ref={sectionRefs.ending}>
            <h2 className="hero-title">尾声</h2>
            <p className="hero-desc">
              我们总是在向前走
              <br />却很少有机会回头看看
              <br />如果这份报告让你有所触动，不妨把它分享给你在意的人
              <br />愿新的一年，
              <br />所有期待，皆有回声。
            </p>
            <div className="ending-year">{yearTitleShort}</div>
            <div className="ending-brand">WEFLOW</div>
          </section>
        </div>
      </div>
    </div>
  )
}

export default AnnualReportWindow
