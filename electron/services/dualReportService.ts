import { parentPort } from 'worker_threads'
import { wcdbService } from './wcdbService'

export interface DualReportMessage {
  content: string
  isSentByMe: boolean
  createTime: number
  createTimeStr: string
}

export interface DualReportFirstChat {
  createTime: number
  createTimeStr: string
  content: string
  isSentByMe: boolean
  senderUsername?: string
}

export interface DualReportStats {
  totalMessages: number
  totalWords: number
  imageCount: number
  voiceCount: number
  emojiCount: number
  myTopEmojiMd5?: string
  friendTopEmojiMd5?: string
  myTopEmojiUrl?: string
  friendTopEmojiUrl?: string
  myTopEmojiCount?: number
  friendTopEmojiCount?: number
}

export interface DualReportData {
  year: number
  selfName: string
  selfAvatarUrl?: string
  friendUsername: string
  friendName: string
  friendAvatarUrl?: string
  firstChat: DualReportFirstChat | null
  firstChatMessages?: DualReportMessage[]
  yearFirstChat?: {
    createTime: number
    createTimeStr: string
    content: string
    isSentByMe: boolean
    friendName: string
    firstThreeMessages: DualReportMessage[]
  } | null
  stats: DualReportStats
  topPhrases: Array<{ phrase: string; count: number }>
  heatmap?: number[][]
  initiative?: { initiated: number; received: number }
  response?: { avg: number; fastest: number; count: number }
  monthly?: Record<string, number>
  streak?: { days: number; startDate: string; endDate: string }
}

class DualReportService {
  private broadcastProgress(status: string, progress: number) {
    if (parentPort) {
      parentPort.postMessage({
        type: 'dualReport:progress',
        data: { status, progress }
      })
    }
  }

  private reportProgress(status: string, progress: number, onProgress?: (status: string, progress: number) => void) {
    if (onProgress) {
      onProgress(status, progress)
      return
    }
    this.broadcastProgress(status, progress)
  }

  private cleanAccountDirName(dirName: string): string {
    const trimmed = dirName.trim()
    if (!trimmed) return trimmed
    if (trimmed.toLowerCase().startsWith('wxid_')) {
      const match = trimmed.match(/^(wxid_[^_]+)/i)
      if (match) return match[1]
      return trimmed
    }
    const suffixMatch = trimmed.match(/^(.+)_([a-zA-Z0-9]{4})$/)
    const cleaned = suffixMatch ? suffixMatch[1] : trimmed

    return cleaned
  }

  private async ensureConnectedWithConfig(
    dbPath: string,
    decryptKey: string,
    wxid: string
  ): Promise<{ success: boolean; cleanedWxid?: string; rawWxid?: string; error?: string }> {
    if (!wxid) return { success: false, error: '未配置微信ID' }
    if (!dbPath) return { success: false, error: '未配置数据库路径' }
    if (!decryptKey) return { success: false, error: '未配置解密密钥' }

    const cleanedWxid = this.cleanAccountDirName(wxid)
    const ok = await wcdbService.open(dbPath, decryptKey, cleanedWxid)
    if (!ok) return { success: false, error: 'WCDB 打开失败' }
    return { success: true, cleanedWxid, rawWxid: wxid }
  }

  private decodeMessageContent(messageContent: any, compressContent: any): string {
    let content = this.decodeMaybeCompressed(compressContent)
    if (!content || content.length === 0) {
      content = this.decodeMaybeCompressed(messageContent)
    }
    return content
  }

  private decodeMaybeCompressed(raw: any): string {
    if (!raw) return ''
    if (typeof raw === 'string') {
      if (raw.length === 0) return ''
      // 只有当字符串足够长（超过16字符）且看起来像 hex 时才尝试解码
      // 短字符串（如 "123456" 等纯数字）容易被误判为 hex
      if (raw.length > 16 && this.looksLikeHex(raw)) {
        const bytes = Buffer.from(raw, 'hex')
        if (bytes.length > 0) return this.decodeBinaryContent(bytes)
      }
      // 只有当字符串足够长（超过16字符）且看起来像 base64 时才尝试解码
      // 短字符串（如 "test", "home" 等）容易被误判为 base64
      if (raw.length > 16 && this.looksLikeBase64(raw)) {
        try {
          const bytes = Buffer.from(raw, 'base64')
          return this.decodeBinaryContent(bytes)
        } catch {
          return raw
        }
      }
      return raw
    }
    return ''
  }

  private decodeBinaryContent(data: Buffer): string {
    if (data.length === 0) return ''
    try {
      if (data.length >= 4) {
        const magic = data.readUInt32LE(0)
        if (magic === 0xFD2FB528) {
          const fzstd = require('fzstd')
          const decompressed = fzstd.decompress(data)
          return Buffer.from(decompressed).toString('utf-8')
        }
      }
      const decoded = data.toString('utf-8')
      const replacementCount = (decoded.match(/\uFFFD/g) || []).length
      if (replacementCount < decoded.length * 0.2) {
        return decoded.replace(/\uFFFD/g, '')
      }
      return data.toString('latin1')
    } catch {
      return ''
    }
  }

  private looksLikeHex(s: string): boolean {
    if (s.length % 2 !== 0) return false
    return /^[0-9a-fA-F]+$/.test(s)
  }

  private looksLikeBase64(s: string): boolean {
    if (s.length % 4 !== 0) return false
    return /^[A-Za-z0-9+/=]+$/.test(s)
  }

  private formatDateTime(milliseconds: number): string {
    const dt = new Date(milliseconds)
    const month = String(dt.getMonth() + 1).padStart(2, '0')
    const day = String(dt.getDate()).padStart(2, '0')
    const hour = String(dt.getHours()).padStart(2, '0')
    const minute = String(dt.getMinutes()).padStart(2, '0')
    return `${month}/${day} ${hour}:${minute}`
  }

  private extractEmojiUrl(content: string): string | undefined {
    if (!content) return undefined
    const attrMatch = /cdnurl\s*=\s*['"]([^'"]+)['"]/i.exec(content)
    if (attrMatch) {
      let url = attrMatch[1].replace(/&amp;/g, '&')
      try {
        if (url.includes('%')) {
          url = decodeURIComponent(url)
        }
      } catch { }
      return url
    }
    const tagMatch = /cdnurl[^>]*>([^<]+)/i.exec(content)
    return tagMatch?.[1]
  }

  private extractEmojiMd5(content: string): string | undefined {
    if (!content) return undefined
    const match = /md5="([^"]+)"/i.exec(content) || /<md5>([^<]+)<\/md5>/i.exec(content)
    return match?.[1]
  }

  private async getDisplayName(username: string, fallback: string): Promise<string> {
    const result = await wcdbService.getDisplayNames([username])
    if (result.success && result.map) {
      return result.map[username] || fallback
    }
    return fallback
  }

  private resolveIsSent(row: any, rawWxid?: string, cleanedWxid?: string): boolean {
    const isSendRaw = row.computed_is_send ?? row.is_send
    if (isSendRaw !== undefined && isSendRaw !== null) {
      return parseInt(isSendRaw, 10) === 1
    }
    const sender = String(row.sender_username || row.sender || row.talker || '').toLowerCase()
    if (!sender) return false
    const rawLower = rawWxid ? rawWxid.toLowerCase() : ''
    const cleanedLower = cleanedWxid ? cleanedWxid.toLowerCase() : ''
    return !!(
      sender === rawLower ||
      sender === cleanedLower ||
      (rawLower && rawLower.startsWith(sender + '_')) ||
      (cleanedLower && cleanedLower.startsWith(sender + '_'))
    )
  }

  private async getFirstMessages(
    sessionId: string,
    limit: number,
    beginTimestamp: number,
    endTimestamp: number
  ): Promise<any[]> {
    const safeBegin = Math.max(0, beginTimestamp || 0)
    const safeEnd = endTimestamp && endTimestamp > 0 ? endTimestamp : Math.floor(Date.now() / 1000)
    const cursorResult = await wcdbService.openMessageCursor(sessionId, Math.max(1, limit), true, safeBegin, safeEnd)
    if (!cursorResult.success || !cursorResult.cursor) return []
    try {
      const rows: any[] = []
      let hasMore = true
      while (hasMore && rows.length < limit) {
        const batch = await wcdbService.fetchMessageBatch(cursorResult.cursor)
        if (!batch.success || !batch.rows) break
        for (const row of batch.rows) {
          rows.push(row)
          if (rows.length >= limit) break
        }
        hasMore = batch.hasMore === true
      }
      return rows.slice(0, limit)
    } finally {
      await wcdbService.closeMessageCursor(cursorResult.cursor)
    }
  }

  async generateReportWithConfig(params: {
    year: number
    friendUsername: string
    dbPath: string
    decryptKey: string
    wxid: string
    onProgress?: (status: string, progress: number) => void
  }): Promise<{ success: boolean; data?: DualReportData; error?: string }> {
    try {
      const { year, friendUsername, dbPath, decryptKey, wxid, onProgress } = params
      this.reportProgress('正在连接数据库...', 5, onProgress)
      const conn = await this.ensureConnectedWithConfig(dbPath, decryptKey, wxid)
      if (!conn.success || !conn.cleanedWxid || !conn.rawWxid) return { success: false, error: conn.error }

      const cleanedWxid = conn.cleanedWxid
      const rawWxid = conn.rawWxid

      const reportYear = year <= 0 ? 0 : year
      const isAllTime = reportYear === 0
      const startTime = isAllTime ? 0 : Math.floor(new Date(reportYear, 0, 1).getTime() / 1000)
      const endTime = isAllTime ? 0 : Math.floor(new Date(reportYear, 11, 31, 23, 59, 59).getTime() / 1000)

      this.reportProgress('加载联系人信息...', 10, onProgress)
      const friendName = await this.getDisplayName(friendUsername, friendUsername)
      let myName = await this.getDisplayName(rawWxid, rawWxid)
      if (myName === rawWxid && cleanedWxid && cleanedWxid !== rawWxid) {
        myName = await this.getDisplayName(cleanedWxid, rawWxid)
      }
      const avatarCandidates = Array.from(new Set([
        friendUsername,
        rawWxid,
        cleanedWxid
      ].filter(Boolean) as string[]))
      let selfAvatarUrl: string | undefined
      let friendAvatarUrl: string | undefined
      const avatarResult = await wcdbService.getAvatarUrls(avatarCandidates)
      if (avatarResult.success && avatarResult.map) {
        selfAvatarUrl = avatarResult.map[rawWxid] || avatarResult.map[cleanedWxid]
        friendAvatarUrl = avatarResult.map[friendUsername]
      }

      this.reportProgress('获取首条聊天记录...', 15, onProgress)
      const firstRows = await this.getFirstMessages(friendUsername, 3, 0, 0)
      let firstChat: DualReportFirstChat | null = null
      if (firstRows.length > 0) {
        const row = firstRows[0]
        const createTime = parseInt(row.create_time || '0', 10) * 1000
        const content = this.decodeMessageContent(row.message_content, row.compress_content)
        firstChat = {
          createTime,
          createTimeStr: this.formatDateTime(createTime),
          content: String(content || ''),
          isSentByMe: this.resolveIsSent(row, rawWxid, cleanedWxid),
          senderUsername: row.sender_username || row.sender
        }
      }
      const firstChatMessages: DualReportMessage[] = firstRows.map((row) => {
        const msgTime = parseInt(row.create_time || '0', 10) * 1000
        const msgContent = this.decodeMessageContent(row.message_content, row.compress_content)
        return {
          content: String(msgContent || ''),
          isSentByMe: this.resolveIsSent(row, rawWxid, cleanedWxid),
          createTime: msgTime,
          createTimeStr: this.formatDateTime(msgTime)
        }
      })

      let yearFirstChat: DualReportData['yearFirstChat'] = null
      if (!isAllTime) {
        this.reportProgress('获取今年首次聊天...', 20, onProgress)
        const firstYearRows = await this.getFirstMessages(friendUsername, 3, startTime, endTime)
        if (firstYearRows.length > 0) {
          const firstRow = firstYearRows[0]
          const createTime = parseInt(firstRow.create_time || '0', 10) * 1000
          const firstThreeMessages: DualReportMessage[] = firstYearRows.map((row) => {
            const msgTime = parseInt(row.create_time || '0', 10) * 1000
            const msgContent = this.decodeMessageContent(row.message_content, row.compress_content)
            return {
              content: String(msgContent || ''),
              isSentByMe: this.resolveIsSent(row, rawWxid, cleanedWxid),
              createTime: msgTime,
              createTimeStr: this.formatDateTime(msgTime)
            }
          })
          yearFirstChat = {
            createTime,
            createTimeStr: this.formatDateTime(createTime),
            content: String(this.decodeMessageContent(firstRow.message_content, firstRow.compress_content) || ''),
            isSentByMe: this.resolveIsSent(firstRow, rawWxid, cleanedWxid),
            friendName,
            firstThreeMessages
          }
        }
      }

      this.reportProgress('统计聊天数据...', 30, onProgress)

      const statsResult = await wcdbService.getDualReportStats(friendUsername, startTime, endTime)
      if (!statsResult.success || !statsResult.data) {
        return { success: false, error: statsResult.error || '获取双人报告统计失败' }
      }

      const cppData = statsResult.data
      const counts = cppData.counts || {}

      const stats: DualReportStats = {
        totalMessages: counts.total || 0,
        totalWords: counts.words || 0,
        imageCount: counts.image || 0,
        voiceCount: counts.voice || 0,
        emojiCount: counts.emoji || 0
      }

      // Process Emojis to find top for me and friend
      let myTopEmojiMd5: string | undefined
      let myTopEmojiUrl: string | undefined
      let myTopCount = -1

      let friendTopEmojiMd5: string | undefined
      let friendTopEmojiUrl: string | undefined
      let friendTopCount = -1

      if (cppData.emojis && Array.isArray(cppData.emojis)) {
        for (const item of cppData.emojis) {
          const rawContent = item.content || ''
          const isMe = rawContent.startsWith('1:')
          const content = rawContent.substring(2) // Remove "1:" or "0:" prefix
          const count = item.count || 0

          if (isMe) {
            if (count > myTopCount) {
              const md5 = this.extractEmojiMd5(content)
              if (md5) {
                myTopCount = count
                myTopEmojiMd5 = md5
                myTopEmojiUrl = this.extractEmojiUrl(content)
              }
            }
          } else {
            if (count > friendTopCount) {
              const md5 = this.extractEmojiMd5(content)
              if (md5) {
                friendTopCount = count
                friendTopEmojiMd5 = md5
                friendTopEmojiUrl = this.extractEmojiUrl(content)
              }
            }
          }
        }
      }

      stats.myTopEmojiMd5 = myTopEmojiMd5
      stats.myTopEmojiUrl = myTopEmojiUrl
      stats.friendTopEmojiMd5 = friendTopEmojiMd5
      stats.friendTopEmojiUrl = friendTopEmojiUrl
      if (myTopCount >= 0) stats.myTopEmojiCount = myTopCount
      if (friendTopCount >= 0) stats.friendTopEmojiCount = friendTopCount

      const topPhrases = (cppData.phrases || []).map((p: any) => ({
        phrase: p.phrase,
        count: p.count
      }))

      // Attach extra stats to the data object (needs interface update if strictly typed, but data is flexible)
      const reportData: DualReportData = {
        year: reportYear,
        selfName: myName,
        selfAvatarUrl,
        friendUsername,
        friendName,
        friendAvatarUrl,
        firstChat,
        firstChatMessages,
        yearFirstChat,
        stats,
        topPhrases,
        // Append new C++ stats
        heatmap: cppData.heatmap,
        initiative: cppData.initiative,
        response: cppData.response,
        monthly: cppData.monthly,
        streak: cppData.streak
      } as any // Use as any to bypass strict type check for new fields, or update interface

      this.reportProgress('双人报告生成完成', 100, onProgress)
      return { success: true, data: reportData }
    } catch (e) {
      return { success: false, error: String(e) }
    }
  }
}

export const dualReportService = new DualReportService()
