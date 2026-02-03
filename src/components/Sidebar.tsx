import { useState, useEffect } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { Home, MessageSquare, BarChart3, Users, FileText, Database, Settings, ChevronLeft, ChevronRight, Download, Aperture, UserCircle, Lock } from 'lucide-react'
import { useAppStore } from '../stores/appStore'
import * as configService from '../services/config'
import './Sidebar.scss'

function Sidebar() {
  const location = useLocation()
  const [collapsed, setCollapsed] = useState(false)
  const [authEnabled, setAuthEnabled] = useState(false)
  const setLocked = useAppStore(state => state.setLocked)

  useEffect(() => {
    configService.getAuthEnabled().then(setAuthEnabled)
  }, [])

  const isActive = (path: string) => {
    return location.pathname === path || location.pathname.startsWith(`${path}/`)
  }

  return (
    <aside className={`sidebar ${collapsed ? 'collapsed' : ''}`}>
      <nav className="nav-menu">
        {/* 首页 */}
        <NavLink
          to="/home"
          className={`nav-item ${isActive('/home') ? 'active' : ''}`}
          title={collapsed ? '首页' : undefined}
        >
          <span className="nav-icon"><Home size={20} /></span>
          <span className="nav-label">首页</span>
        </NavLink>

        {/* 聊天 */}
        <NavLink
          to="/chat"
          className={`nav-item ${isActive('/chat') ? 'active' : ''}`}
          title={collapsed ? '聊天' : undefined}
        >
          <span className="nav-icon"><MessageSquare size={20} /></span>
          <span className="nav-label">聊天</span>
        </NavLink>

        {/* 朋友圈 */}
        <NavLink
          to="/sns"
          className={`nav-item ${isActive('/sns') ? 'active' : ''}`}
          title={collapsed ? '朋友圈' : undefined}
        >
          <span className="nav-icon"><Aperture size={20} /></span>
          <span className="nav-label">朋友圈</span>
        </NavLink>

        {/* 通讯录 */}
        <NavLink
          to="/contacts"
          className={`nav-item ${isActive('/contacts') ? 'active' : ''}`}
          title={collapsed ? '通讯录' : undefined}
        >
          <span className="nav-icon"><UserCircle size={20} /></span>
          <span className="nav-label">通讯录</span>
        </NavLink>

        {/* 私聊分析 */}
        <NavLink
          to="/analytics"
          className={`nav-item ${isActive('/analytics') ? 'active' : ''}`}
          title={collapsed ? '私聊分析' : undefined}
        >
          <span className="nav-icon"><BarChart3 size={20} /></span>
          <span className="nav-label">私聊分析</span>
        </NavLink>

        {/* 群聊分析 */}
        <NavLink
          to="/group-analytics"
          className={`nav-item ${isActive('/group-analytics') ? 'active' : ''}`}
          title={collapsed ? '群聊分析' : undefined}
        >
          <span className="nav-icon"><Users size={20} /></span>
          <span className="nav-label">群聊分析</span>
        </NavLink>

        {/* 年度报告 */}
        <NavLink
          to="/annual-report"
          className={`nav-item ${isActive('/annual-report') ? 'active' : ''}`}
          title={collapsed ? '年度报告' : undefined}
        >
          <span className="nav-icon"><FileText size={20} /></span>
          <span className="nav-label">年度报告</span>
        </NavLink>

        {/* 导出 */}
        <NavLink
          to="/export"
          className={`nav-item ${isActive('/export') ? 'active' : ''}`}
          title={collapsed ? '导出' : undefined}
        >
          <span className="nav-icon"><Download size={20} /></span>
          <span className="nav-label">导出</span>
        </NavLink>


      </nav>

      <div className="sidebar-footer">
        {authEnabled && (
          <button
            className="nav-item"
            onClick={() => setLocked(true)}
            title={collapsed ? '锁定' : undefined}
          >
            <span className="nav-icon"><Lock size={20} /></span>
            <span className="nav-label">锁定</span>
          </button>
        )}

        <NavLink
          to="/settings"
          className={`nav-item ${isActive('/settings') ? 'active' : ''}`}
          title={collapsed ? '设置' : undefined}
        >
          <span className="nav-icon">
            <Settings size={20} />
          </span>
          <span className="nav-label">设置</span>
        </NavLink>

        <button
          className="collapse-btn"
          onClick={() => setCollapsed(!collapsed)}
          title={collapsed ? '展开菜单' : '收起菜单'}
        >
          {collapsed ? <ChevronRight size={18} /> : <ChevronLeft size={18} />}
        </button>
      </div>
    </aside>
  )
}

export default Sidebar
