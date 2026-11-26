import { Suspense, lazy } from 'react'
import { NavLink, Navigate, Outlet, Route, Routes } from 'react-router-dom'
import { useAuth } from './auth'
import CallDetailPage from './pages/CallDetail'
import CampaignDetailPage from './pages/CampaignDetail'
import CampaignsPage from './pages/Campaigns'
import DncPage from './pages/Dnc'
import LoginPage from './pages/Login'
import NewCampaignPage from './pages/NewCampaign'
import QuickCallPage from './pages/QuickCall'
import UsersPage from './pages/Users'

// The LiveKit SDK is large and only this page needs it, so load it on demand.
const TestCallPage = lazy(() => import('./pages/TestCall'))

function Layout() {
  const { user, loading, logout } = useAuth()
  if (loading) return <p className="center muted">Loadingâ€¦</p>
  if (!user) return <Navigate to="/login" replace />
  return (
    <>
      <header className="nav">
        <strong>Cold Caller</strong>
        <NavLink to="/" end>Campaigns</NavLink>
        <NavLink to="/quick-call">Quick call</NavLink>
        <NavLink to="/test-call">Test call</NavLink>
        <NavLink to="/dnc">Do-not-call</NavLink>
        {user.role === 'admin' && <NavLink to="/users">Users</NavLink>}
        <span className="spacer" />
        <span className="muted">{user.email}</span>
        <button className="link" onClick={() => void logout()}>Sign out</button>
      </header>
      <main>
        <Outlet />
      </main>
    </>
  )
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<Layout />}>
        <Route index element={<CampaignsPage />} />
        <Route path="campaigns/new" element={<NewCampaignPage />} />
        <Route path="campaigns/:id" element={<CampaignDetailPage />} />
        <Route path="calls/:id" element={<CallDetailPage />} />
        <Route path="quick-call" element={<QuickCallPage />} />
        <Route
          path="test-call"
          element={
            <Suspense fallback={<p className="muted">Loading…</p>}>
              <TestCallPage />
            </Suspense>
          }
        />
        <Route path="dnc" element={<DncPage />} />
        <Route path="users" element={<UsersPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
