import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { api, type Campaign, type Call } from '../api'

const PAGE = 50

export default function CampaignDetailPage() {
  const { id = '' } = useParams()
  const qc = useQueryClient()
  const navigate = useNavigate()
  const [offset, setOffset] = useState(0)

  const campaign = useQuery({
    queryKey: ['campaign', id],
    queryFn: () => api<Campaign>(`/campaigns/${id}`),
    refetchInterval: 3000,
  })
  const calls = useQuery({
    queryKey: ['campaign-calls', id, offset],
    queryFn: () =>
      api<{ items: Call[]; total: number }>(`/calls?campaign_id=${id}&limit=${PAGE}&offset=${offset}`),
    refetchInterval: 3000,
  })

  const action = useMutation({
    mutationFn: (a: 'start' | 'pause' | 'resume') => api<Campaign>(`/campaigns/${id}/${a}`, { method: 'POST' }),
    onSuccess: () => void qc.invalidateQueries(),
  })
  const del = useMutation({
    mutationFn: () => api(`/campaigns/${id}`, { method: 'DELETE' }),
    onSuccess: () => navigate('/'),
  })

  if (campaign.isLoading) return <p className="muted">Loading…</p>
  if (campaign.error || !campaign.data) return <p className="error">{campaign.error?.message ?? 'Not found'}</p>
  const c = campaign.data
  const total = calls.data?.total ?? 0

  return (
    <>
      <div className="row">
        <h2>{c.name}</h2>
        <span className={`badge ${c.status}`}>{c.status}</span>
        <span className="spacer" />
        {c.status === 'draft' && (
          <>
            <button onClick={() => action.mutate('start')}>Start</button>
            <button className="danger" onClick={() => confirm('Delete this draft?') && del.mutate()}>Delete</button>
          </>
        )}
        {c.status === 'running' && <button onClick={() => action.mutate('pause')}>Pause</button>}
        {c.status === 'paused' && <button onClick={() => action.mutate('resume')}>Resume</button>}
      </div>
      {(action.error || del.error) && <p className="error">{(action.error ?? del.error)?.message}</p>}

      <div className="stats">
        <div><b>{c.total}</b><span>contacts</span></div>
        {Object.entries(c.counts).map(([k, v]) => (
          <div key={k}><b>{v}</b><span>{k.replace('_', ' ')}</span></div>
        ))}
      </div>
      <p className="muted">
        Window {c.call_window_start.slice(0, 5)}–{c.call_window_end.slice(0, 5)} {c.timezone} · max {c.max_concurrency} concurrent · {c.cps} cps
      </p>

      <table>
        <thead>
          <tr><th>Number</th><th>Name</th><th>Status</th><th>Queued</th></tr>
        </thead>
        <tbody>
          {calls.data?.items.map((call) => (
            <tr key={call.id}>
              <td><Link to={`/calls/${call.id}`}>{call.to_number}</Link></td>
              <td>{call.contact_name}</td>
              <td><span className={`badge ${call.status}`}>{call.status.replace('_', ' ')}</span></td>
              <td>{call.queued_at ? new Date(call.queued_at).toLocaleTimeString() : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="row">
        <button disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>Prev</button>
        <span className="muted">{total === 0 ? 0 : offset + 1}–{Math.min(offset + PAGE, total)} of {total}</span>
        <button disabled={offset + PAGE >= total} onClick={() => setOffset(offset + PAGE)}>Next</button>
      </div>
    </>
  )
}
