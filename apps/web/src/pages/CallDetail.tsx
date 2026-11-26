import { useQuery } from '@tanstack/react-query'
import { Link, useParams } from 'react-router-dom'
import { api, type CallDetail } from '../api'

export default function CallDetailPage() {
  const { id = '' } = useParams()
  const { data, isLoading, error } = useQuery({
    queryKey: ['call', id],
    queryFn: () => api<CallDetail>(`/calls/${id}`),
    refetchInterval: 3000,
  })
  if (isLoading) return <p className="muted">Loading…</p>
  if (error || !data) return <p className="error">{error?.message ?? 'Not found'}</p>
  return (
    <>
      <p>
        {data.campaign_id ? <Link to={`/campaigns/${data.campaign_id}`}>← Campaign</Link> : <Link to="/quick-call">← Quick call</Link>}
      </p>
      <div className="row">
        <h2>{data.to_number}</h2>
        <span className={`badge ${data.status}`}>{data.status.replace('_', ' ')}</span>
      </div>
      <p className="muted">{data.contact_name || 'No name'} · attempt {data.attempt}</p>
      {data.failure_reason && <p className="error">{data.failure_reason}</p>}

      <h3>Timeline</h3>
      <ul className="timeline">
        {data.events.map((e, i) => (
          <li key={i}><time>{new Date(e.at).toLocaleTimeString()}</time> {e.type.replace('_', ' ')}</li>
        ))}
      </ul>

      <h3>Outcome</h3>
      {data.outcome ? <pre>{JSON.stringify(data.outcome, null, 2)}</pre> : <p className="muted">Not available yet.</p>}
      <h3>Transcript</h3>
      {data.transcript ? <pre>{data.transcript}</pre> : <p className="muted">Not available yet.</p>}
    </>
  )
}
