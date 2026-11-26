import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { api, type Campaign } from '../api'

export default function CampaignsPage() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['campaigns'],
    queryFn: () => api<Campaign[]>('/campaigns'),
    refetchInterval: 5000,
  })
  return (
    <>
      <div className="row">
        <h2>Campaigns</h2>
        <span className="spacer" />
        <Link className="btn" to="/campaigns/new">New campaign</Link>
      </div>
      {isLoading && <p className="muted">Loading…</p>}
      {error && <p className="error">{error.message}</p>}
      {data && data.length === 0 && <p className="muted">No campaigns yet.</p>}
      {data && data.length > 0 && (
        <table>
          <thead>
            <tr><th>Name</th><th>Status</th><th>Contacts</th><th>Created</th></tr>
          </thead>
          <tbody>
            {data.map((c) => (
              <tr key={c.id}>
                <td><Link to={`/campaigns/${c.id}`}>{c.name}</Link></td>
                <td><span className={`badge ${c.status}`}>{c.status}</span></td>
                <td>{c.total}</td>
                <td>{new Date(c.created_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  )
}
