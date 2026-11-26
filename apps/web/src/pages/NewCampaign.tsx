import { useState, type FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { api, type CampaignCreated } from '../api'

const DEFAULT_SCRIPT = `You are calling on behalf of Acme. Greet {name} politely, say you are an automated assistant, and ask whether they have a minute to hear about our new offer.`

export default function NewCampaignPage() {
  const navigate = useNavigate()
  const [file, setFile] = useState<File | null>(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [result, setResult] = useState<CampaignCreated | null>(null)

  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    if (!file) return
    const fd = new FormData(e.currentTarget)
    fd.set('file', file)
    setBusy(true)
    setError('')
    try {
      const created = await api<CampaignCreated>('/campaigns', { method: 'POST', body: fd })
      if (created.rejected.length === 0) navigate(`/campaigns/${created.campaign.id}`)
      else setResult(created)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed')
    } finally {
      setBusy(false)
    }
  }

  if (result) {
    return (
      <>
        <h2>Campaign created</h2>
        <p>
          {result.accepted} contacts accepted, {result.rejected.length} rejected.{' '}
          <Link to={`/campaigns/${result.campaign.id}`}>Open campaign</Link>
        </p>
        <table>
          <thead><tr><th>Line</th><th>Phone</th><th>Reason</th></tr></thead>
          <tbody>
            {result.rejected.map((r) => (
              <tr key={r.line}><td>{r.line}</td><td>{r.phone}</td><td>{r.reason}</td></tr>
            ))}
          </tbody>
        </table>
      </>
    )
  }

  return (
    <form className="card form" onSubmit={(e) => void submit(e)}>
      <h2>New campaign</h2>
      <label>Name<input name="name" required maxLength={200} /></label>
      <label>
        Contacts CSV
        <input type="file" accept=".csv,text/csv" required onChange={(e) => setFile(e.target.files?.[0] ?? null)} />
        <small className="muted">
          Needs a <code>phone</code> column (Indian numbers). Optional <code>name</code>; any other column can be used in the script as <code>{'{column}'}</code>.
        </small>
      </label>
      <label>
        Script / prompt
        <textarea name="script_prompt" rows={6} defaultValue={DEFAULT_SCRIPT} required />
      </label>
      <div className="grid">
        <label>Caller ID<input name="caller_id" placeholder="Assigned 140-series number" /></label>
        <label>Max concurrent calls<input name="max_concurrency" type="number" min={1} max={100} defaultValue={5} /></label>
        <label>Calls per second<input name="cps" type="number" min={1} max={50} defaultValue={1} /></label>
        <label>Timezone<input name="timezone" defaultValue="Asia/Kolkata" /></label>
        <label>Window start<input name="call_window_start" type="time" defaultValue="09:00" min="09:00" max="21:00" /></label>
        <label>Window end<input name="call_window_end" type="time" defaultValue="21:00" min="09:00" max="21:00" /></label>
      </div>
      <label>Webhook URL (optional)<input name="webhook_url" type="url" /></label>
      {error && <p className="error">{error}</p>}
      <button disabled={busy || !file}>{busy ? 'Uploading…' : 'Create campaign'}</button>
    </form>
  )
}
