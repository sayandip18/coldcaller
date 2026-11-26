import { useMutation, useQuery } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import { api, json, type Call } from '../api'

export default function QuickCallPage() {
  const [to, setTo] = useState('')
  const [name, setName] = useState('')
  const [script, setScript] = useState('You are a friendly assistant calling {name}. Introduce yourself as an automated assistant.')
  const create = useMutation({
    mutationFn: () =>
      api<Call>('/calls', json('POST', { to_number: to, contact_name: name, script_prompt: script })),
  })
  const recent = useQuery({
    queryKey: ['single-calls'],
    queryFn: () => api<{ items: Call[] }>('/calls?limit=20'),
    refetchInterval: 4000,
  })

  function submit(e: FormEvent) {
    e.preventDefault()
    create.mutate(undefined, { onSuccess: () => void recent.refetch() })
  }

  return (
    <>
      <form className="card form" onSubmit={submit}>
        <h2>Quick call</h2>
        <label>Phone number<input value={to} onChange={(e) => setTo(e.target.value)} placeholder="98765 43210" required /></label>
        <label>Contact name<input value={name} onChange={(e) => setName(e.target.value)} /></label>
        <label>Script / prompt<textarea rows={4} value={script} onChange={(e) => setScript(e.target.value)} required /></label>
        {create.error && <p className="error">{create.error.message}</p>}
        <button disabled={create.isPending}>Queue call</button>
      </form>
      <h3>Recent calls</h3>
      <table>
        <thead><tr><th>Number</th><th>Name</th><th>Status</th><th>Created</th></tr></thead>
        <tbody>
          {recent.data?.items.map((c) => (
            <tr key={c.id}>
              <td><Link to={`/calls/${c.id}`}>{c.to_number}</Link></td>
              <td>{c.contact_name}</td>
              <td><span className={`badge ${c.status}`}>{c.status.replace('_', ' ')}</span></td>
              <td>{new Date(c.created_at).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}
