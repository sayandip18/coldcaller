import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { api, json, type Dnc } from '../api'

export default function DncPage() {
  const qc = useQueryClient()
  const [number, setNumber] = useState('')
  const list = useQuery({ queryKey: ['dnc'], queryFn: () => api<Dnc[]>('/dnc') })
  const add = useMutation({
    mutationFn: () => api<Dnc>('/dnc', json('POST', { number })),
    onSuccess: () => {
      setNumber('')
      void qc.invalidateQueries({ queryKey: ['dnc'] })
    },
  })
  const remove = useMutation({
    mutationFn: (id: string) => api(`/dnc/${id}`, { method: 'DELETE' }),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['dnc'] }),
  })

  function submit(e: FormEvent) {
    e.preventDefault()
    add.mutate()
  }

  return (
    <>
      <h2>Do-not-call list</h2>
      <p className="muted">Numbers here are never dialled. Campaign calls to them are marked “skipped dnc”.</p>
      <form className="row" onSubmit={submit}>
        <input value={number} onChange={(e) => setNumber(e.target.value)} placeholder="98765 43210" required />
        <button disabled={add.isPending}>Add</button>
      </form>
      {add.error && <p className="error">{add.error.message}</p>}
      <table>
        <thead><tr><th>Number</th><th>Source</th><th>Added</th><th /></tr></thead>
        <tbody>
          {list.data?.map((d) => (
            <tr key={d.id}>
              <td>{d.number}</td><td>{d.source}</td><td>{new Date(d.added_at).toLocaleString()}</td>
              <td><button className="link" onClick={() => remove.mutate(d.id)}>Remove</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}
