import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useState, type FormEvent } from 'react'
import { Navigate } from 'react-router-dom'
import { api, json, type User } from '../api'
import { useAuth } from '../auth'

export default function UsersPage() {
  const { user: me } = useAuth()
  const qc = useQueryClient()
  const [form, setForm] = useState({ email: '', name: '', password: '', role: 'member' })
  const users = useQuery({ queryKey: ['users'], queryFn: () => api<User[]>('/users'), enabled: me?.role === 'admin' })
  const refresh = () => void qc.invalidateQueries({ queryKey: ['users'] })
  const create = useMutation({
    mutationFn: () => api<User>('/users', json('POST', form)),
    onSuccess: () => {
      setForm({ email: '', name: '', password: '', role: 'member' })
      refresh()
    },
  })
  const patch = useMutation({
    mutationFn: ({ id, ...data }: { id: string; is_active?: boolean; role?: string }) =>
      api<User>(`/users/${id}`, json('PATCH', data)),
    onSuccess: refresh,
  })

  if (me?.role !== 'admin') return <Navigate to="/" replace />

  function submit(e: FormEvent) {
    e.preventDefault()
    create.mutate()
  }

  return (
    <>
      <h2>Users</h2>
      <form className="card form" onSubmit={submit}>
        <h3>Add user</h3>
        <div className="grid">
          <label>Email<input type="email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} required /></label>
          <label>Name<input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></label>
          <label>Temporary password (10+ chars)<input type="password" minLength={10} value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} required /></label>
          <label>Role
            <select value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })}>
              <option value="member">Member</option>
              <option value="admin">Admin</option>
            </select>
          </label>
        </div>
        {create.error && <p className="error">{create.error.message}</p>}
        <button disabled={create.isPending}>Create user</button>
      </form>
      {patch.error && <p className="error">{patch.error.message}</p>}
      <table>
        <thead><tr><th>Email</th><th>Name</th><th>Role</th><th>Status</th><th /></tr></thead>
        <tbody>
          {users.data?.map((u) => (
            <tr key={u.id}>
              <td>{u.email}</td><td>{u.name}</td><td>{u.role}</td>
              <td>{u.is_active ? 'active' : 'disabled'}</td>
              <td>
                {u.id !== me.id && (
                  <button className="link" onClick={() => patch.mutate({ id: u.id, is_active: !u.is_active })}>
                    {u.is_active ? 'Disable' : 'Enable'}
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  )
}
