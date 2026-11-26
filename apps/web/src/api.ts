export type User = {
  id: string
  email: string
  name: string
  role: 'admin' | 'member'
  is_active: boolean
  created_at: string
}

export type Campaign = {
  id: string
  name: string
  status: 'draft' | 'running' | 'paused' | 'done' | 'failed'
  script_prompt: string
  caller_id: string
  max_concurrency: number
  cps: number
  timezone: string
  call_window_start: string
  call_window_end: string
  webhook_url: string | null
  created_at: string
  total: number
  counts: Record<string, number>
}

export type Call = {
  id: string
  campaign_id: string | null
  to_number: string
  contact_name: string
  custom_fields: Record<string, string>
  script_prompt: string | null
  status: string
  attempt: number
  outcome: Record<string, unknown> | null
  transcript: string | null
  failure_reason: string | null
  created_at: string
  queued_at: string | null
  dialed_at: string | null
  answered_at: string | null
  ended_at: string | null
}

export type CallDetail = Call & {
  events: { type: string; payload: Record<string, unknown>; at: string }[]
}

export type CampaignCreated = {
  campaign: Campaign
  accepted: number
  rejected: { line: number; phone: string; reason: string }[]
}

export type Dnc = { id: string; number: string; source: string; added_at: string }

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

function errorMessage(body: unknown, fallback: string): string {
  const detail = (body as { detail?: unknown } | null)?.detail
  if (typeof detail === 'string') return detail
  if (Array.isArray(detail)) {
    return detail
      .map((d: { loc?: unknown[]; msg?: string }) => `${d.loc?.slice(-1)[0] ?? ''}: ${d.msg ?? ''}`)
      .join('; ')
  }
  return fallback
}

export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  if (init.body && !(init.body instanceof FormData) && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json')
  }
  const res = await fetch(`/api${path}`, { ...init, headers, credentials: 'same-origin' })
  if (res.status === 204) return undefined as T
  const body = await res.json().catch(() => null)
  if (!res.ok) throw new ApiError(res.status, errorMessage(body, res.statusText))
  return body as T
}

export const json = (method: string, data: unknown): RequestInit => ({
  method,
  body: JSON.stringify(data),
})
