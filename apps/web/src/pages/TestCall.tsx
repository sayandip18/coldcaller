import { useQuery } from '@tanstack/react-query'
import { ParticipantKind, Room, RoomEvent, Track, type Participant } from 'livekit-client'
import { useEffect, useRef, useState, type FormEvent } from 'react'
import { Link } from 'react-router-dom'
import { api, json, type Call, type CallDetail } from '../api'

type Phase = 'idle' | 'connecting' | 'waiting' | 'live' | 'ended'
type Caption = { id: string; who: 'You' | 'Agent'; text: string; final: boolean }
type Started = { call_id: string; room: string; identity: string; token: string; url: string }

const DEFAULT_SCRIPT = `You are calling on behalf of Acme Solar. Greet {name}, say you are an automated AI assistant from Acme Solar, and ask if they have a minute to hear about our rooftop solar offer. The offer: free site survey and 20% off installation this month. If they are interested, offer a call back from a colleague to book the survey.`

const PHASE_LABEL: Record<Phase, string> = {
  idle: 'Ready',
  connecting: 'Connecting…',
  waiting: 'Waiting for the agent to join…',
  live: 'Connected',
  ended: 'Call ended',
}

// The agent is the only non-"sip-" participant in the room; the browser joins as "sip-<call id>".
const isAgent = (p: Participant) => p.kind === ParticipantKind.AGENT || !p.identity.startsWith('sip-')

function parseFields(text: string): Record<string, string> {
  const fields: Record<string, string> = {}
  for (const line of text.split('\n')) {
    const i = line.indexOf('=')
    if (i > 0) fields[line.slice(0, i).trim()] = line.slice(i + 1).trim()
  }
  return fields
}

function friendlyError(e: unknown): string {
  if (e instanceof DOMException && e.name === 'NotAllowedError')
    return 'Microphone access was blocked. Allow the microphone for this site in your browser and try again.'
  if (e instanceof DOMException && e.name === 'NotFoundError') return 'No microphone was found.'
  return e instanceof Error ? e.message : 'Something went wrong'
}

export default function TestCallPage() {
  const [phase, setPhase] = useState<Phase>('idle')
  const [error, setError] = useState('')
  const [callId, setCallId] = useState('')
  const [muted, setMuted] = useState(false)
  const [agentState, setAgentState] = useState('')
  const [captions, setCaptions] = useState<Caption[]>([])
  const roomRef = useRef<Room | null>(null)
  const audioBox = useRef<HTMLDivElement>(null)
  const phaseRef = useRef<Phase>('idle')
  const waitTimer = useRef<number | undefined>(undefined)

  const go = (p: Phase) => {
    phaseRef.current = p
    setPhase(p)
  }

  useEffect(
    () => () => {
      window.clearTimeout(waitTimer.current)
      roomRef.current?.disconnect()
    },
    [],
  )

  const recent = useQuery({
    queryKey: ['test-calls'],
    queryFn: () => api<{ items: Call[] }>('/calls?is_test=true&limit=10'),
    refetchInterval: phase === 'idle' || phase === 'ended' ? 5000 : false,
  })

  // After the call: show what the agent recorded (the outcome can land a moment after hang-up).
  const result = useQuery({
    queryKey: ['test-call', callId],
    queryFn: () => api<CallDetail>(`/calls/${callId}`),
    enabled: phase === 'ended' && !!callId,
    refetchInterval: (q) => (q.state.data?.outcome ? false : 2000),
  })

  function upsertCaption(id: string, who: Caption['who'], text: string, final: boolean) {
    setCaptions((prev) => {
      const i = prev.findIndex((c) => c.id === id)
      if (i === -1) return [...prev, { id, who, text, final }]
      const next = prev.slice()
      next[i] = { id, who, text, final }
      return next
    })
  }

  function wire(room: Room, identity: string) {
    room.on(RoomEvent.TrackSubscribed, (track) => {
      if (track.kind === Track.Kind.Audio) audioBox.current?.appendChild(track.attach())
    })
    room.on(RoomEvent.TrackUnsubscribed, (track) => track.detach().forEach((el) => el.remove()))
    room.on(RoomEvent.ParticipantConnected, (p) => {
      if (isAgent(p)) {
        window.clearTimeout(waitTimer.current)
        setError('')
        go('live')
      }
    })
    room.on(RoomEvent.ParticipantAttributesChanged, (_changed, p) => {
      if (isAgent(p)) setAgentState(p.attributes['lk.agent.state'] ?? '')
    })
    room.on(RoomEvent.ParticipantDisconnected, (p) => {
      if (isAgent(p)) room.disconnect() // the agent hung up (call finished or failed)
    })
    room.on(RoomEvent.Disconnected, () => {
      window.clearTimeout(waitTimer.current)
      setAgentState('')
      go('ended')
    })
    // Live captions: the agent publishes what each side said on this text-stream topic.
    room.registerTextStreamHandler('lk.transcription', (reader, info) => {
      const attrs = reader.info.attributes ?? {}
      const id = attrs['lk.segment_id'] ?? reader.info.id
      const final = attrs['lk.transcription_final'] === 'true'
      const who: Caption['who'] = info.identity === identity ? 'You' : 'Agent'
      void (async () => {
        let text = ''
        for await (const chunk of reader) {
          text += chunk
          upsertCaption(id, who, text, false)
        }
        upsertCaption(id, who, text, final || who === 'Agent')
      })()
    })
  }

  async function start(e: FormEvent<HTMLFormElement>) {
    e.preventDefault()
    const form = new FormData(e.currentTarget)
    setError('')
    setCaptions([])
    setMuted(false)
    go('connecting')
    let room: Room | null = null
    try {
      const started = await api<Started>(
        '/test-calls',
        json('POST', {
          script_prompt: String(form.get('script')),
          contact_name: String(form.get('name') ?? ''),
          custom_fields: parseFields(String(form.get('fields') ?? '')),
        }),
      )
      setCallId(started.call_id)
      room = new Room({
        audioCaptureDefaults: { echoCancellation: true, noiseSuppression: true, autoGainControl: true },
      })
      roomRef.current = room
      wire(room, started.identity)
      await room.connect(started.url, started.token)
      await room.localParticipant.setMicrophoneEnabled(true) // asks for microphone permission
      await room.startAudio().catch(() => undefined)
      go('waiting')
      // Start the agent only now, so it never greets an empty room.
      await api(`/test-calls/${started.call_id}/agent`, { method: 'POST' })
      waitTimer.current = window.setTimeout(() => {
        if (phaseRef.current === 'waiting')
          setError('The agent has not joined after 20 seconds. Is the agent container running (pnpm infra:livekit)?')
      }, 20000)
    } catch (err) {
      setError(friendlyError(err))
      room?.disconnect()
      if (phaseRef.current !== 'ended') go('idle')
    }
  }

  async function toggleMute() {
    const next = !muted
    await roomRef.current?.localParticipant.setMicrophoneEnabled(!next)
    setMuted(next)
  }

  const inCall = phase === 'connecting' || phase === 'waiting' || phase === 'live'
  const insecure = !window.isSecureContext

  return (
    <>
      <h2>Test call</h2>
      <p className="muted">
        Talk to the voice agent from this browser. No phone, carrier or real number is involved, and nothing is
        queued, blocked or sent to webhooks.
      </p>
      {insecure && (
        <p className="error">Microphone access needs https or http://localhost. Open the app on localhost.</p>
      )}

      {!inCall && (
        <form className="card form" onSubmit={(e) => void start(e)}>
          <label>
            Script / prompt
            <textarea name="script" rows={7} defaultValue={DEFAULT_SCRIPT} required />
            <small className="muted">
              <code>{'{name}'}</code> and any field below are filled in, exactly as for a campaign.
            </small>
          </label>
          <div className="grid">
            <label>
              Contact name
              <input name="name" defaultValue="Asha" />
            </label>
            <label>
              Extra fields (one <code>key=value</code> per line)
              <textarea name="fields" rows={2} placeholder="city=Pune" />
            </label>
          </div>
          {error && <p className="error">{error}</p>}
          <button disabled={insecure}>Start test call</button>
        </form>
      )}

      {inCall && (
        <div className="card">
          <div className="row">
            <span className={`status-chip ${phase}`}>{PHASE_LABEL[phase]}</span>
            {agentState && <span className="muted">Agent is {agentState}</span>}
            <span className="spacer" />
            <button disabled={phase === 'connecting'} onClick={() => void toggleMute()}>
              {muted ? 'Unmute' : 'Mute'}
            </button>
            <button className="danger" onClick={() => roomRef.current?.disconnect()}>
              End call
            </button>
          </div>
          {error && <p className="error">{error}</p>}
          {phase === 'live' && <p className="muted">Speak when you hear the agent. Use headphones to avoid echo.</p>}
        </div>
      )}

      <div ref={audioBox} hidden />

      {(inCall || phase === 'ended') && (
        <>
          <h3>Conversation</h3>
          <div className="captions">
            {captions.length === 0 && <p className="muted">Captions appear here as you talk.</p>}
            {captions.map((c) => (
              <p key={c.id} className={`cap ${c.who === 'You' ? 'you' : 'agent'} ${c.final ? '' : 'interim'}`}>
                <b>{c.who}</b> {c.text}
              </p>
            ))}
          </div>
        </>
      )}

      {phase === 'ended' && (
        <>
          {error && <p className="error">{error}</p>}
          <h3>Result</h3>
          {result.data?.outcome ? (
            <>
              <table>
                <tbody>
                  {Object.entries(result.data.outcome).map(([k, v]) => (
                    <tr key={k}>
                      <th>{k}</th>
                      <td>{v === null || v === '' ? '—' : String(v)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {result.data.transcript && <pre>{result.data.transcript}</pre>}
            </>
          ) : (
            <p className="muted">Waiting for the agent to report the outcome…</p>
          )}
          <button onClick={() => go('idle')}>New test call</button>
        </>
      )}

      {recent.data && recent.data.items.length > 0 && (
        <>
          <h3>Recent test calls</h3>
          <table>
            <thead>
              <tr>
                <th>When</th>
                <th>Status</th>
                <th>Result</th>
              </tr>
            </thead>
            <tbody>
              {recent.data.items.map((c) => (
                <tr key={c.id}>
                  <td>
                    <Link to={`/calls/${c.id}`}>{new Date(c.created_at).toLocaleString()}</Link>
                  </td>
                  <td>
                    <span className={`badge ${c.status}`}>{c.status.replace('_', ' ')}</span>
                  </td>
                  <td>{String(c.outcome?.result ?? '—')}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </>
  )
}
