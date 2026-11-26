# Going live with a real carrier

What is built and tested, what you must get from the carrier, and the steps from "works against a
fake phone network" to "calls a real customer". Anything marked **unverified** has not been
exercised against a real carrier or on a production host.

## The path a call takes

```
worker --> LiveKit SIP --> Asterisk bridge --> carrier (Pulse / ConnectHub) --> customer's phone
                 \--> voice agent joins the LiveKit room when the customer answers
```

The bridge and the carrier are the only parts that differ from the local setup. Locally, `sipp`
plays the carrier, and everything else (queue, limits, LiveKit, agent) is identical.

## 1. What to get from the carrier

Ask for these in writing. The right answers decide the settings in step 3. They are questions, not
assumptions: carriers differ.

| Ask | Why it matters | Setting |
|---|---|---|
| SIP host/IP and port for outbound calls | where Asterisk sends calls | `PULSE_HOST`, `PULSE_PORT` |
| How do they authenticate us: our public IP whitelisted, username/password, or registration? | | `PULSE_AUTH` (`ip`, `userpass`, `register`), `PULSE_USERNAME`, `PULSE_PASSWORD` |
| The IP(s) they will see us come from, so they can whitelist them | Asterisk's public IP | |
| Called-number format: `+91...`, `91...`, `0...`, or with an access code? | wrong format = every call rejected | `DIAL_KEEP_PLUS`, `DIAL_PREFIX` |
| Which numbers may we present as caller ID, and what is the format (E.164 or national)? | calls from other numbers are blocked or flagged | `ALLOWED_CALLER_IDS`, `DEFAULT_CALLER_ID` |
| Do they need the caller ID in `P-Asserted-Identity` as well as `From`? | some do | `PULSE_SEND_PAI` |
| Contracted CPS (calls per second) and concurrent-channel limits | exceeding them gets calls rejected or the trunk throttled | `CARRIER_CPS`, `MAX_GLOBAL_CHANNELS` |
| Codecs (we offer G.711 u-law and a-law) and DTMF method | | |
| Do they answer SIP `OPTIONS` pings? | decides whether the health probe can be on | `PULSE_QUALIFY_SECONDS` |
| What do they return for busy / no answer / invalid number / rejected? | confirm they use standard SIP codes (486, 480, 404, 603) | |
| Test numbers and a test window, and whether test calls are billed | for step 6 | |

For promotional calls in India also confirm with the carrier and your legal adviser: which number
series you must call from, registration requirements for the business, the permitted calling hours
(the app enforces 09:00 to 21:00 IST per campaign), and how to obtain and refresh the do-not-disturb
register (the app can import it, see below). **This project does not provide legal advice.**

## 2. Install and configure

On a Linux host with Docker (**unverified**; development was on Docker Desktop for Windows):

1. Copy `infra/carrier.env.example` to `infra/carrier.env` and fill it in. It is gitignored.
2. Set the LiveKit key and secret to real random values in `infra/livekit/livekit.yaml` and
   `infra/livekit/sip.yaml` (the checked-in ones are development defaults), and set
   `LIVEKIT_API_KEY`/`LIVEKIT_API_SECRET` for the API and worker to match.
3. Change `AGENT_SHARED_SECRET` and `WEBHOOK_SIGNING_SECRET` from their development defaults.
4. Put `DEEPGRAM_API_KEY`, `CARTESIA_API_KEY` and `OPENAI_API_KEY` in `infra/agent.env`.
5. `pnpm infra:carrier` starts LiveKit, the SIP service, the agent and the Asterisk bridge.
6. Create the LiveKit trunk that points at Asterisk, and put its id in the API environment:
   ```
   pnpm livekit-trunk create --address asterisk:5060 --number +91XXXXXXXXXX
   # LIVEKIT_SIP_TRUNK_ID=ST_...   DIALER=livekit
   ```
   The `--number` is the caller ID LiveKit uses when a campaign names none.
7. Set the limits that protect the carrier and the agent (section 4):
   `CARRIER_CPS`, `MAX_GLOBAL_CHANNELS`, `ALLOWED_CALLER_IDS`, `DEFAULT_CALLER_ID`.
8. Import the do-not-disturb register as an admin: `POST /api/ncpr/import` (see README).

### Network and firewall

Everything in the Docker network talks freely. On a real host you must also:

- Open the **Asterisk RTP range** (`RTP_START` to `RTP_END`, default 30000 to 32000/udp) and SIP
  (5060/udp) **to the carrier only**.
- Run Asterisk and the LiveKit SIP service with **host networking** or an explicit public address.
  Set `EXTERNAL_ADDRESS` (and `LOCAL_NET`) in `carrier.env` if Asterisk is behind NAT, and
  `use_external_ip` / the public IP in `infra/livekit/sip.yaml`. Media will not flow (one-way or no
  audio) if the advertised address is wrong. **Unverified.**
- Set `LIVEKIT_SIP_MATCH` in `carrier.env` to the address of the LiveKit SIP service, so Asterisk
  accepts calls only from it. The default allows any Docker network, which is fine locally only.
- Keep the LiveKit SIP service's RTP range (`rtp_port` in `sip.yaml`) at 10000 to 20000 or sized for
  your peak. A small range fails under load with `failed to listen on udp port` (this happened
  at 100 ports and 20 concurrent calls).

## 3. How failures reach the app

The bridge passes the carrier's answer back to LiveKit unchanged, and the app reacts to each kind
differently. Verified against simulated carriers, not a real one:

| Carrier does | App records | Retried? |
|---|---|---|
| busy (486/600) or declines (603) | `busy` | no |
| unavailable (480) or rings out (no answer) | `no_answer` | no |
| invalid number (404, 410, 484, 604) | `failed` | no |
| refuses *us* (401/403/407), errors (5xx), or is unreachable | infrastructure error | yes, with backoff, then the dead-letter queue |

A dead carrier takes about 35 seconds to be detected (SIP Timer B), then calls retry and dead-letter
instead of being marked failed. After fixing the cause, replay them:
`python -m app.cli dlq list`, then `dlq replay`.

Keep `RING_SECONDS` (default 38) above 33 and below `LIVEKIT_RING_TIMEOUT` (40) so a dead carrier is
told apart from a ringing phone. Turn the OPTIONS probe on (`PULSE_QUALIFY_SECONDS=30`) **only** if the
carrier answers OPTIONS: while a probe goes unanswered Asterisk refuses every call.

## 4. Capacity: measure it, do not guess

The two numbers that matter are `MAX_GLOBAL_CHANNELS` (concurrent calls) and `CARRIER_CPS`. They must be
at or below **both** the carrier's contract **and** what your agent fleet can carry. If a customer
answers and no agent is available they hear silence and the call is hung up, which is the worst outcome
for a cold-calling system.

`python -m app.loadtest` pushes a synthetic campaign through the running system and checks the limits
held, using the database for exact timings:

```
uv run --directory apps/api python -m app.loadtest --calls 200 --concurrency 30 --cps 8 \
    --global-limit 30 --email you@example.com --password '...' --report load.json --cleanup
```

`--global-limit` must equal the worker's `MAX_GLOBAL_CHANNELS`. It exits non-zero if any check fails:
every call finished, peak concurrency within the limit, starts within CPS in any one second, every
channel slot returned, nothing dead-lettered.

### What was measured (development laptop, everything on one Docker VM, stub agent)

| Setup | Result |
|---|---|
| Worker + simulated dialer, 500 calls, 20 cps, 50 concurrent | all limits held. Achieved about 90% of the CPS limit (about 18/s), and never more than 20 starts in any one second |
| Full chain (LiveKit, Asterisk, agent), 20 concurrent, 5 cps, 120 calls | 120 completed |
| Full chain, 30 concurrent, 3 cps, 150 calls | 150 completed |
| Full chain, 30 concurrent, 5 cps, 150 calls | 150 completed |
| Full chain, 30 concurrent, 8 cps, 200 calls | 200 completed (5 agent dispatches needed a retry) |

Treat these as a **floor for the stub agent on one machine**, not a forecast. The real agent (speech
recognition, model, speech synthesis, voice-activity detection) costs more CPU per call, and a
production host will differ. Repeat the load test on the real hardware **with the real agent and real
provider keys**, and set the limits from that.

### What broke under load, and the fixes (keep these in mind when sizing)

- **LiveKit SIP media ports** (`rtp_port`): 100 ports capped it near 20 concurrent calls. Now 10000 to 20000.
- **Asterisk media ports** (`RTP_START`/`RTP_END`): a bridged call holds 4 ports and keeps them briefly
  after hangup. 100 ports capped it near 25 calls. Now 2000 (about 400 calls).
- **Warm agent processes** (`AGENT_IDLE_PROCESSES`, default 16): starting one takes seconds. If new calls
  arrive faster than the pool refills, callers wait. Size it to roughly new-calls-per-second times 3.
  The agent also refuses new jobs when its own CPU is high ("could not find any available nodes"); the
  dialer retries the dispatch twice before giving up.
- **Agent CPU** was the first resource to run out, ahead of LiveKit, the SIP service and the app.
  The worker, queue and database stayed far from their limits.

## 5. Safety nets already in place

- Caller ID must be on `ALLOWED_CALLER_IDS`, checked when the campaign is made and again before each dial.
- Calling window 09:00 to 21:00 (per campaign), the DNC list, and the NCPR register are checked when a
  call is queued and again just before it is dialled.
- If the agent does not join within 10 seconds of the customer answering, the call is hung up and marked
  failed (and the channel slot freed), never left silent.
- A worker that loses Redis (restart or flush) recovers by itself; calls queued in the database are
  re-enqueued. Channel leases expire on their own if a worker dies.
- Opt-out phrases ("stop calling me", "remove my number") hang up and add the number to the DNC list,
  independent of the language model.

## 6. First real calls

Do these in order, and stop at the first surprise.

1. **Bridge only, no agent.** From the Asterisk container, place one call to a number you control and
   watch `docker logs <asterisk>` (the dialplan logs the carrier's answer: `dial result: status=...
   carrier-sip=...`). Confirm the called-number format, the caller ID the phone displays, and audio both ways.
2. **One call through the app**, `POST /api/calls`, to your own phone with `DIALER=livekit`. Check the
   call's page for the outcome and transcript.
3. **Failure cases**: a number that is busy, one that you let ring out, and one that does not exist.
   Confirm the statuses match the table in section 3.
4. **Browser test call** (the Test call page) to tune the script and voice before any customer hears it.
5. **A 10-contact campaign** of consenting people, at low CPS and concurrency.
6. **Load test** at the limits you intend to run, with the real agent.
7. Only then raise the limits, in steps, watching answer rates and failures.

Rollback is `pause` on the campaign (calls already in flight finish) and, for an emergency, stopping the
worker: nothing dials without it.

## 7. Not done / unverified

- Everything about the real carrier: authentication mode, number format, caller-ID handling, SIP
  behaviour on their side, and billing.
- Production hosting: host networking, NAT/external-address handling, firewall rules, TLS/SRTP between
  the legs. The compose file is a development environment.
- The real agent under load (all numbers above are for the stub).
- Call recording. Not built; transcripts are stored, audio is not.
- Monitoring and alerting. The app logs and exposes campaign counts, but there are no dashboards or
  alerts on failure rate, dead-letter depth, or channel saturation. Add them before unattended runs.
