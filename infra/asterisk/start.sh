#!/bin/bash
# Renders the Asterisk config from environment variables, then starts Asterisk.
# Everything carrier-specific is an environment variable (see infra/carrier.env.example), so
# moving from the local test carrier to the real one needs no rebuild.
set -euo pipefail

: "${PULSE_HOST:=sipp}"                 # carrier SIP host (dev default: the sipp test container)
: "${PULSE_PORT:=5070}"
: "${PULSE_AUTH:=ip}"                   # ip | userpass | register
: "${PULSE_USERNAME:=}"
: "${PULSE_PASSWORD:=}"
: "${PULSE_QUALIFY_SECONDS:=0}"         # OPTIONS keepalive to the carrier. 0 = off (default).
                                        # ONLY enable if the carrier answers OPTIONS: while a probe
                                        # goes unanswered Asterisk refuses to place any call.
: "${PULSE_SEND_PAI:=no}"               # yes = also send P-Asserted-Identity with the caller ID
: "${LIVEKIT_SIP_MATCH:=172.16.0.0/12}" # who may send calls in: the LiveKit SIP service's address
: "${DIAL_KEEP_PLUS:=yes}"
: "${DIAL_PREFIX:=}"
: "${RING_SECONDS:=38}"                 # how long the customer's phone may ring. Keep it above 33 s (SIP
                                        # Timer B, so a dead carrier is told apart from a ringing phone)
                                        # and below LIVEKIT_RING_TIMEOUT (40 s).
: "${RTP_START:=30000}"             # media ports: a bridged call holds 4 (RTP+RTCP on each leg) and
: "${RTP_END:=32000}"               # keeps them briefly after hangup, so 2000 ports is ~400 calls
: "${EXTERNAL_ADDRESS:=}"               # public IP, only when Asterisk sits behind NAT
: "${LOCAL_NET:=}"                      # private range(s) not to be treated as NAT, e.g. 10.0.0.0/8

case "$PULSE_AUTH" in
  ip) ;;
  userpass|register)
    if [ -z "$PULSE_USERNAME" ] || [ -z "$PULSE_PASSWORD" ]; then
      echo "start.sh: PULSE_AUTH=$PULSE_AUTH needs PULSE_USERNAME and PULSE_PASSWORD" >&2; exit 1
    fi ;;
  *) echo "start.sh: PULSE_AUTH must be ip, userpass or register (got '$PULSE_AUTH')" >&2; exit 1 ;;
esac

NAT_BLOCK=""
if [ -n "$EXTERNAL_ADDRESS" ]; then
  NAT_BLOCK="external_media_address=$EXTERNAL_ADDRESS"$'\n'"external_signaling_address=$EXTERNAL_ADDRESS"
  if [ -n "$LOCAL_NET" ]; then
    for net in ${LOCAL_NET//,/ }; do NAT_BLOCK+=$'\n'"local_net=$net"; done
  fi
fi

CARRIER_ENDPOINT_EXTRA=""
CARRIER_AUTH_BLOCK=""
if [ "$PULSE_AUTH" != "ip" ]; then
  CARRIER_ENDPOINT_EXTRA="outbound_auth=carrier-auth"
  CARRIER_AUTH_BLOCK="[carrier-auth]
type=auth
auth_type=userpass
username=$PULSE_USERNAME
password=$PULSE_PASSWORD"
fi
if [ "$PULSE_AUTH" = "register" ]; then
  CARRIER_AUTH_BLOCK+="

[carrier-registration]
type=registration
outbound_auth=carrier-auth
server_uri=sip:$PULSE_HOST:$PULSE_PORT
client_uri=sip:$PULSE_USERNAME@$PULSE_HOST
retry_interval=30
expiration=300"
fi
if [ "$PULSE_SEND_PAI" = "yes" ]; then
  CARRIER_ENDPOINT_EXTRA+=$'\n'"send_pai=yes"$'\n'"trust_id_outbound=yes"
fi

export PULSE_HOST PULSE_PORT PULSE_QUALIFY_SECONDS LIVEKIT_SIP_MATCH DIAL_KEEP_PLUS DIAL_PREFIX \
       RING_SECONDS RTP_START RTP_END NAT_BLOCK CARRIER_ENDPOINT_EXTRA CARRIER_AUTH_BLOCK

# Only substitute our own variables: the dialplan is full of ${...} that Asterisk must keep.
VARS='$PULSE_HOST $PULSE_PORT $PULSE_QUALIFY_SECONDS $LIVEKIT_SIP_MATCH $DIAL_KEEP_PLUS $DIAL_PREFIX
$RING_SECONDS $RTP_START $RTP_END $NAT_BLOCK $CARRIER_ENDPOINT_EXTRA $CARRIER_AUTH_BLOCK'
for f in pjsip extensions rtp modules; do
  envsubst "$VARS" < "/templates/$f.conf.tmpl" > "/etc/asterisk/$f.conf"
done
echo "start.sh: carrier=$PULSE_HOST:$PULSE_PORT auth=$PULSE_AUTH keep_plus=$DIAL_KEEP_PLUS prefix='${DIAL_PREFIX}'"

exec /usr/local/bin/entrypoint.sh "$@"
