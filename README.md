# NetPulse Presence

Event-driven Wi-Fi & wired device presence detection for OpenWrt routers,
designed for Home Assistant.

No polling, anywhere that matters. `netpulse-presence.py` listens directly
to kernel and daemon event streams on the router and pushes state changes
over MQTT the instant they happen. Home Assistant needs no custom
integration — its built-in MQTT integration auto-discovers every tracked
device as a standard `device_tracker` entity.

## Why this exists

Polling-based OpenWrt integrations periodically ask "is this device still
here?" on a timer, using independent caches for Wi-Fi and wired presence
that can drift out of sync with each other — causing wired devices to get
stuck "home" long after they disconnect, Wi-Fi devices to flicker through a
false "wired" state during normal disconnects, and config changes that
silently don't take effect until a full restart.

NetPulse Presence avoids this whole class of bugs by construction: there is
one source of truth per event type, and Home Assistant only ever reacts to
a message — it never asks a question.

## How it works

- **Wi-Fi** — one router (your main router) runs `CentralWifiTracker`,
  which ingests `AP-STA-CONNECTED`/`AP-STA-DISCONNECTED` events for
  *every* AP on the network — itself and any satellite AP — over syslog,
  and is the single source of truth for Wi-Fi presence network-wide. A
  roam between two APs becomes one ordered pair of events instead of two
  independent trackers racing their own timers, so there's no flicker.
  Satellite APs don't run their own tracker at all — they just forward
  their syslog to the main router, optionally paired with a lightweight
  heartbeat (`enable_wifi_heartbeat`) that self-heals a lost event — see
  [Wi-Fi tracking](#wi-fi-tracking-central) below.
- **Wired** — a `WiredTracker` consumes a live `ip monitor neigh` stream, a
  genuine kernel event source that emits a line the instant any ARP/NDP
  neighbor entry changes state. An optional ICMP ping watchdog
  (`wired_ping_interval`) catches devices that go quiet without ever
  triggering a neighbor-table transition (e.g. a powered-off NAS).
- **Wi-Fi/wired de-duplication** — since Wi-Fi and wired clients share the
  same `br-lan` bridge, a `WifiMacRegistry` is used so the wired tracker
  never misclassifies a Wi-Fi client as wired. Each router broadcasts its
  own Wi-Fi sightings over MQTT so this works correctly even when Wi-Fi
  tracking and wired tracking run on different physical routers.
- **Transport** — MQTT, publishing standard Home Assistant MQTT-discovery
  payloads, so `device_tracker` entities appear automatically with no
  YAML and no custom component.

## Repository contents

| File | Purpose |
|---|---|
| `netpulse-presence.py` | The presence-detection daemon itself. |
| `netpulse-presence` | OpenWrt `/etc/init.d` procd service script. |
| `netpulse-presence.settings.json.example` | Example configuration file. |
| `syslog-ng-router.conf.example` | Example `/etc/syslog-ng.conf` for the main router — receives events forwarded by every satellite AP. |
| `syslog-ng-satellite.conf.example` | Example `/etc/syslog-ng.conf` for a satellite AP — forwards its own events to the main router. |

> This repository contains only the router-side agent. It does not include
> a Home Assistant custom component — none is needed, since the script
> publishes standard HA MQTT-discovery payloads that Home Assistant's
> built-in MQTT integration already understands. See
> [Home Assistant setup](#home-assistant-setup) below.

## Architecture

```
┌───────────────────────┐         MQTT           ┌───────────────────────┐
│     OpenWrt router     │ ─────────────────────▶ │     Home Assistant     │
│                         │  netpulse/<mac>/state  │                         │
│  netpulse-presence.py   │  netpulse/<mac>/       │   MQTT integration      │
│                         │    attributes          │   (built-in)            │
│  - hostapd (Wi-Fi)      │  netpulse/<router>/    │                         │
│  - ip monitor neigh     │    status              │   device_tracker         │
│    (wired)              │  homeassistant/        │   entities, auto-        │
│                         │    device_tracker/...  │   discovered via MQTT   │
└───────────────────────┘                          └───────────────────────┘
```

Run `netpulse-presence.py` on **every** router/AP whose Wi-Fi clients you
want to track. For wired tracking, run it (with `enable_wired_tracking:
true`) only on the router that actually owns `br-lan`'s DHCP/ARP table —
typically just your main router, not satellite APs in bridge mode.

## Installation

1. Copy the files to the router. The init script's `PROG` points at
   `/etc/config/netpulse-presence.py`, so the daemon has to live there:
   ```sh
   scp netpulse-presence.py root@<router>:/etc/config/netpulse-presence.py
   scp netpulse-presence root@<router>:/etc/init.d/netpulse-presence
   scp netpulse-presence.settings.json.example root@<router>:/etc/config/netpulse-presence.settings.json
   ```
2. Install the MQTT client library:
   ```sh
   opkg update && opkg install python3-paho-mqtt
   ```
3. Edit `/etc/config/netpulse-presence.settings.json` — at minimum set
   `mqtt_host`, `mqtt_username`/`mqtt_password`, and `router_name`.
4. On your **main router only**, set `"enable_wired_tracking": true`. Leave
   it `false` on satellite APs to avoid tracking the same wired devices
   twice through the same `br-lan` bridge.
5. Enable and start the service:
   ```sh
   chmod +x /etc/config/netpulse-presence.py /etc/init.d/netpulse-presence
   service netpulse-presence enable
   service netpulse-presence start
   ```
6. Check logs:
   ```sh
   logread -f | grep netpulse-presence
   ```

## Configuration reference

All settings live in `/etc/config/netpulse-presence.settings.json`; any key
you omit falls back to its default below.

### General

| Key | Default | Description |
|---|---|---|
| `mqtt_host` | `homeassistant.local` | MQTT broker address. |
| `mqtt_port` | `1883` | MQTT broker port. |
| `mqtt_username` / `mqtt_password` | `""` | MQTT credentials. |
| `mqtt_base_topic` | `netpulse` | Topic prefix for all published messages. |
| `router_name` | hostname | Identifies this router in topics/attributes. |
| `dhcp_leases_file` | `/tmp/dhcp.leases` | dnsmasq leases file, for hostname/IP resolution. Only read when `enable_wifi_tracking` or `enable_wired_tracking` is `true` — irrelevant on a satellite running only `enable_wifi_heartbeat`. |
| `log_level` | `INFO` | Python logging level. |

### Wi-Fi tracking (central)

Wi-Fi presence is always tracked network-wide by a single router — there's
no per-AP polling mode to choose between. Run these settings on your
**main router only**; satellite APs use [`enable_wifi_heartbeat`](#satellite-ap-setup)
instead.

| Key | Default | Description |
|---|---|---|
| `enable_wifi_tracking` | `true` | Enable Wi-Fi presence tracking. Set to `true` on the main router only — `false` on every satellite AP. |
| `wifi_central_grace_seconds` | `60` | Seconds to wait after an `AP-STA-DISCONNECTED` before declaring `not_home`, unless a connect event for that MAC (on any AP) arrives first. |
| `wifi_central_syslog_source_pattern` | `WifiAP-\w+` | Regex matching the syslog hostname field identifying which AP a log line came from. |
| `wifi_central_ap_timeout_seconds` | `1200` | Drop an AP's reported clients if it hasn't logged anything in this long (dead-AP watchdog). |
| `wifi_central_local_resync_seconds` | `90` | Periodically re-checks *this router's own* hostapd interfaces via `ubus` and corrects any drift from a lost syslog line — re-confirming clients the event stream missed, or starting the grace-period departure for clients whose disconnect event never arrived. Set to `0` to disable. See [Handling missed events](#handling-missed-events) below. |
| `enable_wifi_heartbeat` | `false` | Enable **only on a satellite AP**, *instead of* `enable_wifi_tracking`. Periodically re-announces this AP's own hostapd clients over syslog so the central router can self-heal a lost event on a satellite it has no `ubus` access to. See [Handling missed events](#handling-missed-events). |
| `wifi_heartbeat_interval` | `60` | Seconds between heartbeat re-announcements, when `enable_wifi_heartbeat` is `true`. |
| `wifi_heartbeat_miss_threshold` | `2` | Consecutive missed cycles required before the heartbeat declares a client actually gone. Protects against a single transient `ubus` hiccup being mistaken for a real disconnect; a `ubus` failure that prevents checking an interface at all is never counted as a miss. |

### Wired tracking

| Key | Default | Description |
|---|---|---|
| `enable_wired_tracking` | `false` | Enable wired presence tracking. Only enable on the router that owns `br-lan`'s DHCP/ARP table. |
| `wired_static_leases_only` | `true` | Only track devices with a static DHCP lease already configured in LuCI. |
| `wired_whitelist` | `[]` | MAC/prefix whitelist, used instead when `wired_static_leases_only` is `false`. |
| `wired_interfaces` | `["br-lan"]` | Interfaces to watch for neighbor-table changes. |
| `wired_grace_seconds` | `30` | Seconds to wait after a `FAILED`/deleted neighbor entry before declaring `not_home`. |
| `wired_ping_interval` | `30` | Seconds between active ping sweeps of currently-home wired devices. Set to `0` to disable and rely on passive ARP/NDP only. |
| `wired_ping_timeout` | `2` | Seconds to wait for a ping reply. |
| `wired_ping_failures` | `3` | Consecutive ping failures before declaring `not_home`. |

### Wired tracking and static leases

By default (`wired_static_leases_only: true`), only devices with a static
DHCP lease already configured in LuCI (Network → DHCP and DNS → Static
Leases) are tracked. This needs no manual MAC list: adding or removing a
static lease on the router automatically changes what gets tracked, the
next time that device's neighbor entry changes state.

If you'd rather use an explicit MAC/IP-prefix whitelist instead, set
`wired_static_leases_only: false` and fill in `wired_whitelist`.

### Satellite AP setup

Wi-Fi presence is always tracked network-wide, from a single router — the
main router ingests `AP-STA-CONNECTED`/`AP-STA-DISCONNECTED` events for
*every* AP on the network (itself and any satellite APs) via `logread -f`,
and is the single source of truth for Wi-Fi presence. A roam between two
APs becomes one ordered pair of events, not two independent trackers
racing their own timers — no flicker.

Both the main router and every satellite AP run `syslog-ng` — the router
to *receive* everyone's events, satellites to *forward* their own. To set
up a satellite AP:

1. **On the main router**, install `syslog-ng` and use
   [`syslog-ng-router.conf.example`](syslog-ng-router.conf.example) as
   `/etc/syslog-ng.conf`. It listens on UDP 514 for events forwarded by
   every satellite, and merges in this router's own local events too:
   ```sh
   opkg update
   opkg install syslog-ng
   ```
   On some OpenWrt versions/feeds, `syslog-ng` and the stock `logd` both
   ship a `/sbin/logread`, so `opkg` refuses to install one while the
   other is present. If you hit that error, remove `logd` first:
   ```sh
   opkg remove logd
   opkg install syslog-ng
   ```
   On other versions the two coexist fine (this is the more common case
   in practice) — `logd` keeps running as normal, and `syslog-ng` just
   listens on 514 alongside it. Only remove `logd` if `opkg` actually
   reports a conflict; doing it unconditionally can change how `logread`
   behaves (an in-memory-only `logd` buffer vs. whatever `syslog-ng`'s own
   `logread` substitute reads from, e.g. a file).
2. **On each satellite AP**, install `syslog-ng` the same way, and use
   [`syslog-ng-satellite.conf.example`](syslog-ng-satellite.conf.example)
   as `/etc/syslog-ng.conf`, with `IP_ROUTER` replaced by the main
   router's actual LAN IP. It forwards this AP's own local events to the
   main router over UDP 514 — it does not need to receive anything.
3. Restart `syslog-ng` on both after editing its config:
   ```sh
   /etc/init.d/syslog-ng restart
   ```
4. Satellite APs must **not** run their own `CentralWifiTracker` — set
   `"enable_wifi_tracking": false` on them. The main router is the only
   source of truth. Optionally, also set `"enable_wifi_heartbeat": true`
   on each satellite — see [Handling missed events](#handling-missed-events)
   below.

Both example configs write locally to `/var/log/messages` (a real file,
not the in-memory buffer OpenWrt's stock `logd` uses) — confirm
`mount | grep /var` shows `tmpfs` if you want to keep this RAM-only.

### Handling missed events

`central` mode is purely event-driven: it only reacts to
`AP-STA-CONNECTED`/`AP-STA-DISCONNECTED` lines. Two failure modes are
handled specifically:

- **A single connect/disconnect line gets lost** (dropped UDP syslog
  packet, brief network blip) while the AP itself keeps running. `local`
  mode doesn't have this problem, since it re-polls the *complete* client
  list every cycle — a missed cycle just gets corrected by the next one.
  Central mode closes most of this gap with a periodic local resync
  (`wifi_central_local_resync_seconds`, default 90s): it re-queries this
  router's own hostapd interfaces directly via `ubus` — the same call
  `local` mode uses — and reconciles any drift. This fully self-heals the
  radios on the router that's *running* the central tracker (commonly your
  main router, which is often an AP itself), but not a remote satellite's,
  since the main router has no `ubus` access to a device it isn't running
  on.

- **A satellite AP reboots.** hostapd dies without ever logging a clean
  `AP-STA-DISCONNECTED` for whoever was connected, so those clients would
  otherwise stay stuck `home` indefinitely. This is caught directly:
  hostapd logs an `AP-ENABLED` line the moment a radio starts beaconing
  again after a restart, and every client still shown as `home` on that
  exact station/interface is put through the normal grace-period
  departure the instant that line arrives — a device that's genuinely
  still there (or that reconnects anywhere within the grace window) has
  that timer cancelled by its own fresh `AP-STA-CONNECTED`, exactly like
  any other roam, so nothing changes for it. This needs no `ubus` access
  to the satellite at all — it works from the syslog line alone.

Between the two, the only case that isn't fully self-healing **on the main
router alone** is a satellite AP that stays running the whole time yet
drops one specific event on the floor:

- **A satellite AP drops an isolated event while staying up.** Neither
  `local_resync` (no `ubus` access to a remote device) nor the
  `AP-ENABLED` handling (nothing restarted) can catch this from the main
  router's side. The fix has to run on the satellite itself: set
  `"enable_wifi_heartbeat": true` on it (instead of
  `enable_wifi_tracking`). Every `wifi_heartbeat_interval` seconds, that
  satellite queries its own hostapd clients via `ubus` — the same call
  `local_resync` uses — and re-announces them over syslog using the exact
  wording a real `AP-STA-CONNECTED`/`AP-STA-DISCONNECTED` line uses. That
  synthetic line flows through the same syslog-ng forwarding you already
  set up and is parsed by the central tracker exactly like a genuine
  hostapd event — no central-side changes needed, and an already-`home`
  client is a cheap no-op there, so this is safe to leave running
  continuously.

With `wifi_central_local_resync_seconds` (main router), `AP-ENABLED`
handling (any router, including satellites), and `enable_wifi_heartbeat`
(satellites) combined, `central` mode has the same self-healing property
`local` mode gets for free — just spread across the pieces that actually
have the information needed to provide it.

## MQTT topic reference

| Topic | Payload | Retained |
|---|---|---|
| `netpulse/<router_name>/status` | `online` / `offline` | yes (LWT) |
| `netpulse/<mac>/state` | `home` / `not_home` | yes |
| `netpulse/<mac>/attributes` | JSON: `mac`, `source_type`, `source_router`, `ip`, `hostname`, `ssid` | yes |
| `netpulse/_internal/wifi_seen/<router_name>` | MAC address | no |
| `homeassistant/device_tracker/netpulse_<mac>/config` | HA MQTT discovery payload | yes |

## Home Assistant setup

No custom component is required.

1. Make sure the **MQTT integration** is already configured in Home
   Assistant (Settings → Devices & Services → MQTT), pointing at the same
   broker as your routers.
2. That's it. As soon as a router publishes its first discovery payload for
   a device, a `device_tracker` entity appears automatically, grouped
   under a single "NetPulse Presence" device in Settings → Devices.

## Troubleshooting

- Tail the daemon's logs on a router: `logread -f | grep netpulse-presence`.
- If a device never appears in Home Assistant, confirm the router can
  reach the MQTT broker (`netpulse/<router_name>/status` should read
  `online`) and that the device actually matches your wired/Wi-Fi tracking
  filters (static lease, whitelist, tracked interface, etc.).
- If a satellite AP's clients aren't being picked up, verify that AP's syslog is actually reaching the main router
  (`wifi_central_syslog_source_pattern` must match its hostname field).

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## License

MIT
