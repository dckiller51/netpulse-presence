# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/).

## [2.0.0] - 2026-07-05

### Changed

- **Breaking:** Wi-Fi tracking is now always network-wide. The per-AP
  `local` polling mode has been removed entirely, along with the
  `wifi_tracking_mode`, `wifi_poll_interval`, and `wifi_grace_seconds`
  config keys — there is no longer a choice to make here. The main
  router always runs `CentralWifiTracker`; satellite APs run
  `enable_wifi_heartbeat` instead of a tracker of their own.

### Added

- `syslog-ng-router.conf.example` and `syslog-ng-satellite.conf.example`
  — real, working `/etc/syslog-ng.conf` examples for both roles.

### Removed

- The `WifiTracker` (local per-AP polling) class.
- `uci_dhcp_file` support and its config key. In practice the router
  serving DHCP already resolves every device's hostname via
  `dhcp_leases_file` regardless of which AP it's connected to, making
  this redundant.

### Fixed

- DHCP lease loading is now skipped entirely on a router running only
  `enable_wifi_heartbeat`, instead of doing pointless work on every start.
- `WifiHeartbeat` and the main router's `_local_resync` no longer mistake
  a single transient `ubus` failure for a mass disconnect: a client on an
  interface that failed to respond this cycle is left alone rather than
  declared gone, a total `ubus` failure skips the cycle outright, and
  `wifi_heartbeat_miss_threshold` requires several consecutive misses
  before the heartbeat gives up on a client.
- Corrected the "Satellite AP setup" instructions: a satellite AP does
  run its own `syslog-ng`, forwarding events to the main router via an
  explicit `network()` destination — it is not just OpenWrt's stock
  `logd` UCI forwarding as previously (incorrectly) documented.

## [1.0.0] - 2026-07-04

Initial public release.

### Added

- Event-driven Wi-Fi presence detection (`WifiTracker`), polling
  `ubus call hostapd.<iface> get_clients` on a short interval.
- Event-driven wired presence detection (`WiredTracker`), consuming a
  live `ip monitor neigh` stream with an optional ICMP ping watchdog for
  devices that go quiet without a neighbor-table transition.
- Wi-Fi/wired de-duplication across routers via `WifiMacRegistry`, so a
  Wi-Fi client isn't misclassified as wired on a shared `br-lan` bridge.
- MQTT transport publishing standard Home Assistant MQTT-discovery
  payloads — `device_tracker` entities appear automatically, no custom
  component required.
- `central` Wi-Fi tracking mode (`CentralWifiTracker`) for multi-AP
  networks: one router ingests `AP-STA-CONNECTED`/`AP-STA-DISCONNECTED`
  syslog events from every AP on the network and becomes the single
  source of truth, eliminating roaming flicker between APs.
- Central mode self-healing, to match the reliability `local` mode gets
  for free by re-polling the full client list every cycle:
  - **Local resync** (`wifi_central_local_resync_seconds`) — periodically
    re-checks the central router's own hostapd interfaces via `ubus` and
    corrects any drift from a lost syslog line.
  - **AP-restart detection** — reacts to hostapd's `AP-ENABLED` line to
    re-validate every client attributed to a radio the instant it
    restarts, catching the clients a rebooting AP never gets to log a
    clean disconnect for.
  - **Satellite heartbeat** (`enable_wifi_heartbeat`) — an optional
    lightweight mode a satellite AP runs instead of its own tracker,
    periodically re-announcing its own hostapd clients over syslog using
    the exact wording of a real hostapd event, so the central tracker can
    self-heal a lost event on a satellite it has no `ubus` access to.
    Debounced via `wifi_heartbeat_miss_threshold` so a single transient
    `ubus` hiccup is never mistaken for a real disconnect.
- Static-lease-based and whitelist-based wired tracking filters.
