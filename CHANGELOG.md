# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/).

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
- `uci_dhcp_file` support for hostname resolution on satellite APs with
  no local DHCP server.
