#!/usr/bin/env python3
"""
netpulse-presence
==================

Event-driven Wi-Fi and wired presence detector for OpenWrt, designed to feed
the "netpulse" Home Assistant custom component over MQTT.

Unlike polling-based trackers, this script never asks "is this device still
here?" on a timer. Instead it listens directly to the kernel/daemon event
streams that already know the answer the instant it changes:

  - Wi-Fi: ubus hostapd.<iface> events (a client associates/disassociates).
    Implemented as a short poll loop over `ubus call hostapd.* get_clients`
    on OpenWrt builds where this proc model is more portable than wiring up
    a true ubus event subscription from a single-file script. The poll
    interval is short (default 2s) and is NOT what we mean by "polling" in
    the sense that caused problems in poll-based integrations: there is only
    ONE always-fresh source of truth here (the current hostapd client list),
    so there is no possibility of two independent caches drifting apart.

  - Wired: `ip monitor neigh` (IPv4 and IPv6), which is a genuine kernel
    event stream - it blocks and prints a line the instant any ARP/NDP
    neighbor entry changes state. No polling at all on this path.

  - Transport: MQTT, with Home Assistant MQTT discovery so device_tracker
    entities appear automatically with no YAML and no custom component code
    required to register them.

Run as a service (see netpulse-presence.init for OpenWrt procd integration).
"""

import json
import logging
import re
import socket
import subprocess
import sys
import threading
import time
import signal
from collections import defaultdict
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: python3-paho-mqtt is required. Install with:", file=sys.stderr)
    print("   opkg update && opkg install python3-paho-mqtt", file=sys.stderr)
    sys.exit(1)


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

CONFIG_PATH = Path("/etc/config/netpulse-presence.settings.json")

DEFAULT_CONFIG = {
    "mqtt_host": "homeassistant.local",
    "mqtt_port": 1883,
    "mqtt_username": "",
    "mqtt_password": "",
    "mqtt_base_topic": "netpulse",
    "router_name": "",  # if empty, falls back to the device hostname
    "enable_wifi_tracking": True,
    "enable_wired_tracking": False,
    # This router (and ONLY this router should have enable_wifi_tracking
    # on) ingests AP-STA-CONNECTED / AP-STA-DISCONNECTED events for ALL
    # access points - itself plus any satellite AP - via syslog (see
    # wifi_central_syslog_source_pattern below), and is the single source
    # of truth for Wi-Fi presence network-wide: a roam between two APs is
    # one ordered pair of events, never two independent trackers racing.
    # Satellite APs must NOT run their own CentralWifiTracker; set
    # enable_wifi_tracking=False on them and use enable_wifi_heartbeat
    # instead (see below) - they only need their local syslog forwarded
    # to this router (External system log server = this router's IP, see
    # syslog-ng setup notes near CentralWifiTracker).
    # Regex matching the syslog hostname/source field that identifies
    # which AP a given log line came from. The default matches any
    # "WifiAP-NN"-style hostname; adjust if your AP naming differs.
    "wifi_central_syslog_source_pattern": r"WifiAP-\w+",
    # How long (seconds) to wait after an AP-STA-DISCONNECTED before
    # declaring a device not_home, UNLESS a connect event for that same MAC
    # (on any AP) arrives first - which is the whole point of the central
    # mode: a same-MAC reconnect anywhere cancels the pending departure
    # immediately, regardless of which AP it was on before.
    "wifi_central_grace_seconds": 60,
    # If an AP hasn't sent ANY syslog traffic in this many seconds, it's
    # considered dead/unreachable, and its previously-reported clients are
    # dropped (since we can no longer trust their last known state).
    # Equivalent to disconnectIdxBumper's "dead AP" watchdog.
    "wifi_central_ap_timeout_seconds": 1200,
    # Central mode is otherwise PURELY event-driven (AP-STA-CONNECTED /
    # AP-STA-DISCONNECTED over syslog): if a single event line is lost
    # (a dropped UDP syslog packet, a syslog-ng restart, a brief network
    # blip) a device can stay stuck "home" forever, since nothing will
    # ever tell the tracker it left. This periodically re-checks this
    # router's OWN local hostapd interfaces via ubus and reconciles any
    # drift: it re-confirms clients the event stream missed, and starts
    # the normal grace-period departure for clients the event stream
    # never told us left. This only self-heals THIS router's own radios -
    # a satellite AP's clients still rely on its syslog events reaching us
    # (see wifi_central_ap_timeout_seconds for the "AP gone completely
    # dark" case). Set to 0 to disable.
    "wifi_central_local_resync_seconds": 90,
    # Run this on a SATELLITE AP (alongside enable_wifi_tracking=False) to
    # close the one remaining gap wifi_central_local_resync_seconds can't
    # reach: a lost individual event on a router the central tracker has
    # no ubus access to. Every wifi_heartbeat_interval seconds, this
    # router re-announces its own currently-authorized hostapd clients
    # over syslog using the exact same wording a real
    # AP-STA-CONNECTED/AP-STA-DISCONNECTED line uses - so it flows through
    # the same syslog-ng forwarding and is parsed by CentralWifiTracker
    # exactly like a genuine hostapd event, no central-side changes
    # needed. Safe to enable even where enable_wifi_tracking is False.
    "enable_wifi_heartbeat": False,
    "wifi_heartbeat_interval": 60,
    # Require this many CONSECUTIVE missed cycles before treating a
    # previously-seen client as actually gone. Without this, a single
    # transient `ubus` failure on one poll (CPU spike, timeout, etc.)
    # would look identical to a real disconnect and cause a FALSE
    # AP-STA-DISCONNECTED - the opposite of what the heartbeat is for.
    "wifi_heartbeat_miss_threshold": 2,
    "wired_static_leases_only": True,
    "wired_whitelist": [],
    "wired_interfaces": ["br-lan"],
    # Active ping watchdog for wired devices: periodically sends an ICMP
    # ping to each currently-home wired device to detect disconnection
    # independently of the passive `ip monitor neigh` stream.
    # Without this, a device that goes offline without generating ARP/NDP
    # traffic (e.g. a powered-off NAS, a stopped VM/container) can stay
    # STALE in the kernel neighbor table indefinitely - the kernel only
    # transitions STALE→PROBE→FAILED when another device tries to reach
    # it, which may never happen for always-idle hosts. The ping watchdog
    # catches these "silent" departures proactively.
    # Set to 0 to disable (fall back to passive ARP-only detection).
    "wired_ping_interval": 30,      # seconds between ping sweeps
    "wired_ping_timeout": 2,        # seconds to wait for a ping reply
    "wired_ping_failures": 3,       # consecutive failures before not_home
    "wired_grace_seconds": 30,
    "dhcp_leases_file": "/tmp/dhcp.leases",
    "log_level": "INFO",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s netpulse-presence[%(process)d] %(levelname)s: %(message)s",
)
log = logging.getLogger("netpulse-presence")


def load_config() -> dict:
    """Load settings, falling back to defaults for any missing key.

    A missing config file is not fatal - we run with defaults so the
    service can still start (mainly useful in development/testing).
    """
    config = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH) as f:
                user_config = json.load(f)
            config.update(user_config)
        except (json.JSONDecodeError, OSError) as exc:
            log.error("Failed to read %s: %s. Using defaults.", CONFIG_PATH, exc)
    else:
        log.warning("Config file %s not found, using defaults.", CONFIG_PATH)

    if not config["router_name"]:
        config["router_name"] = socket.gethostname()

    log.setLevel(getattr(logging, config.get("log_level", "INFO").upper(), logging.INFO))
    return config


# --------------------------------------------------------------------------
# Small shell helpers
# --------------------------------------------------------------------------

def run(cmd: list, timeout: float = 5.0) -> str:
    """Run a command and return its stdout, raising on non-zero exit."""
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, check=True
    )
    return result.stdout


def run_lines(cmd: list, timeout: float = 5.0):
    """Run a command and yield its stdout lines, swallowing errors quietly."""
    try:
        out = run(cmd, timeout=timeout)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
        log.debug("Command failed %s: %s", cmd, exc)
        return
    for line in out.splitlines():
        yield line


# --------------------------------------------------------------------------
# Hostname / DHCP lease resolution
# --------------------------------------------------------------------------

def load_dhcp_leases(leases_file: str) -> dict:
    """Parse /tmp/dhcp.leases (dnsmasq format) into {MAC: {"ip":..., "hostname":...}}.

    dnsmasq lease line format: <expiry> <mac> <ip> <hostname> <client-id>
    A hostname of "*" means the client didn't advertise a hostname via DHCP
    option 12 (very common for IoT/ESP-style devices), which we treat the
    same as "unknown" here.
    """
    leases = {}
    try:
        with open(leases_file) as f:
            for line in f:
                parts = line.split()
                if len(parts) < 4:
                    continue
                mac = parts[1].upper()
                ip = parts[2]
                hostname = parts[3]
                if hostname == "*":
                    hostname = mac.replace(":", "")
                leases[mac] = {"ip": ip, "hostname": hostname}
    except OSError as exc:
        log.debug("Could not read DHCP leases file %s: %s", leases_file, exc)
    return leases



def load_arp_table() -> dict:
    """Read the local kernel neighbor table for a MAC -> IP mapping.

    This works even on an AP with no local DHCP server: the ARP/NDP table
    reflects every device the AP's own network stack has actually talked to
    on the shared br-lan segment, regardless of which router handed out the
    IP. This is the same data source the wired tracker reads via
    `ip monitor neigh`, but read as a one-shot snapshot here (`ip neigh
    show`) since we only need it occasionally to label a Wi-Fi client, not
    to detect connect/disconnect events.
    """
    mac_to_ip = {}
    for family_flag in ("-4", "-6"):
        try:
            out = run(["ip", family_flag, "neigh", "show"])
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            continue
        for line in out.splitlines():
            parts = line.split()
            if len(parts) < 5 or "lladdr" not in parts:
                continue
            ip = parts[0]
            mac = parts[parts.index("lladdr") + 1].upper()
            # Skip link-local addresses (fe80::/10 for IPv6, 169.254/16 for
            # IPv4): they're never registered in DNS and are not useful as
            # a displayed "device IP" - keep looking for a real one instead.
            if ip.startswith("fe80:") or ip.startswith("169.254."):
                continue
            # Prefer IPv4 if we see both; don't overwrite an existing IPv4
            # entry with a later IPv6 one.
            if mac not in mac_to_ip or family_flag == "-4":
                mac_to_ip[mac] = ip
    return mac_to_ip


def resolve_hostname_via_dns(ip: str) -> str | None:
    """Reverse-DNS lookup of an IP via whatever resolver is configured
    (typically the main router's dnsmasq, see /etc/resolv.conf).

    Used as a fallback on APs that have no local DHCP leases file of their
    own (e.g. satellite APs in bridge mode) but whose /etc/resolv.conf
    still points at the main router's dnsmasq, which already knows every
    device's hostname. Returns None on any failure - this is best-effort
    cosmetic labelling, never required for presence detection itself.
    """
    try:
        hostname, _, _ = socket.gethostbyaddr(ip)
    except (socket.herror, socket.gaierror, OSError):
        return None
    if not hostname or hostname == ip:
        # musl's resolver (used on OpenWrt) can return the queried address
        # back as the "hostname" on a failed/empty lookup instead of
        # raising, unlike glibc. Treat that the same as "not found".
        return None
    # Strip a ".lan" or similar search-domain suffix for a cleaner display name
    return hostname.split(".")[0]


def resolve_device_info(mac: str, leases: dict, cache: dict) -> tuple[str, str]:
    """Return (ip, hostname) for a MAC, with graceful fallbacks.

    Shared helper used by CentralWifiTracker. Mirrors
    the tracker's own _resolve_device_info logic (lease lookup, then ARP +
    reverse-DNS fallback) but as a free function so it doesn't need to live
    on a particular tracker instance - CentralWifiTracker tracks multiple
    remote APs' clients, not just its own.
    """
    if mac in cache:
        return cache[mac]

    lease = leases.get(mac)
    if lease and lease.get("hostname") and lease.get("ip"):
        result = (lease["ip"], lease["hostname"])
        cache[mac] = result
        return result

    ip = "unknown"
    hostname = mac.replace(":", "")
    has_resolved_real_name = False

    arp_table = load_arp_table()
    ip_from_arp = arp_table.get(mac)
    if ip_from_arp:
        ip = ip_from_arp
        resolved = resolve_hostname_via_dns(ip_from_arp)
        if resolved:
            hostname = resolved
            has_resolved_real_name = True

    result = (ip, hostname)
    if has_resolved_real_name:
        cache[mac] = result
    return result


def load_static_lease_macs() -> set:
    """Read /etc/config/dhcp for "config host" sections (static leases).

    Mirrors the same UCI host sections visible in LuCI's Static Leases page.
    Returns an uppercase MAC set. Used as an automatic whitelist for the
    wired tracker when wired_static_leases_only is enabled, so users never
    have to hand-maintain a MAC list - it just reflects whatever static
    leases already exist on the router.
    """
    macs = set()
    try:
        out = run(["uci", "show", "dhcp"])
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
        log.debug("Could not read UCI dhcp config: %s", exc)
        return macs

    # Lines look like: dhcp.@host[3].mac='AA:BB:CC:DD:EE:FF'
    # or (multi-mac):  dhcp.@host[3].mac='AA:BB:CC:DD:EE:FF AA:BB:CC:DD:EE:F1'
    for line in out.splitlines():
        m = re.match(r"dhcp\.@host\[\d+\]\.mac=(.+)$", line)
        if not m:
            continue
        raw = m.group(1).strip().strip("'\"")
        for mac in raw.split():
            macs.add(mac.upper())
    return macs


# --------------------------------------------------------------------------
# MQTT publisher (with Home Assistant discovery)
# --------------------------------------------------------------------------

class Publisher:
    """Wraps the MQTT client and Home Assistant discovery/state publishing."""

    # Every discovered entity declares the SAME device identifier here, so
    # Home Assistant's native MQTT integration groups all tracked devices
    # (phones, laptops, appliances, ...) under one single "NetPulse
    # Presence" device in Settings -> Devices, instead of creating a
    # separate device per MAC.
    HUB_DEVICE_IDENTIFIER = "netpulse_hub"
    HUB_DEVICE_NAME = "NetPulse Presence"

    def __init__(self, config: dict):
        self.config = config
        self.base = config["mqtt_base_topic"]
        self.router_name = config["router_name"]
        self._known_discovered: set[str] = set()
        self._lock = threading.Lock()
        # Set by subscribe_wifi_seen(); re-subscribed on every (re)connect
        # since clean_session=True means the broker forgets subscriptions
        # across a dropped connection.
        self._wifi_registry_to_subscribe: "WifiMacRegistry | None" = None

        client_id = f"netpulse-presence-{self.router_name}"
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=True,
        )
        if config.get("mqtt_username"):
            self.client.username_pw_set(config["mqtt_username"], config.get("mqtt_password", ""))

        status_topic = f"{self.base}/{self.router_name}/status"
        self.client.will_set(status_topic, payload="offline", qos=1, retain=True)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            log.info("Connected to MQTT broker")
            status_topic = f"{self.base}/{self.router_name}/status"
            client.publish(status_topic, "online", qos=1, retain=True)
            if self._wifi_registry_to_subscribe is not None:
                self._do_subscribe_wifi_seen(self._wifi_registry_to_subscribe)
        else:
            log.error("MQTT connect failed with reason code %s", reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        log.warning("Disconnected from MQTT broker (reason_code=%s); paho will auto-reconnect", reason_code)

    def connect(self):
        self.client.connect_async(self.config["mqtt_host"], self.config["mqtt_port"], keepalive=60)
        self.client.loop_start()

    def publish_wifi_seen(self, mac: str) -> None:
        """Broadcast a local Wi-Fi sighting so other routers' wired
        trackers can also defer to it. Not retained: this is a
        point-in-time signal, not a state - a subscriber's own
        WifiMacRegistry.mark_seen_remote() is what tracks the grace window.
        """
        topic = f"{self.base}/_internal/wifi_seen/{self.router_name}"
        self.client.publish(topic, mac, qos=0, retain=False)

    def subscribe_wifi_seen(self, wifi_registry: "WifiMacRegistry") -> None:
        """Subscribe to every router's Wi-Fi-sighting broadcasts and feed
        them into the given registry. Used by a router running the wired
        tracker, so it also knows about Wi-Fi clients seen by OTHER
        routers/APs on the same network - not just its own
        CentralWifiTracker, if any is even running locally.
        """
        self._wifi_registry_to_subscribe = wifi_registry
        # If we're already connected by the time this is called, subscribe
        # right away too - don't wait for a future reconnect.
        if self.client.is_connected():
            self._do_subscribe_wifi_seen(wifi_registry)

    def _do_subscribe_wifi_seen(self, wifi_registry: "WifiMacRegistry") -> None:
        topic = f"{self.base}/_internal/wifi_seen/+"

        def _on_wifi_seen(client, userdata, msg):
            mac = msg.payload.decode("utf-8", errors="ignore").strip().upper()
            if mac:
                wifi_registry.mark_seen_remote(mac)

        self.client.message_callback_add(topic, _on_wifi_seen)
        self.client.subscribe(topic, qos=0)
        log.debug("Subscribed to cross-router Wi-Fi sighting broadcasts (%s)", topic)

    def stop(self):
        status_topic = f"{self.base}/{self.router_name}/status"
        self.client.publish(status_topic, "offline", qos=1, retain=True)
        time.sleep(0.2)
        self.client.loop_stop()
        self.client.disconnect()

    def _ensure_discovery(self, mac: str, hostname: str):
        """Publish the Home Assistant MQTT discovery payload once per MAC."""
        with self._lock:
            if mac in self._known_discovered:
                return
            self._known_discovered.add(mac)

        object_id = mac.replace(":", "").lower()
        config_topic = f"homeassistant/device_tracker/netpulse_{object_id}/config"
        payload = {
            "unique_id": f"netpulse_{object_id}",
            "name": hostname,
            "device": {
                "identifiers": [self.HUB_DEVICE_IDENTIFIER],
                "name": self.HUB_DEVICE_NAME,
                "manufacturer": "netpulse-presence",
                "model": "OpenWrt presence hub",
            },
            "state_topic": f"{self.base}/{mac}/state",
            "payload_home": "home",
            "payload_not_home": "not_home",
            "json_attributes_topic": f"{self.base}/{mac}/attributes",
            "source_type": "router",
        }
        self.client.publish(config_topic, json.dumps(payload), qos=1, retain=True)
        log.debug("Published discovery config for %s (%s)", mac, hostname)

    def publish_state(self, mac: str, state: str, attributes: dict):
        """Publish a presence state change for one device, with discovery."""
        hostname = attributes.get("hostname", mac.replace(":", ""))
        object_id = mac.replace(":", "").lower()

        # --- SECURITE CONFIGURATION HOME ASSISTANT ---
        if hostname.lower() != object_id:
            self._ensure_discovery(mac, hostname)
        else:
            log.debug("Hostname non résolu pour %s, saut de la discovery MQTT pour préserver le nom existant dans HA", mac)
        # ---------------------------------------------

        self.client.publish(f"{self.base}/{mac}/state", state, qos=1, retain=True)
        self.client.publish(
            f"{self.base}/{mac}/attributes", json.dumps(attributes), qos=1, retain=True
        )
        log.info("State update: %s (%s) -> %s [%s]", mac, hostname, state, attributes.get("source_type"))


# --------------------------------------------------------------------------
# Shared Wi-Fi/wired cross-check registry
# --------------------------------------------------------------------------

class WifiMacRegistry:
    """Thread-safe short-term memory of MACs seen as Wi-Fi clients.

    This registry can be fed from two sources:
      1. This router's own CentralWifiTracker/WifiHeartbeat (mark_seen()).
      2. Other routers, via MQTT (mark_seen_remote()) - this is essential
         in a multi-AP setup: Wi-Fi and wired tracking can run on
         different physical routers (e.g. Wi-Fi tracked on a satellite
         AP via enable_wifi_heartbeat, wired tracked by the main router),
         so an in-memory-only registry local to one process would never
         see Wi-Fi sightings that happened on another router. Without
         this cross-router sharing, a device connected via Wi-Fi on
         AP-02 would still get misclassified as "wired" by the main
         router, which also sees its MAC on the shared br-lan bridge.
    """

    def __init__(self, grace_seconds: float, on_mark_seen=None):
        self.grace_seconds = grace_seconds
        self._last_seen: dict[str, float] = {}
        self._lock = threading.Lock()
        # Optional callback(mac: str) invoked whenever a MAC is marked seen
        # locally, so it can be broadcast to other routers over MQTT.
        self._on_mark_seen = on_mark_seen

    def mark_seen(self, mac: str) -> None:
        """Record a Wi-Fi sighting from THIS router's own CentralWifiTracker."""
        with self._lock:
            self._last_seen[mac] = time.monotonic()
        if self._on_mark_seen:
            self._on_mark_seen(mac)

    def mark_seen_remote(self, mac: str) -> None:
        """Record a Wi-Fi sighting reported by ANOTHER router over MQTT.

        Does not re-trigger _on_mark_seen, to avoid an infinite relay loop
        of routers re-broadcasting what they just heard from each other.
        """
        with self._lock:
            self._last_seen[mac] = time.monotonic()

    def is_recently_wifi(self, mac: str) -> bool:
        with self._lock:
            last_seen = self._last_seen.get(mac)
        if last_seen is None:
            return False
        return (time.monotonic() - last_seen) <= self.grace_seconds


# --------------------------------------------------------------------------
# Wi-Fi tracker (hostapd, short poll loop - single source of truth)
# --------------------------------------------------------------------------

class CentralWifiTracker:
    """Network-wide Wi-Fi presence, single source of truth, ROUTER ONLY.

    Instead of each AP independently polling its own hostapd clients and
    racing its own grace timer against every other AP's tracker, this
    class runs on the router ONLY and ingests AP-STA-CONNECTED /
    AP-STA-DISCONNECTED events for every AP on the network - itself and
    any satellite AP - through a single, time-ordered stream: `logread -f`.

    Why this fixes roaming "blips": a roam from AP-A to AP-B produces two
    events (DISCONNECTED on A, then CONNECTED on B) a few seconds apart in
    the SAME log stream, processed by the SAME tracker. The CONNECTED
    event immediately cancels any pending "away" timer for that MAC,
    regardless of which AP it was previously tied to. There is no second,
    independently-timed tracker that might publish "not_home" before the
    new sighting arrives.

    Prerequisites:
      - This router: syslog-ng installed, configured to listen on UDP 514
        AND to feed itself (External system log server = 127.0.0.1).
      - Each satellite AP: System > Logging > External system log server
        = this router's IP, so its hostapd events get forwarded here.
      - Satellite APs must NOT run their own CentralWifiTracker instance -
        this router is the only source of truth. They should instead run
        enable_wifi_heartbeat (see WifiHeartbeat).

    Because this is purely event-driven, it also runs a periodic LOCAL
    resync (see wifi_central_local_resync_seconds / _local_resync) that
    re-checks THIS router's own hostapd interfaces via ubus and corrects
    any drift caused by a lost syslog line. This only covers this
    router's own radios; a satellite AP's clients still depend on its
    events actually reaching us.

    A satellite AP rebooting is a special case of that: hostapd dies
    without logging a clean AP-STA-DISCONNECTED for anyone, so its
    previous clients would otherwise stay stuck "home" forever. This is
    instead caught via hostapd's own AP-ENABLED log line (emitted when a
    radio starts beaconing again after a restart) - see _on_ap_restart.
    """

    AP_STA_RE = re.compile(
        r"hostapd:\s*(?P<iface>\S+):\s*(?P<event>AP-STA-CONNECTED|AP-STA-DISCONNECTED)\s+"
        r"(?P<mac>([0-9a-fA-F]{1,2}:){5}[0-9a-fA-F]{1,2})"
    )
    # hostapd logs this when a radio (re)starts beaconing - notably right
    # after the whole AP reboots. A reboot kills hostapd without it ever
    # getting to log an AP-STA-DISCONNECTED for each of its clients, so
    # without this we'd have no way to know those clients actually left -
    # they'd just stay stuck "home" until/unless they happen to reconnect
    # somewhere and generate a fresh AP-STA-CONNECTED.
    AP_ENABLED_RE = re.compile(r"hostapd:\s*(?P<iface>\S+):\s*AP-ENABLED\b")

    def __init__(self, config: dict, publisher: "Publisher", leases: dict,
                 wifi_registry: "WifiMacRegistry | None" = None):
        self.config = config
        self.publisher = publisher
        self.leases = leases
        self.wifi_registry = wifi_registry
        self.router_name = config["router_name"]
        self.grace_seconds = config["wifi_central_grace_seconds"]
        self.ap_timeout_seconds = config["wifi_central_ap_timeout_seconds"]
        self.source_pattern = re.compile(config["wifi_central_syslog_source_pattern"])
        self.local_resync_interval = config.get("wifi_central_local_resync_seconds", 90)
        self._resolved_cache: dict[str, tuple[str, str]] = {}
        self._ssid_by_iface: dict[str, str] = {}

        # mac -> {"station": str, "iface": str, "state": "home"/"not_home"}
        self._current: dict[str, dict] = {}
        # mac -> threading.Timer pending the not_home transition
        self._pending_away: dict[str, threading.Timer] = {}
        # station -> monotonic timestamp of last log line seen from it
        self._last_seen_from_station: dict[str, float] = {}
        self._lock = threading.RLock()
        self._proc = None
        self._stop = threading.Event()
        self._watchdog_thread = None

    # -- public API, mirrors WiredTracker's run()/stop() shape --

    def run(self):
        log.info(
            "Central Wi-Fi tracker started (grace=%ss, ap_timeout=%ss, source_pattern=%r)",
            self.grace_seconds, self.ap_timeout_seconds, self.source_pattern.pattern,
        )
        self._watchdog_thread = threading.Thread(
            target=self._dead_ap_watchdog, name="CentralWifiTracker-watchdog", daemon=True,
        )
        self._watchdog_thread.start()

        if self.local_resync_interval > 0:
            resync_thread = threading.Thread(
                target=self._local_resync_loop, name="CentralWifiTracker-local-resync", daemon=True,
            )
            resync_thread.start()

        if self.wifi_registry:
            keepalive_thread = threading.Thread(
                target=self._registry_keepalive, name="CentralWifiTracker-keepalive", daemon=True,
            )
            keepalive_thread.start()

        while not self._stop.is_set():
            try:
                self._proc = subprocess.Popen(
                    ["logread", "-f"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    bufsize=1,
                )
                for line in self._proc.stdout:
                    if self._stop.is_set():
                        break
                    self._handle_line(line)
            except Exception:
                log.exception("Unexpected error in central Wi-Fi log loop, restarting in 5s")
            finally:
                if self._proc:
                    self._proc.terminate()
                    self._proc = None
            if not self._stop.is_set():
                self._stop.wait(5)

    def stop(self):
        self._stop.set()
        if self._proc:
            self._proc.terminate()
        with self._lock:
            for timer in self._pending_away.values():
                timer.cancel()

    # -- log line processing --

    def _handle_line(self, line: str):
        sta_match = self.AP_STA_RE.search(line)
        enabled_match = None if sta_match else self.AP_ENABLED_RE.search(line)
        if not sta_match and not enabled_match:
            return

        station_match = self.source_pattern.search(line)
        # Fall back to this router's own name when no station hostname is
        # found in the line at all - this happens for the router's OWN
        # local hostapd events, which (depending on syslog-ng setup) may
        # not be tagged with a hostname prefix the way forwarded lines
        # from a satellite AP are.
        station = station_match.group(0) if station_match else self.router_name
        self._last_seen_from_station[station] = time.monotonic()

        if enabled_match:
            self._on_ap_restart(station, enabled_match.group("iface"))
            return

        mac = sta_match.group("mac").upper()
        iface = sta_match.group("iface")
        event = sta_match.group("event")

        if event == "AP-STA-CONNECTED":
            self._on_connect(mac, station, iface)
        else:
            self._on_disconnect(mac, station, iface)

    def _on_ap_restart(self, station: str, iface: str):
        """A radio just (re)started beaconing - most notably right after
        the whole AP reboots. hostapd never got to log individual
        AP-STA-DISCONNECTED events for whoever was connected before the
        restart, so treat every client we still show as "home" on this
        exact station/iface as needing to re-prove itself: start the
        normal grace-period departure for each of them, exactly as a real
        disconnect would. A device that's still actually there (or that
        reconnects to any AP quickly) gets that timer cancelled by its own
        fresh AP-STA-CONNECTED before it expires, same as any other
        roam - so this is a no-op for devices that were never really gone.
        """
        with self._lock:
            affected = [
                mac for mac, entry in self._current.items()
                if entry.get("station") == station
                and entry.get("iface") == iface
                and entry.get("state") == "home"
                and mac not in self._pending_away
            ]
        if affected:
            log.info(
                "Central Wi-Fi: %s/%s just restarted, re-validating %d "
                "previously-connected client(s)",
                station, iface, len(affected),
            )
        for mac in affected:
            self._on_disconnect(mac, station, iface)

    def _on_connect(self, mac: str, station: str, iface: str):
        if self.wifi_registry:
            if not self.wifi_registry.is_recently_wifi(mac):
                self.wifi_registry.mark_seen(mac)
            else:
                self.wifi_registry.mark_seen_remote(mac)

        with self._lock:
            # Cancel any pending "away" timer for this MAC - this is the
            # roaming fix: a connect anywhere (even on a DIFFERENT AP than
            # the one that just disconnected it) immediately supersedes
            # the pending departure instead of racing it.
            timer = self._pending_away.pop(mac, None)
            if timer:
                timer.cancel()
            already_home = self._current.get(mac, {}).get("state") == "home"
            same_station = self._current.get(mac, {}).get("station") == station
            self._current[mac] = {"station": station, "iface": iface, "state": "home"}

        if already_home and same_station:
            # No-op republish would just spam MQTT with no new
            # information; skip it (same dedup spirit as
            # _maybe_transition's state_unchanged check).
            return

        log.info("Central Wi-Fi: %s connected on %s/%s", mac, station, iface)
        self._publish(mac, "home", station, iface)

    def _on_disconnect(self, mac: str, station: str, iface: str):
        with self._lock:
            entry = self._current.get(mac)
            # Ignore a disconnect for a station/iface that isn't the one we
            # currently believe this MAC is on - this happens naturally
            # during a fast roam if events arrive slightly out of order
            # (e.g. the new AP's CONNECTED is processed before the old
            # AP's DISCONNECTED). Acting on a stale disconnect here would
            # incorrectly start an away timer right after a fresh connect.
            if entry and (entry.get("station") != station or entry.get("iface") != iface):
                log.debug(
                    "Central Wi-Fi: ignoring stale disconnect for %s on %s/%s "
                    "(currently tracked on %s/%s)",
                    mac, station, iface, entry.get("station"), entry.get("iface"),
                )
                return

            existing_timer = self._pending_away.get(mac)
            if existing_timer:
                return  # already pending, nothing new to do

            timer = threading.Timer(self.grace_seconds, self._confirm_away, args=(mac,))
            timer.daemon = True
            self._pending_away[mac] = timer
            timer.start()
        log.debug(
            "Central Wi-Fi: %s disconnected from %s/%s, departure pending (%ss grace)",
            mac, station, iface, self.grace_seconds,
        )

    def _confirm_away(self, mac: str):
        with self._lock:
            self._pending_away.pop(mac, None)
            entry = self._current.get(mac)
            if not entry or entry.get("state") != "home":
                return
            station, iface = entry.get("station"), entry.get("iface")
            entry["state"] = "not_home"

        log.info("Central Wi-Fi: %s confirmed away (grace expired, was on %s/%s)",
                  mac, station, iface)
        self._publish(mac, "not_home", station, iface)

    def _publish(self, mac: str, state: str, station: str, iface: str):
        ip, hostname = resolve_device_info(mac, self.leases, self._resolved_cache)
        attributes = {
            "mac": mac,
            "source_type": "wifi",
            "source_router": station,
            "ip": ip,
            "hostname": hostname,
            # NOTE: this is the hostapd INTERFACE name (e.g. "phy2-ap0"),
            # not the actual SSID string. hostapd's AP-STA-CONNECTED /
            # AP-STA-DISCONNECTED syslog lines don't include the SSID at
            # all, only the interface -
            # which can call `ubus call iwinfo info` to resolve a real
            # SSID. Resolving the interface -> SSID mapping here would
            # require either a local ubus call (only valid for THIS
            # router's own interfaces, not a remote AP's) or maintaining a
            # static interface->SSID table in config. Left as a known
            # limitation for now - the interface name is still useful to
            # tell which physical radio/band a client is on.
            "ssid": iface,
        }
        self.publisher.publish_state(mac, state, attributes)

    # -- dead-AP watchdog (equivalent of disconnectIdxBumper's Task 2) --

    def _registry_keepalive(self):
        """Periodically refresh wifi_registry for every MAC currently
        tracked as home, since a STABLE Wi-Fi association produces no
        further hostapd syslog events at all once the initial
        AP-STA-CONNECTED has fired - nothing re-confirms "still on
        Wi-Fi" to WiredTracker's dedup registry on its own. Without this,
        a device connected for longer than the registry's grace window
        would eventually have its wifi_registry entry expire even though
        it never actually left Wi-Fi, letting WiredTracker briefly
        reclaim and republish it as "wired" (since the same MAC is still
        visible in the shared br-lan ARP table either way).
        """
        # Refresh well before the registry's own grace window could
        # expire - a comfortable safety margin under config["wifi_central_
        # grace_seconds"], not tied 1:1 to it, so a single missed refresh
        # cycle (e.g. a slow loop iteration) can't itself cause a lapse.
        interval = max(5, self.grace_seconds // 3)
        while not self._stop.wait(interval):
            with self._lock:
                home_macs = [mac for mac, e in self._current.items() if e.get("state") == "home"]
            for mac in home_macs:
                self.wifi_registry.mark_seen_remote(mac)

    def _dead_ap_watchdog(self):
        """Periodically drop clients last reported by an AP that has gone
        silent (crashed, lost power, lost syslog connectivity) for too
        long - otherwise those devices would be stuck showing "home"
        forever, since no DISCONNECTED event will ever arrive from a dead
        AP to start their away timer.
        """
        check_interval = min(60, max(10, self.ap_timeout_seconds // 10))
        while not self._stop.wait(check_interval):
            now = time.monotonic()
            with self._lock:
                dead_stations = {
                    station for station, last_seen in self._last_seen_from_station.items()
                    if now - last_seen > self.ap_timeout_seconds
                }
                if not dead_stations:
                    continue
                macs_to_drop = [
                    mac for mac, entry in self._current.items()
                    if entry.get("station") in dead_stations and entry.get("state") == "home"
                ]
            for station in dead_stations:
                log.warning(
                    "Central Wi-Fi: AP %r has not reported any STA event in over %ss, "
                    "treating its previously-seen clients as away",
                    station, self.ap_timeout_seconds,
                )
            for mac in macs_to_drop:
                with self._lock:
                    entry = self._current.get(mac)
                    if not entry or entry.get("state") != "home":
                        continue
                    timer = self._pending_away.pop(mac, None)
                    if timer:
                        timer.cancel()
                    station, iface = entry.get("station"), entry.get("iface")
                    entry["state"] = "not_home"
                self._publish(mac, "not_home", station, iface)

    # -- periodic local resync (self-heals a lost syslog event, but only
    #    for THIS router's own hostapd interfaces - see config comment) --

    def _discover_local_interfaces(self):
        """Returns hostapd ubus objects, or None if `ubus list` itself
        failed - callers must treat None as "couldn't check", never as
        "zero interfaces/clients" (see _local_resync).
        """
        try:
            out = run(["ubus", "list"])
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            log.debug("Central Wi-Fi resync: could not list ubus objects: %s", exc)
            return None
        return [line.strip() for line in out.splitlines() if line.strip().startswith("hostapd.")]

    def _get_local_authorized_clients(self):
        """Return ({mac: phy_iface}, queried_ifaces) for this router's own
        radios, queried directly via ubus (the same call WifiHeartbeat
        uses) rather than relying on hostapd's syslog events. Returns
        (None, None) if ubus itself was unreachable this cycle.
        """
        ifaces = self._discover_local_interfaces()
        if ifaces is None:
            return None, None
        clients = {}
        queried_ifaces = set()
        for iface_obj in ifaces:
            try:
                out = run(["ubus", "call", iface_obj, "get_clients"])
                data = json.loads(out)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
                     OSError, json.JSONDecodeError) as exc:
                log.debug("Central Wi-Fi resync: could not query %s: %s", iface_obj, exc)
                continue
            phy_iface = iface_obj.split(".", 1)[1] if "." in iface_obj else iface_obj
            queried_ifaces.add(phy_iface)
            for mac, info in data.get("clients", {}).items():
                if info.get("authorized"):
                    clients[mac.upper()] = phy_iface
        return clients, queried_ifaces

    def _local_resync(self):
        """One reconciliation pass against this router's own hostapd state.

        Two kinds of drift get corrected here:
          - a client hostapd still shows as authorized, but whose
            CONNECTED event never reached us (or was logged before
            syslog-ng was even listening) -> treated as a fresh connect.
          - a client we still show as "home" on this router, but that
            hostapd no longer lists at all -> its DISCONNECTED event was
            lost, so we start the normal grace-period departure exactly
            as _on_disconnect would have.
        Both paths reuse _on_connect/_on_disconnect so behavior (grace
        timers, dedup, MQTT publish) stays identical to the event-driven
        path - this is purely a safety net for missed events, not a
        separate code path. A total ubus failure is skipped outright
        rather than treated as "nobody's connected", and a client on an
        interface that failed to respond THIS cycle is left alone rather
        than declared gone - a transient hiccup here must never look
        like a mass disconnect.
        """
        actual, queried_ifaces = self._get_local_authorized_clients()
        if actual is None:
            log.debug("Central Wi-Fi resync: skipping this cycle, ubus unreachable")
            return

        for mac, iface in actual.items():
            self._on_connect(mac, self.router_name, iface)

        with self._lock:
            stale = [
                mac for mac, entry in self._current.items()
                if entry.get("station") == self.router_name
                and entry.get("state") == "home"
                and mac not in actual
                and entry.get("iface") in queried_ifaces
            ]
        for mac in stale:
            with self._lock:
                entry = self._current.get(mac)
                if not entry:
                    continue
                station, iface = entry.get("station"), entry.get("iface")
            self._on_disconnect(mac, station, iface)

    def _local_resync_loop(self):
        log.info(
            "Central Wi-Fi: local resync watchdog started (interval=%ss)",
            self.local_resync_interval,
        )
        while not self._stop.wait(self.local_resync_interval):
            try:
                self._local_resync()
            except Exception:
                log.exception("Unexpected error during Central Wi-Fi local resync")


class WifiHeartbeat:
    """Runs on a satellite AP alongside its syslog-ng forwarding, to close
    the one gap wifi_central_local_resync_seconds can't reach from the
    main router: a single AP-STA-CONNECTED or AP-STA-DISCONNECTED line
    getting lost on a satellite that's otherwise running fine.

    Every wifi_heartbeat_interval seconds it queries this router's own
    hostapd interfaces via ubus - the same call the main router's
    _local_resync uses - and re-announces the result over syslog using
    the EXACT wording a genuine hostapd AP-STA-CONNECTED/AP-STA-DISCONNECTED
    line uses. That synthetic line then flows through the satellite's
    existing syslog-ng forwarding to the central router completely
    unchanged, and gets parsed by CentralWifiTracker._handle_line exactly
    like a real hostapd event - already-home clients are a cheap no-op
    there, and a client hostapd no longer lists starts the normal
    grace-period departure.

    This is intentionally independent of enable_wifi_tracking: a
    satellite AP runs this INSTEAD of its own CentralWifiTracker, not
    alongside it.
    """

    def __init__(self, config: dict):
        self.interval = config.get("wifi_heartbeat_interval", 60)
        self.miss_threshold = max(1, config.get("wifi_heartbeat_miss_threshold", 2))
        # mac -> phy_iface last reported as authorized
        self._previous: dict[str, str] = {}
        # mac -> number of consecutive cycles it's been missing from an
        # interface that WAS successfully queried (see _tick)
        self._miss_count: dict[str, int] = {}
        self._stop = threading.Event()

    def _discover_interfaces(self):
        """Returns the list of hostapd ubus objects, or None if `ubus
        list` itself failed - callers must treat None as "couldn't check
        anything this cycle", never as "zero clients everywhere".
        """
        try:
            out = run(["ubus", "list"])
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            log.debug("Wi-Fi heartbeat: could not list ubus objects: %s", exc)
            return None
        return [line.strip() for line in out.splitlines() if line.strip().startswith("hostapd.")]

    def _get_authorized_clients(self):
        """Returns (clients, queried_ifaces), or (None, None) if `ubus`
        itself was unreachable this cycle. queried_ifaces is the set of
        phy interfaces that were SUCCESSFULLY queried - a MAC previously
        seen on an interface that ISN'T in this set gives us no new
        information (we simply couldn't check it this cycle), which is
        very different from that interface confirming it's gone.
        """
        ifaces = self._discover_interfaces()
        if ifaces is None:
            return None, None
        clients = {}
        queried_ifaces = set()
        for iface_obj in ifaces:
            try:
                out = run(["ubus", "call", iface_obj, "get_clients"])
                data = json.loads(out)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
                     OSError, json.JSONDecodeError) as exc:
                log.debug("Wi-Fi heartbeat: could not query %s: %s", iface_obj, exc)
                continue
            phy_iface = iface_obj.split(".", 1)[1] if "." in iface_obj else iface_obj
            queried_ifaces.add(phy_iface)
            for mac, info in data.get("clients", {}).items():
                if info.get("authorized"):
                    clients[mac.upper()] = phy_iface
        return clients, queried_ifaces

    def _emit(self, iface: str, event: str, mac: str):
        # `logger -t hostapd` produces a syslog line of the form
        # "<host> hostapd: <message>", which is exactly what
        # CentralWifiTracker.AP_STA_RE expects - and since it runs
        # locally, syslog's own hostname field already identifies this
        # router the same way a real hostapd event would.
        try:
            subprocess.run(
                ["logger", "-t", "hostapd", f"{iface}: {event} {mac}"],
                timeout=5, check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            log.debug("Wi-Fi heartbeat: logger call failed: %s", exc)

    def _tick(self):
        current, queried_ifaces = self._get_authorized_clients()
        if current is None:
            # ubus itself is unreachable this cycle - do NOT touch any
            # state or emit anything. Treating this as "nobody's
            # connected" would turn a transient local hiccup into a mass
            # false disconnect for every client on this AP, which is
            # exactly the failure mode this whole mechanism exists to
            # avoid.
            log.debug("Wi-Fi heartbeat: skipping this cycle, ubus unreachable")
            return

        for mac, iface in current.items():
            self._emit(iface, "AP-STA-CONNECTED", mac)
            self._miss_count.pop(mac, None)

        carried_over = {}
        for mac, prev_iface in self._previous.items():
            if mac in current:
                continue
            if prev_iface not in queried_ifaces:
                # This MAC's interface wasn't successfully queried this
                # cycle - we simply don't know, so keep it as-is rather
                # than counting a miss or declaring it gone.
                carried_over[mac] = prev_iface
                continue
            misses = self._miss_count.get(mac, 0) + 1
            if misses < self.miss_threshold:
                self._miss_count[mac] = misses
                carried_over[mac] = prev_iface
                log.debug(
                    "Wi-Fi heartbeat: %s missing from %s (%d/%d consecutive "
                    "cycles), waiting for confirmation before declaring it gone",
                    mac, prev_iface, misses, self.miss_threshold,
                )
                continue
            self._miss_count.pop(mac, None)
            self._emit(prev_iface, "AP-STA-DISCONNECTED", mac)

        self._previous = {**current, **carried_over}

    def run(self):
        log.info(
            "Wi-Fi heartbeat started (interval=%ss, miss_threshold=%s)",
            self.interval, self.miss_threshold,
        )
        while not self._stop.wait(self.interval):
            try:
                self._tick()
            except Exception:
                log.exception("Unexpected error in Wi-Fi heartbeat loop")

    def stop(self):
        self._stop.set()


# --------------------------------------------------------------------------
# Wired tracker (`ip monitor neigh` - true kernel event stream)
# --------------------------------------------------------------------------

WIRED_CONNECTED_STATES = {"REACHABLE", "PERMANENT", "DELAY", "PROBE"}


class WiredTracker:
    """Tracks wired client presence via a live `ip monitor neigh` stream."""

    NEIGH_LINE_RE = re.compile(
        r"^(?P<ip>\S+)\s+dev\s+(?P<iface>\S+)\s+(?:lladdr\s+(?P<mac>[0-9a-fA-F:]+)\s+)?"
        r"(?:router\s+)?(?P<state>REACHABLE|STALE|DELAY|PROBE|FAILED|PERMANENT|NOARP|INCOMPLETE)\b"
    )
    DELETED_RE = re.compile(r"^Deleted\s+")

    def __init__(self, config: dict, publisher: Publisher, leases: dict,
                 static_lease_macs: set, wifi_registry: WifiMacRegistry | None = None):
        self.config = config
        self.publisher = publisher
        self.leases = leases
        self.static_lease_macs = static_lease_macs
        self.wifi_registry = wifi_registry
        self.router_name = config["router_name"]
        self.whitelist = [w.upper() for w in config.get("wired_whitelist", [])]
        self.interfaces = set(config.get("wired_interfaces", ["br-lan"]))
        self.grace_seconds = config["wired_grace_seconds"]
        self.ping_interval = config.get("wired_ping_interval", 30)
        self.ping_timeout = config.get("wired_ping_timeout", 2)
        self.ping_failures_threshold = config.get("wired_ping_failures", 3)
        self._current_state = {}  # mac -> "home"/"not_home"
        self._pending_away = {}  # mac -> threading.Timer
        self._ip_to_mac = {}  # ip -> mac
        self._mac_to_ip = {}  # mac -> ip (reverse, for ping watchdog)
        self._ping_fail_count = {}  # mac -> consecutive ping failure count
        self._proc = None
        self._stop = threading.Event()
        self._lock = threading.Lock()

    def _is_tracked(self, mac: str, iface: str) -> bool:
        if self.interfaces and iface not in self.interfaces:
            return False
        if self.wifi_registry:
            recently_wifi = self.wifi_registry.is_recently_wifi(mac)
            log.debug("Wi-Fi registry check for %s: recently_wifi=%s", mac, recently_wifi)
            if recently_wifi:
                return False
        if self.static_lease_macs:
            return mac in self.static_lease_macs
        if self.whitelist:
            return any(mac.startswith(prefix) for prefix in self.whitelist)
        return False

    def _set_state(self, mac: str, iface: str, state: str):
        is_connected = state in WIRED_CONNECTED_STATES

        with self._lock:
            existing_timer = self._pending_away.pop(mac, None)
        if existing_timer:
            existing_timer.cancel()

        if is_connected:
            self._transition(mac, "home")
        elif state in ("STALE", "DELAY", "PROBE"):
            pass
        else:
            timer = threading.Timer(self.grace_seconds, self._transition, args=(mac, "not_home"))
            timer.daemon = True
            with self._lock:
                self._pending_away[mac] = timer
            timer.start()

    def _transition(self, mac: str, state: str):
        with self._lock:
            self._pending_away.pop(mac, None)
            if self._current_state.get(mac) == state:
                return
            self._current_state[mac] = state

        lease = self.leases.get(mac, {})
        attributes = {
            "mac": mac,
            "source_type": "wired",
            "source_router": self.router_name,
            "ip": lease.get("ip", "unknown"),
            "hostname": lease.get("hostname", mac.replace(":", "")),
            # Toujours présente (à None) pour que le payload "wired" ait la
            # même forme de clés que le payload "wifi" - voir le commentaire
            # équivalent dans _maybe_transition.
            "ssid": None,
        }
        self.publisher.publish_state(mac, state, attributes)

    def _handle_line(self, line: str):
        line = line.strip()
        if not line:
            return

        deleted = bool(self.DELETED_RE.match(line))
        if deleted:
            line = self.DELETED_RE.sub("", line)

        m = self.NEIGH_LINE_RE.match(line)
        if not m:
            return

        ip = m.group("ip")
        mac = m.group("mac")
        iface = m.group("iface")
        state = "FAILED" if deleted else m.group("state")

        if mac:
            mac = mac.upper()
            self._ip_to_mac[ip] = mac
            self._mac_to_ip[mac] = ip
        else:
            mac = self._ip_to_mac.get(ip)
            if not mac:
                return

        if not self._is_tracked(mac, iface):
            self._withdraw_if_tracked(mac)
            return

        self._set_state(mac, iface, state)

    def _withdraw_if_tracked(self, mac: str) -> bool:
        """Stop tracking `mac` as wired (it's now recognized as Wi-Fi, or
        otherwise no longer matches our tracking filters). Returns True if
        it actually was being tracked. Shared between the neighbor-stream
        handler and the ping watchdog, since a MAC can transition to Wi-Fi
        without ever generating a fresh `ip monitor neigh` line on its own
        (a stable Wi-Fi association produces no further ARP/NDP churn) -
        without this, the ping watchdog would keep pinging and flip-
        flopping a device's wired state forever, even though it's already
        correctly tracked as home over Wi-Fi elsewhere.
        """
        with self._lock:
            was_tracked = mac in self._current_state
            self._current_state.pop(mac, None)
            self._ping_fail_count.pop(mac, None)
            timer = self._pending_away.pop(mac, None)
        if timer:
            timer.cancel()
        if was_tracked:
            log.info("%s is now recognized as a Wi-Fi client, withdrawing its wired state", mac)
        return was_tracked

    def _seed_from_snapshot(self):
        """Read the current neighbor table once and feed each line through
        the normal line handler, so already-stable devices (always-on NAS,
        TVs, set-top boxes, etc.) are picked up immediately instead of
        waiting for their next ARP/NDP state transition - a transition
        which, for a device that never goes through a disconnect/reconnect
        cycle, may simply never happen. Without this, `ip monitor neigh`
        only ever reports *changes*, so anything already stable in the
        neighbor table at service start (or restart) stays invisible to
        netpulse-presence forever, even though it has a static DHCP lease
        and is genuinely on the network.

        Minor caveat: at process start, this snapshot may run before any
        cross-router Wi-Fi sightings have arrived over MQTT, so a device
        that is actually on Wi-Fi could be briefly (mis)classified as wired
        on the very first seed. This self-corrects as soon as a Wi-Fi
        sighting comes in (see `_handle_line`'s wifi_registry check), so it
        is not worth delaying startup for.
        """
        seeded = 0
        for family_flag in ("-4", "-6"):
            try:
                out = run(["ip", family_flag, "neigh", "show"])
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
                log.debug("Could not read initial neigh snapshot (%s): %s", family_flag, exc)
                continue
            for line in out.splitlines():
                self._handle_line(line)
                seeded += 1
        log.info(
            "Wired tracker: processed %d snapshot line(s) from initial neigh table, %d MAC(s) currently tracked",
            seeded, len(self._current_state),
        )

    def _ping_one(self, ip: str) -> bool:
        """Send a single ICMP ping to ip. Returns True if the host replied."""
        try:
            result = subprocess.run(
                ["ping", "-c", "1", "-W", str(self.ping_timeout), ip],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=self.ping_timeout + 1,
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, OSError):
            return False

    def _ping_watchdog(self):
        """Periodically ping all currently-home wired devices to detect
        silent departures (powered-off hosts, stopped containers/VMs)
        that would never generate an ARP/NDP event on their own - the
        kernel only probes a STALE entry when someone else tries to reach
        that IP, which may never happen for an idle host.

        Only runs when wired_ping_interval > 0 in config.
        After wired_ping_failures consecutive failures, the device is
        declared not_home, independently of the ip monitor neigh stream.
        A successful ping resets the failure counter and also refreshes
        the ARP entry (the kernel sees the reply), so the two mechanisms
        work naturally together rather than fighting each other.
        """
        log.info(
            "Wired ping watchdog started (interval=%ss, timeout=%ss, failures=%s)",
            self.ping_interval, self.ping_timeout, self.ping_failures_threshold,
        )
        while not self._stop.wait(self.ping_interval):
            with self._lock:
                home_macs = [
                    mac for mac, state in self._current_state.items()
                    if state == "home"
                ]

            for mac in home_macs:
                if self.wifi_registry and self.wifi_registry.is_recently_wifi(mac):
                    # This device has since been (re)confirmed as a Wi-Fi
                    # client elsewhere - possibly by a satellite AP's
                    # heartbeat/events, which never goes through this
                    # tracker's own `ip monitor neigh` stream at all. Stop
                    # pinging it as wired instead of fighting the Wi-Fi
                    # tracker's own state for the same MAC.
                    self._withdraw_if_tracked(mac)
                    continue

                ip = self._mac_to_ip.get(mac)
                if not ip or ":" in ip:
                    # Skip if no IP known, or IPv6 (ping6 needs a different
                    # command; IPv4 is sufficient for presence detection)
                    continue

                alive = self._ping_one(ip)

                with self._lock:
                    if alive:
                        if mac in self._ping_fail_count:
                            log.debug(
                                "Ping watchdog: %s (%s) replied, resetting failure count",
                                mac, ip,
                            )
                        self._ping_fail_count.pop(mac, None)
                    else:
                        count = self._ping_fail_count.get(mac, 0) + 1
                        self._ping_fail_count[mac] = count
                        log.debug(
                            "Ping watchdog: %s (%s) no reply (%d/%d)",
                            mac, ip, count, self.ping_failures_threshold,
                        )
                        if count >= self.ping_failures_threshold:
                            self._ping_fail_count.pop(mac, None)
                            timer = self._pending_away.pop(mac, None)
                            if timer:
                                timer.cancel()
                            log.info(
                                "Ping watchdog: %s (%s) confirmed away after %d "
                                "consecutive failures",
                                mac, ip, self.ping_failures_threshold,
                            )
                            threading.Thread(
                                target=self._transition,
                                args=(mac, "not_home"),
                                daemon=True,
                            ).start()

    def run(self):
        log.info(
            "Wired tracker started (interfaces=%s, grace=%ss, static_leases_only=%s)",
            sorted(self.interfaces), self.grace_seconds, bool(self.static_lease_macs),
        )
        if self.ping_interval > 0:
            threading.Thread(
                target=self._ping_watchdog,
                name="WiredTracker-ping",
                daemon=True,
            ).start()
        while not self._stop.is_set():
            try:
                self._proc = subprocess.Popen(
                    ["ip", "monitor", "neigh"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    bufsize=1,
                )
                # Seed from a one-shot snapshot BEFORE consuming the live
                # event stream, so devices already stable in the neighbor
                # table (always-on NAS, TVs, boxes...) get an initial
                # home/not_home publish right away rather than staying
                # silently untracked until/unless their ARP/NDP state
                # happens to change.
                self._seed_from_snapshot()

                for line in self._proc.stdout:
                    if self._stop.is_set():
                        break
                    self._handle_line(line)
            except Exception:
                log.exception("Unexpected error in wired monitor loop, restarting in 5s")
            finally:
                if self._proc:
                    self._proc.terminate()
                    self._proc = None
            if not self._stop.is_set():
                self._stop.wait(5)

    def stop(self):
        """Gracefully stop the wired monitor process and clear any pending timers."""
        self._stop.set()
        with self._lock:
            if self._proc:
                try:
                    self._proc.terminate()
                except OSError:
                    pass
                self._proc = None
            for timer in self._pending_away.values():
                timer.cancel()
            self._pending_away.clear()


# --------------------------------------------------------------------------
# Main Orchestrator
# --------------------------------------------------------------------------

def main():
    config = load_config()
    
    log.info("Starting NetPulse Presence Hub on %s", config["router_name"])

    # Only CentralWifiTracker/WiredTracker resolve hostnames
    # via leases - a satellite AP running ONLY enable_wifi_heartbeat has
    # no use for any of this (WifiHeartbeat never even sees `leases`), so
    # skip the work entirely rather than parsing DHCP/UCI files for
    # nothing on every start.
    needs_leases = config["enable_wifi_tracking"] or config["enable_wired_tracking"]
    leases = {}
    static_leases = set()
    if needs_leases:
        leases = load_dhcp_leases(config["dhcp_leases_file"])
        static_leases = load_static_lease_macs() if config["wired_static_leases_only"] else set()
    
    publisher = Publisher(config)
    publisher.connect()

    # Wi-Fi and Ethernet share the same br-lan bridge, so a Wi-Fi client's
    # MAC is visible to ANY router's wired tracker on the network - not
    # just the router running CentralWifiTracker. In a multi-AP
    # setup it's common for Wi-Fi tracking (on a satellite AP) and wired
    # tracking (on the main router) to run as entirely separate processes
    # on different machines, so a purely in-memory, per-process registry
    # would never see those cross-router sightings. To fix this, every
    # router broadcasts its own Wi-Fi sightings over MQTT
    # (publish_wifi_seen), and any router running a wired tracker
    # subscribes to ALL routers' broadcasts (subscribe_wifi_seen) to build
    # The cross-router Wi-Fi dedup registry's grace window must outlive
    # CentralWifiTracker's own grace period, so a wired tracker never
    # mistakenly re-claims a MAC as wired in the gap between two Wi-Fi
    # sightings.
    wifi_registry = WifiMacRegistry(
        grace_seconds=config["wifi_central_grace_seconds"] + 10,
        on_mark_seen=publisher.publish_wifi_seen,
    )
    if config["enable_wired_tracking"]:
        publisher.subscribe_wifi_seen(wifi_registry)

    trackers = []
    threads = []

    if config["enable_wifi_tracking"]:
        # Single source of truth for Wi-Fi presence across all APs, fed by
        # syslog-ng-forwarded hostapd events. See CentralWifiTracker's
        # docstring for the required setup (syslog-ng on this router,
        # satellite APs forwarding their logs here). This only ever runs
        # on the main router - satellite APs run enable_wifi_heartbeat
        # instead, never their own tracker.
        wifi_tracker = CentralWifiTracker(config, publisher, leases, wifi_registry)
        t = threading.Thread(target=wifi_tracker.run, name="CentralWifiTracker", daemon=True)
        trackers.append(wifi_tracker)
        threads.append(t)

    if config["enable_wired_tracking"]:
        wired_tracker = WiredTracker(config, publisher, leases, static_leases, wifi_registry)
        trackers.append(wired_tracker)
        t = threading.Thread(target=wired_tracker.run, name="WiredTracker", daemon=True)
        threads.append(t)

    if config.get("enable_wifi_heartbeat"):
        # A satellite AP in central mode runs this INSTEAD of its own
        # CentralWifiTracker - it re-announces its own hostapd
        # clients over syslog so the main router's CentralWifiTracker can
        # self-heal a lost individual event, without needing ubus access
        # to this router. See WifiHeartbeat's docstring.
        heartbeat = WifiHeartbeat(config)
        trackers.append(heartbeat)
        t = threading.Thread(target=heartbeat.run, name="WifiHeartbeat", daemon=True)
        threads.append(t)

    for thread in threads:
        thread.start()

    # Handle OS termination signals cleanly
    stop_event = threading.Event()
    
    def handle_signal(signum, frame):
        log.info("Received termination signal (%s), shutting down...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Keep primary thread alive until signaled
    while not stop_event.is_set():
        try:
            time.sleep(1.0)
        except KeyboardInterrupt:
            break

    # Shutdown sequence
    for tracker in trackers:
        tracker.stop()
        
    publisher.stop()
    log.info("NetPulse Presence Hub stopped cleanly.")

if __name__ == "__main__":
    main()