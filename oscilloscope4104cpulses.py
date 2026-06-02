from __future__ import annotations

import time
import threading
import re
import shutil
from pathlib import Path
from queue import Queue
from typing import Dict, Tuple, List, Union, Optional
from datetime import datetime

import numpy as np
import pyvisa


# ========================= USER SETTINGS =========================
USE_DIRECT_RESOURCE = True
DIRECT_RESOURCE = "TCPIP0::169.254.2.219::INSTR"
# DIRECT_RESOURCE = "TCPIP0::169.254.2.219::4000::SOCKET"


# True: try DIRECT_RESOURCE first; if *IDN? fails, try common Tek LAN aliases.
# False: use only DIRECT_RESOURCE and stop immediately if it fails.
TRY_RESOURCE_FALLBACKS = True
SOCKET_SERVER_PORT = 4000

MODEL_MATCH = "MDO4104C"

OUTDIR = Path("broadcom_externalsupply_41_5V_darkcounts")

N_WF = 1500
RECORD_LENGTH = 100_000

# Filled after setup from the scope readback. This lets waveform transfer show
# both the requested and scope-applied record lengths.
APPLIED_RECORD_LENGTH: Optional[int] = None

# TRIGGERED: wait for TRIG_SOURCE/TRIG_LEVEL_V before each saved waveform.
# AUTO_UNTRIGGERED_ROLL: acquire without requiring an external trigger.
#   On MDO4000-series scopes, true roll display requires trigger AUTO mode and
#   a slow HORIZONTAL_SCALE_S, typically 40 ms/div or slower.
ACQUISITION_MODE = "AUTO_UNTRIGGERED_ROLL"  # TRIGGERED or AUTO_UNTRIGGERED_ROLL

# Scope display/acquisition geometry. Values are per division unless noted.
# Use None to leave a setting unchanged on the scope.

# HORIZONTAL_SCALE_S accepted
# 400ps
# 1, 2, 4, 10, 20, 40, 100, 200, 400 ns, us, ms, s
# 1ks

HORIZONTAL_SCALE_S = "100us"       # seconds/div; accepts strings like "10us", "100ms"
HORIZONTAL_DELAY_S = None       # seconds; accepts strings like "0s", "50us"
HORIZONTAL_POSITION_PCT = None  # percent, e.g. 50.0

SAVE_VERTICAL_SCALE_V = "2.5mV"    # volts/div; accepts strings like "5mV", "100mV"
SAVE_VERTICAL_OFFSET_V = None   # volts; accepts strings like "0V", "25mV"
SAVE_VERTICAL_POSITION_DIV = None  # divisions, e.g. 0.0

TRIG_SOURCE = "CH1"
TRIG_LEVEL_V = 0.850
TRIG_SLOPE = "RISE"        # RISE or FALL
TRIG_COUPLING = "DC"       # DC/AC/HFREJ/LFREJ/NOISEREJ (depends on scope)

SAVE_SOURCE = "CH4"
SET_BANDWIDTH = True
BANDWIDTH_OPTION = "20MHz"
# BANDWIDTH_OPTION = "100MHz"  # experimental; scope may reject or coerce this
# BANDWIDTH_OPTION = "250MHz"
# BANDWIDTH_OPTION = "FULL"  # same as 1GHz on MDO4104C
# ==================== RELIABILITY / PERFORMANCE TUNING ====================
# How many times to retry a single waveform index before aborting the run.
MAX_RETRIES_PER_WF = 8

# When the connection drops, try to reconnect this many times.
RECONNECT_MAX_ATTEMPTS = 8

# Seconds to wait between reconnect attempts (increases a bit with each try).
RECONNECT_BACKOFF_S = 1.0

# Poll interval while waiting for acquisition to complete (lower = more SCPI traffic).
POLL_S = 0.02

# Timeout waiting for trigger/acquisition completion (seconds)
ACQ_TIMEOUT_S = 30.0

# In AUTO_UNTRIGGERED_ROLL mode the scope may keep running instead of stopping
# by itself. The script waits at least this long, then explicitly stops and reads.
AUTO_CAPTURE_MIN_S = 0.10
AUTO_CAPTURE_SETTLE_RECORDS = 1.2

# If the direct VISA resource disappears (VI_ERROR_RSRC_NFOUND), try rediscovering the scope.
REDISCOVER_ON_RSRC_NFOUND = True

# CLEAR_LINE keeps one live status line and clears it before redrawing. This is
# compact like the old carriage-return mode, but survives terminal resizing much
# better because stale characters are erased.
# LINES prints every status as a permanent line.
PROGRESS_OUTPUT_MODE = "CLEAR_LINE"  # CLEAR_LINE or LINES




# Fidelity-oriented data return mode:
# FULL -> no display decimation (recommended)
# REDUced -> display-style decimated points
DATA_RESOLUTION = "FULL"

# Transfer width in bytes per sample: 1 or 2.
# 2 preserves more amplitude detail and is recommended for realistic shapes.
DATA_WIDTH = 2

FILE_PREFIX = "tek"

# If scope doesn't report endianness, fallback to this.
IS_BIG_ENDIAN = False

# Time formatting like -3.080e-07
TIME_SCI_DECIMALS = 3
# Voltage formatting like 0.027 (Tek-like)
VOLT_FMT = "%.6g"

# If you change timebase/vertical during run, you can refresh preamble every N waveforms
REFRESH_PREAMBLE_EVERY = 0  # 0 = never

# ================================================================



# ==================== CSV / SAVE SPEED TUNING ====================
# Use large OS buffer for CSV writes (bytes). Bigger -> fewer syscalls.
CSV_BUFFER_BYTES = 1024 * 1024  # 1 MiB

# Enable/disable output formats independently.
SAVE_CSV = True
SAVE_NPZ = False

# Write CSVs in a background thread while acquisition continues.
# Output CSV content/format is unchanged.
ASYNC_CSV_WRITER = True
# Backpressure buffer (number of waveforms waiting for disk write).
CSV_QUEUE_MAX_ITEMS = 16

# ================================================================

SettingValue = Union[str, float, int]

TIME_UNITS = {
    "ps": 1e-12,
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "μs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "sec": 1.0,
}

VOLT_UNITS = {
    "uv": 1e-6,
    "µv": 1e-6,
    "μv": 1e-6,
    "mv": 1e-3,
    "v": 1.0,
}

FREQ_UNITS = {
    "hz": 1.0,
    "khz": 1e3,
    "mhz": 1e6,
    "ghz": 1e9,
}

HORIZONTAL_SCALE_MIN_S = 400e-12
HORIZONTAL_SCALE_MAX_S = 1000.0
VERTICAL_SCALE_MIN_V = 1e-3
VERTICAL_SCALE_MAX_V = 10.0
TIME_SCALE_MANTISSAS = (1.0, 2.0, 4.0, 10.0, 20.0, 40.0, 100.0, 200.0, 400.0)


def parse_quantity(value: SettingValue, units: Dict[str, float], default_unit: str, setting_name: str) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        raise TypeError(f"{setting_name} must be None, a number, or a string with units.")

    text = value.strip().lower().replace(" ", "")
    match = re.fullmatch(r"([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?)([a-zµμ]*)", text)
    if not match:
        raise ValueError(f"Could not parse {setting_name}={value!r}. Example values: '10us', '100ms', '5mV'.")

    number = float(match.group(1))
    unit = match.group(2) or default_unit
    if unit not in units:
        allowed = ", ".join(sorted(units))
        raise ValueError(f"Unknown unit '{unit}' for {setting_name}. Allowed units: {allowed}.")
    return number * units[unit]


def build_horizontal_scale_values() -> List[float]:
    values = [400e-12]
    for unit_scale in (1e-9, 1e-6, 1e-3, 1.0):
        values.extend(mantissa * unit_scale for mantissa in TIME_SCALE_MANTISSAS)
    values.append(1000.0)
    return sorted(set(values))


HORIZONTAL_SCALE_VALUES_S = build_horizontal_scale_values()


def nearest_allowed_scale(value: float, allowed_values: List[float], setting_name: str) -> float:
    if value <= 0:
        raise ValueError(f"{setting_name} must be positive.")
    if value < allowed_values[0] or value > allowed_values[-1]:
        raise ValueError(
            f"{setting_name}={value:g} is outside allowed range {allowed_values[0]:g} to {allowed_values[-1]:g}."
        )
    return min(allowed_values, key=lambda candidate: abs(np.log(value / candidate)))


def parse_time(value: SettingValue, setting_name: str) -> float:
    return parse_quantity(value, TIME_UNITS, "s", setting_name)


def parse_voltage(value: SettingValue, setting_name: str) -> float:
    return parse_quantity(value, VOLT_UNITS, "v", setting_name)


def parse_frequency(value: SettingValue, setting_name: str) -> float:
    return parse_quantity(value, FREQ_UNITS, "hz", setting_name)


def parse_percent(value: SettingValue, setting_name: str) -> float:
    if isinstance(value, str):
        value = value.strip()
        if value.endswith("%"):
            value = value[:-1]
    return float(value)


def selected_horizontal_scale_s() -> Optional[float]:
    if HORIZONTAL_SCALE_S is None:
        return None
    requested = parse_time(HORIZONTAL_SCALE_S, "HORIZONTAL_SCALE_S")
    selected = nearest_allowed_scale(requested, HORIZONTAL_SCALE_VALUES_S, "HORIZONTAL_SCALE_S")
    if not np.isclose(requested, selected, rtol=1e-12, atol=0.0):
        print(f"Warning: HORIZONTAL_SCALE_S={requested:g} s/div is not a scope scale; using {selected:g} s/div.")
    return selected


def selected_vertical_scale_v() -> Optional[float]:
    if SAVE_VERTICAL_SCALE_V is None:
        return None
    requested = parse_voltage(SAVE_VERTICAL_SCALE_V, "SAVE_VERTICAL_SCALE_V")
    if requested <= 0:
        raise ValueError("SAVE_VERTICAL_SCALE_V must be positive.")
    if requested < VERTICAL_SCALE_MIN_V or requested > VERTICAL_SCALE_MAX_V:
        raise ValueError(
            f"SAVE_VERTICAL_SCALE_V={requested:g} is outside allowed range "
            f"{VERTICAL_SCALE_MIN_V:g} to {VERTICAL_SCALE_MAX_V:g} V/div."
        )
    return requested


def normalized_acquisition_mode() -> str:
    mode = ACQUISITION_MODE.strip().upper()
    aliases = {
        "TRIG": "TRIGGERED",
        "TRIGGER": "TRIGGERED",
        "ROLL": "AUTO_UNTRIGGERED_ROLL",
        "RLL": "AUTO_UNTRIGGERED_ROLL",
        "UNTRIGGERED": "AUTO_UNTRIGGERED_ROLL",
        "AUTO": "AUTO_UNTRIGGERED_ROLL",
        "AUTO_ROLL": "AUTO_UNTRIGGERED_ROLL",
    }
    mode = aliases.get(mode, mode)
    if mode not in ("TRIGGERED", "AUTO_UNTRIGGERED_ROLL"):
        raise ValueError(
            "ACQUISITION_MODE must be 'TRIGGERED' or 'AUTO_UNTRIGGERED_ROLL'."
        )
    return mode


def using_triggered_acquisition() -> bool:
    return normalized_acquisition_mode() == "TRIGGERED"


def rm_open() -> pyvisa.ResourceManager:
    return pyvisa.ResourceManager()


def safe_query(inst, cmd: str, default: str = "") -> str:
    try:
        return inst.query(cmd).strip()
    except Exception:
        return default


def qf(inst, cmd: str, default: float) -> float:
    s = safe_query(inst, cmd, "")
    if not s:
        return default
    try:
        return float(s)
    except ValueError:
        return default


def qi(inst, cmd: str, default: int) -> int:
    s = safe_query(inst, cmd, "")
    if not s:
        return default
    try:
        return int(float(s))
    except ValueError:
        return default


def tcpip_host_from_resource(resource: str) -> Optional[str]:
    """Extract host/IP from a TCPIP VISA resource string."""
    parts = resource.split("::")
    if len(parts) >= 2 and parts[0].upper().startswith("TCPIP"):
        return parts[1]
    return None


def direct_resource_candidates(resource: str) -> List[str]:
    """Build common Tek LAN resource strings from one direct resource."""
    candidates = [resource]
    if not TRY_RESOURCE_FALLBACKS:
        return candidates

    host = tcpip_host_from_resource(resource)
    if host:
        candidates.extend(
            [
                f"TCPIP0::{host}::inst0::INSTR",
                f"TCPIP0::{host}::{SOCKET_SERVER_PORT}::SOCKET",
            ]
        )

    # Keep order while removing duplicates.
    unique: List[str] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def connect(resource: str, timeout_ms: int = 20000):
    rm = rm_open()
    inst = rm.open_resource(resource)
    # print("THIS: " + inst.query("TRIGger:STATE?"))
    inst.timeout = timeout_ms
    inst.write_termination = "\n"
    inst.read_termination = "\n"
    # Faster transfers for large binary blocks
    try:
        inst.chunk_size = 8_000_000
    except Exception:
        pass
    return inst


def connect_any(resources: List[str], timeout_ms: int = 20000):
    """Try several VISA resource forms and return the first working session."""
    errors: List[str] = []
    for resource in resources:
        try:
            print(f"Trying VISA resource: {resource}")
            inst = connect(resource, timeout_ms=timeout_ms)
            idn = safe_query(inst, "*IDN?", default="").strip()
            if idn:
                print(f"Connected using: {resource}")
                return inst, resource, idn
            close_quiet(inst)
            error = f"{resource}: opened, but *IDN? returned empty"
            print(f"  {error}")
            errors.append(error)
        except BaseException as exc:
            error = f"{resource}: {exc}"
            print(f"  {error}")
            errors.append(error)

    detail = "\n".join(errors)
    if TRY_RESOURCE_FALLBACKS:
        raise RuntimeError(
            "Could not connect to the oscilloscope with any VISA resource form.\n"
            f"Tried:\n{detail}\n\n"
            "Enable the Tek VXI-11 Server for ::INSTR, or enable Socket Server "
            f"on port {SOCKET_SERVER_PORT} for ::SOCKET."
        )
    raise RuntimeError(
        "Could not connect to the oscilloscope with DIRECT_RESOURCE.\n"
        f"Tried:\n{detail}"
    )


def discover_scope(model_substring: str = "MDO4104C", timeout_ms: int = 2500, verbose: bool = True) -> Tuple[str, str]:
    rm = rm_open()
    resources = rm.list_resources()

    if verbose:
        print("VISA resources found:")
        for r in resources:
            print(" ", r)

    preferred = [r for r in resources if ("USB" in r.upper() or "TCPIP" in r.upper())]
    others = [r for r in resources if r not in preferred]
    scan_list = preferred + others

    last_errors: List[str] = []

    for res in scan_list:
        try:
            inst = rm.open_resource(res)
            inst.timeout = timeout_ms
            inst.write_termination = "\n"
            inst.read_termination = "\n"
            idn = inst.query("*IDN?").strip()
            close_quiet(inst)

            if verbose:
                print(f"  Test {res} -> {idn}")

            up = idn.upper()
            if "TEKTRONIX" in up and model_substring.upper() in up:
                print("\n✅ Found scope:", res)
                return res, idn

        except Exception as e:
            last_errors.append(f"{res}: {e}")
            if verbose:
                print(f"  Skip {res} (error: {e})")

    msg = "Scope not found.\n"
    if last_errors:
        msg += "Last errors:\n" + "\n".join(last_errors[-8:])
    raise RuntimeError(msg)


# ========================= CONNECTION RECOVERY =========================

def _is_winerror_10054(e: BaseException) -> bool:
    # Windows: "An existing connection was forcibly closed by the remote host"
    return isinstance(e, OSError) and getattr(e, "winerror", None) == 10054


def _is_invalid_session(e: BaseException) -> bool:
    return isinstance(e, getattr(pyvisa.errors, "InvalidSession", Exception))


def _is_rsrc_nfound(e: BaseException) -> bool:
    # VI_ERROR_RSRC_NFOUND (-1073807343): resource not present / insufficient location info
    return isinstance(e, getattr(pyvisa.errors, "VisaIOError", Exception)) and getattr(e, "error_code", None) == -1073807343


def close_quiet(inst) -> None:
    """Best-effort close/clear of a VISA session."""
    try:
        if inst is None:
            return
        try:
            inst.clear()
        except Exception:
            pass
        try:
            inst.close()
        except Exception:
            pass
    except Exception:
        pass


def reconnect(resource_hint: str, model_substring: str, timeout_ms: int = 20000, verbose: bool = True):
    """Reconnect to the scope, optionally rediscovering it if the resource vanishes."""
    last_err: Optional[BaseException] = None
    backoff = RECONNECT_BACKOFF_S

    for attempt in range(1, RECONNECT_MAX_ATTEMPTS + 1):
        try:
            # First try the previous resource (fast path)
            if resource_hint:
                inst = connect(resource_hint, timeout_ms=timeout_ms)
                idn = safe_query(inst, "*IDN?", default="").strip()
                if idn:
                    if verbose:
                        print(f"✅ Reconnected using cached resource: {resource_hint}")
                    return inst, resource_hint, idn
                close_quiet(inst)

            # If that fails (or returns empty), try rediscovery (slow path)
            if REDISCOVER_ON_RSRC_NFOUND:
                res, idn = discover_scope(model_substring=model_substring, timeout_ms=2500, verbose=verbose)
                inst = connect(res, timeout_ms=timeout_ms)
                idn2 = safe_query(inst, "*IDN?", default=idn).strip() or idn
                if verbose:
                    print(f"✅ Reconnected after rediscovery: {res}")
                return inst, res, idn2

        except BaseException as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            last_err = e
            if verbose:
                print(f"❌ Reconnect failed (attempt {attempt}/{RECONNECT_MAX_ATTEMPTS}): {e}")
            time.sleep(backoff)
            backoff = min(backoff * 1.3, 5.0)

    raise RuntimeError(f"Reconnect failed after {RECONNECT_MAX_ATTEMPTS} attempts. Last error: {last_err}")

def set_channel_bandwidth(inst, ch: str, option: str) -> None:
    opt = option.strip().upper()
    mapping: Dict[str, Union[str, float]] = {
        "FULL": "FULL",
        "1GHZ": "FULL",
        "1000MHZ": "FULL",
        "20MHZ": 20e6,
        "250MHZ": 250e6,
    }

    if opt in mapping:
        target = mapping[opt]
    else:
        target = parse_frequency(option, "BANDWIDTH_OPTION")
        if target <= 0:
            raise ValueError("BANDWIDTH_OPTION must be positive.")
        print(
            f"Warning: requesting experimental bandwidth {target:g} Hz. "
            "MDO4104C may reject it or coerce it to FULL/1GHz, 250MHz, or 20MHz."
        )

    if target == "FULL":
        print(f"{ch} bandwidth request: FULL")
        inst.write(f"{ch}:BANDWIDTH FULL")
    else:
        print(f"{ch} bandwidth request: {float(target):g} Hz")
        inst.write(f"{ch}:BANDWIDTH {float(target)}")

    rb = safe_query(inst, f"{ch}:BANDWIDTH?", default="")
    if rb:
        print(f"{ch} bandwidth readback: {rb}")


def apply_horizontal_settings(inst) -> None:
    horizontal_scale_s = selected_horizontal_scale_s()
    if horizontal_scale_s is not None:
        inst.write(f"HORizontal:MAIn:SCAle {horizontal_scale_s}")
    if HORIZONTAL_DELAY_S is not None:
        inst.write(f"HORizontal:DELay:TIMe {parse_time(HORIZONTAL_DELAY_S, 'HORIZONTAL_DELAY_S')}")
    if HORIZONTAL_POSITION_PCT is not None:
        inst.write(f"HORizontal:POSition {parse_percent(HORIZONTAL_POSITION_PCT, 'HORIZONTAL_POSITION_PCT')}")


def apply_vertical_settings(inst, ch: str) -> None:
    vertical_scale_v = selected_vertical_scale_v()
    if vertical_scale_v is not None:
        inst.write(f"{ch}:SCAle {vertical_scale_v}")
    if SAVE_VERTICAL_OFFSET_V is not None:
        inst.write(f"{ch}:OFFSet {parse_voltage(SAVE_VERTICAL_OFFSET_V, 'SAVE_VERTICAL_OFFSET_V')}")
    if SAVE_VERTICAL_POSITION_DIV is not None:
        inst.write(f"{ch}:POSition {float(SAVE_VERTICAL_POSITION_DIV)}")


def set_and_verify_trigger_mode(inst, mode: str) -> None:
    target = mode.strip().upper()
    if target not in ("AUTO", "NORMAL"):
        raise ValueError("Trigger mode must be AUTO or NORMAL.")

    inst.write(f"TRIGger:A:MODe {target}")
    readback = safe_query(inst, "TRIGger:A:MODe?", default="").strip().upper()
    if target == "NORMAL":
        accepted = readback.startswith("NORM")
    else:
        accepted = readback.startswith("AUTO")

    if not accepted:
        err = safe_query(inst, "ALLev?", default="").strip()
        detail = f" Scope error: {err}" if err else ""
        raise RuntimeError(
            f"Scope did not accept trigger mode {target}; readback was '{readback}'.{detail}"
        )


def setup_scope(inst):
    global APPLIED_RECORD_LENGTH

    acq_mode = normalized_acquisition_mode()
    inst.write("ACQuire:STATE STOP")
    try:
        inst.write("HORizontal:MODe MANual")
    except Exception:
        pass
    apply_horizontal_settings(inst)
    apply_vertical_settings(inst, SAVE_SOURCE)
    inst.write(f"HORizontal:RECOrdlength {RECORD_LENGTH}")
    rec_readback = qi(inst, "HORizontal:RECOrdlength?", RECORD_LENGTH)
    if rec_readback != RECORD_LENGTH:
        print(f"Warning: requested RECORD_LENGTH={RECORD_LENGTH}, scope applied {rec_readback}.")
    else:
        print(f"Record length applied: {rec_readback}")
    APPLIED_RECORD_LENGTH = rec_readback

    if acq_mode == "TRIGGERED":
        inst.write("TRIGger:A:TYPe EDGe")
        inst.write(f"TRIGger:A:EDGE:SOUrce {TRIG_SOURCE}")
        inst.write(f"TRIGger:A:EDGE:SLOPe {TRIG_SLOPE}")
        inst.write(f"TRIGger:A:EDGE:COUPling {TRIG_COUPLING}")
        inst.write(f"TRIGger:A:LEVel:{TRIG_SOURCE} {TRIG_LEVEL_V}")

    if acq_mode == "TRIGGERED":
        inst.write("ACQuire:STOPAfter SEQuence")
    else:
        inst.write("ACQuire:STOPAfter RUNSTop")
    inst.write("ACQuire:MODe SAMple")
    if acq_mode == "TRIGGERED":
        set_and_verify_trigger_mode(inst, "NORMAL")
    else:
        set_and_verify_trigger_mode(inst, "AUTO")


def arm_and_wait(inst, poll_s: float = 0.01, timeout_s: float = 30.0):
    inst.write("ACQuire:STATE RUN")
    t0 = time.time()
    while True:
        st = safe_query(inst, "ACQuire:STATE?", default="1")
        if st in ("0", "STOP", "STOPPED"):
            return
        if time.time() - t0 > timeout_s:
            raise TimeoutError("Timed out waiting for trigger/acquisition completion.")
        time.sleep(poll_s)


def run_auto_capture_window(
    inst,
    const: Dict[str, Union[float, int, bool, str]],
    poll_s: float = 0.01,
    timeout_s: float = 30.0,
) -> None:
    record_s = float(const["XINCR"]) * int(const["NR_PT"])
    wait_s = max(AUTO_CAPTURE_MIN_S, record_s * AUTO_CAPTURE_SETTLE_RECORDS)
    inst.write("ACQuire:STATE RUN")
    time.sleep(wait_s)
    inst.write("ACQuire:STATE STOP")

    t0 = time.time()
    while True:
        st = safe_query(inst, "ACQuire:STATE?", default="0")
        if st in ("0", "STOP", "STOPPED"):
            return
        if time.time() - t0 > timeout_s:
            raise TimeoutError("Timed out stopping auto acquisition before waveform read.")
        time.sleep(poll_s)


def acquire_and_wait(inst, const: Dict[str, Union[float, int, bool, str]], poll_s: float = 0.01, timeout_s: float = 30.0):
    if using_triggered_acquisition():
        arm_and_wait(inst, poll_s=poll_s, timeout_s=timeout_s)
    else:
        run_auto_capture_window(inst, const, poll_s=poll_s, timeout_s=timeout_s)


def acquisition_wait_status(i: int) -> str:
    if using_triggered_acquisition():
        return f"[{i}/{N_WF}] Waiting for trigger on {TRIG_SOURCE} @ {TRIG_LEVEL_V} V ..."
    return f"[{i}/{N_WF}] Acquiring auto untriggered roll waveform from {SAVE_SOURCE} ..."


def format_duration(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def print_progress(message: str) -> None:
    """Print acquisition progress using the configured terminal output mode."""
    mode = PROGRESS_OUTPUT_MODE.strip().upper()

    # Full-line logging is useful for saving the exact acquisition history.
    if mode == "LINES":
        print(message, flush=True)
        return

    # Compact live status line. The ANSI clear-line escape removes leftover
    # text from longer previous messages, and the current terminal width is used
    # so resize events do not leave stale characters visible.
    width = shutil.get_terminal_size((140, 20)).columns
    visible = message[: max(1, width - 1)]
    print("\r\033[2K" + visible, end="", flush=True)


def compact_progress_mode() -> bool:
    """Return True when progress output is limited to one live terminal line."""
    return PROGRESS_OUTPUT_MODE.strip().upper() != "LINES"


def progress_target_labels(saved_targets: List[str]) -> List[str]:
    """Shorten saved target labels in compact mode so timing and ETA stay visible."""
    if not compact_progress_mode():
        return saved_targets
    return [Path(target).name for target in saved_targets]


def get_preamble_constants(inst, source: str) -> Dict[str, Union[float, int, str, bool]]:
    """
    Correct units reliably by querying WFMOutpre:<KEY>? individually.
    This avoids preamble parsing issues and stops TIME=0,1,2,... bugs.
    """
    # Make binary transfers safe/clean
    try:
        inst.write("HEADER 0")
    except Exception:
        pass
    inst.write("*CLS")
    try:
        inst.clear()
    except Exception:
        pass

    inst.write(f"DATa:SOUrce {source}")
    inst.write("DATa:ENCdg RIBinary")
    data_resolution = DATA_RESOLUTION.strip().upper() if DATA_RESOLUTION else ""
    if data_resolution:
        try:
            inst.write(f"DATa:RESolution {data_resolution}")
        except Exception:
            pass

    requested_width: Optional[int] = None
    if DATA_WIDTH in (1, 2):
        requested_width = int(DATA_WIDTH)
        try:
            inst.write(f"DATa:WIDth {requested_width}")
        except Exception:
            pass

    ymult = qf(inst, "WFMOutpre:YMULT?", 1.0)
    yoff  = qf(inst, "WFMOutpre:YOFF?", 0.0)
    yzero = qf(inst, "WFMOutpre:YZERO?", 0.0)

    xincr = qf(inst, "WFMOutpre:XINCR?", 1.0)
    xzero = qf(inst, "WFMOutpre:XZERO?", 0.0)
    ptoff = qf(inst, "WFMOutpre:PT_OFF?", 0.0)

    # Use the scope-accepted record length. If the scope coerces the request
    # from 20000 to 12500, transfer 12500 instead of chopping to the request.
    rec_len_scope = qi(inst, "HORizontal:RECOrdlength?", APPLIED_RECORD_LENGTH or RECORD_LENGTH)
    requested_npt = RECORD_LENGTH
    target_npt = rec_len_scope if rec_len_scope > 0 else requested_npt
    print(
        f"Waveform transfer window request: {requested_npt} points; "
        f"scope record length readback: {rec_len_scope}; transfer stop: {target_npt}"
    )
    byt_nr = qi(inst, "WFMOutpre:BYT_NR?", requested_width if requested_width else 2)
    byt_or = safe_query(inst, "WFMOutpre:BYT_OR?", "").upper().strip()  # MSB/LSB sometimes blank
    bn_fmt = safe_query(inst, "WFMOutpre:BN_FMT?", "RI").upper().strip()

    if byt_or.startswith("MSB"):
        is_big_endian = True
    elif byt_or.startswith("LSB"):
        is_big_endian = False
    else:
        is_big_endian = IS_BIG_ENDIAN

    signed = (bn_fmt == "RI")

    # Apply width/range once and lock to requested width when valid.
    if requested_width in (1, 2):
        try:
            inst.write(f"DATa:WIDth {requested_width}")
        except Exception:
            pass
        byt_nr = qi(inst, "WFMOutpre:BYT_NR?", requested_width)
        if byt_nr != requested_width:
            print(f"Warning: requested DATA_WIDTH={requested_width}, scope applied {byt_nr}.")
    else:
        try:
            byt_nr = qi(inst, "WFMOutpre:BYT_NR?", byt_nr)
        except Exception:
            pass
        inst.write(f"DATa:WIDth {byt_nr}")

    inst.write("DATa:STARt 1")
    inst.write(f"DATa:STOP {target_npt}")
    data_start = qi(inst, "DATa:STARt?", 1)
    data_stop = qi(inst, "DATa:STOP?", target_npt)
    npt = max(1, data_stop - data_start + 1)
    if npt != target_npt:
        print(f"Warning: DATA window clamped to {npt} points (requested {target_npt}).")

    return {
        "YMULT": ymult, "YOFF": yoff, "YZERO": yzero,
        "XINCR": xincr, "XZERO": xzero, "PT_OFF": ptoff,
        "NR_PT": npt,
        "BYT_NR": byt_nr,
        "IS_BIG_ENDIAN": is_big_endian,
        "SIGNED": signed,
    }


def print_capture_readback(inst, const: Dict[str, Union[float, int, str, bool]]) -> None:
    """Print key scope readbacks to verify fidelity settings before acquisition."""
    data_res = safe_query(inst, "DATa:RESolution?", default="?")
    byt_nr = safe_query(inst, "WFMOutpre:BYT_NR?", default=str(const["BYT_NR"]))
    xinc = safe_query(inst, "WFMOutpre:XINCR?", default=str(const["XINCR"]))
    ymult = safe_query(inst, "WFMOutpre:YMULT?", default=str(const["YMULT"]))
    trig_mode = safe_query(inst, "TRIGger:A:MODe?", default="?")
    stop_after = safe_query(inst, "ACQuire:STOPAfter?", default="?")
    horiz_scale = safe_query(inst, "HORizontal:MAIn:SCAle?", default="?")
    horiz_delay = safe_query(inst, "HORizontal:DELay:TIMe?", default="?")
    horiz_pos = safe_query(inst, "HORizontal:POSition?", default="?")
    vert_scale = safe_query(inst, f"{SAVE_SOURCE}:SCAle?", default="?")
    vert_offset = safe_query(inst, f"{SAVE_SOURCE}:OFFSet?", default="?")
    vert_pos = safe_query(inst, f"{SAVE_SOURCE}:POSition?", default="?")
    print(
        "Readback: "
        f"ACQUISITION_MODE={normalized_acquisition_mode()}, "
        f"TRIG_MODE={trig_mode}, STOPAfter={stop_after}, "
        f"H_SCALE={horiz_scale}, H_DELAY={horiz_delay}, H_POS={horiz_pos}, "
        f"{SAVE_SOURCE}_SCALE={vert_scale}, {SAVE_SOURCE}_OFFSET={vert_offset}, "
        f"{SAVE_SOURCE}_POS={vert_pos}, "
        f"DATA:RESolution={data_res}, BYT_NR={byt_nr}, XINCR={xinc}, YMULT={ymult}"
    )


def read_curve_only(inst, const: Dict[str, Union[float, int, bool, str]]) -> np.ndarray:
    """
    Fast path: just CURVe? decode (no extra queries).
    """
    byt_nr = int(const["BYT_NR"])
    signed = bool(const["SIGNED"])
    is_big_endian = bool(const["IS_BIG_ENDIAN"])

    if byt_nr == 1:
        datatype = "b" if signed else "B"
        raw = inst.query_binary_values("CURVe?", datatype=datatype, container=np.array)
    else:
        datatype = "h" if signed else "H"
        raw = inst.query_binary_values("CURVe?", datatype=datatype, is_big_endian=is_big_endian, container=np.array)

    return np.asarray(raw, dtype=np.float64)


def scale_waveform(y_raw: np.ndarray, const: Dict[str, Union[float, int, bool, str]]) -> Tuple[np.ndarray, np.ndarray]:
    """
    Apply Tek scaling using cached constants.
    TIME array is precomputed outside; here we only do volts.
    """
    ymult = float(const["YMULT"])
    yoff  = float(const["YOFF"])
    yzero = float(const["YZERO"])
    npt   = int(const["NR_PT"])

    if y_raw.size > npt:
        y_raw = y_raw[:npt]
    else:
        npt = y_raw.size

    v = (y_raw - yoff) * ymult + yzero
    return v, y_raw


def build_time_array(const: Dict[str, Union[float, int, bool, str]]) -> np.ndarray:
    xincr = float(const["XINCR"])
    xzero = float(const["XZERO"])
    ptoff = float(const["PT_OFF"])
    npt   = int(const["NR_PT"])
    idx = np.arange(npt, dtype=np.float64)
    t = (idx - ptoff) * xincr + xzero
    return t


def build_header_lines(inst, idn: str, const: Dict[str, Union[float, int, bool, str]], save_source: str) -> List[str]:
    """
    Exact header layout like your pasted Tek files.
    """
    parts = [p.strip() for p in idn.split(",")] if idn else []
    model = parts[1] if len(parts) >= 2 else "MDO4104C"
    fw    = parts[3] if len(parts) >= 4 else ""

    horiz_scale = safe_query(inst, "HORizontal:MAIn:SCAle?", default="")
    horiz_delay = safe_query(inst, "HORizontal:DELay:TIMe?", default="")

    sample_interval = str(const["XINCR"])
    rec_len = str(const["NR_PT"])

    # Keep same line as your examples (full range)
    gating = "0.0% to 100.0%"

    probe_att   = safe_query(inst, f"{save_source}:PROBe?", default="1")
    vert_offset = safe_query(inst, f"{save_source}:OFFSet?", default="")
    vert_scale  = safe_query(inst, f"{save_source}:SCAle?", default="")
    vert_pos    = safe_query(inst, f"{save_source}:POSition?", default="")

    H: List[str] = []
    H.append(f"Model,{model}\n")
    H.append(f"Firmware Version,{fw}\n")
    H.append("\n")

    H.append("Waveform Type,ANALOG\n")
    H.append("Point Format,Y\n")
    H.append("Horizontal Units,s\n")
    H.append(f"Horizontal Scale,{horiz_scale}\n")
    H.append(f"Horizontal Delay,{horiz_delay}\n")
    H.append(f"Sample Interval,{sample_interval}\n")
    H.append(f"Record Length,{rec_len}\n")
    H.append(f"Gating,{gating}\n")
    H.append(f"Probe Attenuation,{probe_att}\n")
    H.append("Vertical Units,V\n")
    H.append(f"Vertical Offset,{vert_offset}\n")
    H.append(f"Vertical Scale,{vert_scale}\n")
    H.append(f"Vertical Position,{vert_pos}\n")

    H.append(",\n")
    H.append(",\n")
    H.append(",\n")
    H.append("Label,\n")
    H.append(f"TIME,{save_source}\n")
    return H


def write_npz_fast(path: Path, t: np.ndarray, v: np.ndarray):
    """Fast binary save (optional) - much faster than CSV, useful for later conversion."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Save as compressed npz (still quite fast; if you want maximum speed, switch to npy)
    np.savez_compressed(path, t=t, v=v)


def write_csv_fast(path: Path, header_lines: List[str], t: np.ndarray, v: np.ndarray):
    """
    Fast, correct writing:
    - Header is written exactly as lines (same as your pasted files)
    - Data is written by numpy.savetxt (much faster than per-row writerow)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", buffering=CSV_BUFFER_BYTES) as f:
        f.writelines(header_lines)
        data = np.column_stack((t, v))
        t_fmt = f"%.{TIME_SCI_DECIMALS}e"
        np.savetxt(f, data, delimiter=",", fmt=[t_fmt, VOLT_FMT])


def start_csv_writer():
    """
    Start background CSV writer.
    Queue item format: (path, header_lines, t_array, v_array).
    """
    q: Queue = Queue(maxsize=CSV_QUEUE_MAX_ITEMS)
    state: Dict[str, Optional[BaseException]] = {"error": None}

    def _worker():
        while True:
            item = q.get()
            try:
                if item is None:
                    return
                out, header_lines, t_local, v_local = item
                if state["error"] is None:
                    write_csv_fast(out, header_lines, t_local, v_local)
            except BaseException as e:
                if state["error"] is None:
                    state["error"] = e
            finally:
                q.task_done()

    th = threading.Thread(target=_worker, name="csv-writer", daemon=True)
    th.start()
    return q, state, th


def main():
    if not SAVE_CSV and not SAVE_NPZ:
        raise ValueError("At least one output format must be enabled: SAVE_CSV or SAVE_NPZ.")

    inst = None
    if USE_DIRECT_RESOURCE:
        resource = DIRECT_RESOURCE
        try:
            inst, resource, idn = connect_any(direct_resource_candidates(resource))
        except BaseException as e:
            if _is_rsrc_nfound(e):
                print(f"\nCould not open VISA resource: {resource}")
                print("Check scope power/network and run again.")
                return
            raise
        if not idn:
            close_quiet(inst)
            raise RuntimeError(f"Connected to {resource} but *IDN? returned empty.")
    else:
        resource, idn = discover_scope(model_substring=MODEL_MATCH, verbose=True)
        try:
            inst = connect(resource)
        except BaseException as e:
            if _is_rsrc_nfound(e):
                print(f"\nCould not open VISA resource: {resource}")
                print("Check scope power/network and run again.")
                return
            raise

    csv_queue = None
    csv_writer_state = None
    csv_writer_thread = None
    stop_requested = False

    try:
        print("\nConnected to:", idn)
        print("Resource:", resource)

        # Stability: clean session before heavy binary transfers
        try:
            inst.write("HEADER 0")
        except Exception:
            pass
        inst.write("*CLS")
        try:
            inst.clear()
        except Exception:
            pass

        if SET_BANDWIDTH:
            set_channel_bandwidth(inst, SAVE_SOURCE, BANDWIDTH_OPTION)

        setup_scope(inst)
        OUTDIR.mkdir(parents=True, exist_ok=True)

        # --- Cache constants + header + time array ONCE (big speedup) ---
        const = get_preamble_constants(inst, SAVE_SOURCE)
        print(f"Record length for transfer: {int(const['NR_PT'])}")
        print_capture_readback(inst, const)
        t_arr = build_time_array(const)
        header_lines = build_header_lines(inst, idn, const, SAVE_SOURCE)

        if SAVE_CSV and ASYNC_CSV_WRITER:
            csv_queue, csv_writer_state, csv_writer_thread = start_csv_writer()

        run_start_s = time.time()
        avg_waveform_s: Optional[float] = None
        last_progress_summary: Optional[str] = None

        for i in range(1, N_WF + 1):
            attempts = 0
            while True:
                try:
                    waveform_start_s = time.time()
                    if SAVE_CSV and csv_writer_state and csv_writer_state["error"] is not None:
                        err = csv_writer_state["error"]
                        raise RuntimeError(f"CSV writer failed: {err}") from err

                    # In compact auto-untriggered mode, keep the last saved
                    # timing/ETA line visible. The short "Acquiring..." line
                    # would otherwise overwrite it almost immediately.
                    show_wait_status = (
                        using_triggered_acquisition()
                        or not compact_progress_mode()
                        or avg_waveform_s is None
                    )
                    if show_wait_status:
                        status = acquisition_wait_status(i)
                        # In compact triggered mode, the scope may wait here
                        # for a while. Keep the latest timing/ETA visible while
                        # still showing that the script is armed for a trigger.
                        if compact_progress_mode() and using_triggered_acquisition() and last_progress_summary:
                            status = f"{status} | {last_progress_summary}"
                        print_progress(status)
                    acquire_and_wait(inst, const, timeout_s=ACQ_TIMEOUT_S, poll_s=POLL_S)

                    # Optional refresh if you expect settings to change during run
                    if REFRESH_PREAMBLE_EVERY and (i % REFRESH_PREAMBLE_EVERY == 0):
                        const = get_preamble_constants(inst, SAVE_SOURCE)
                        t_arr = build_time_array(const)
                        header_lines = build_header_lines(inst, idn, const, SAVE_SOURCE)

                    y_raw = read_curve_only(inst, const)
                    v_arr, _ = scale_waveform(y_raw, const)

                    # ts = time.strftime("%Y%m%d_%H%M%S")
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # milliseconds
                    out_base = OUTDIR / f"{FILE_PREFIX}_{SAVE_SOURCE}_BW{BANDWIDTH_OPTION}_{ts}_{i:04d}"
                    t_slice = t_arr[: len(v_arr)]
                    saved_targets: List[str] = []

                    if SAVE_CSV:
                        out_csv = out_base.with_suffix(".csv")
                        if ASYNC_CSV_WRITER and csv_queue is not None:
                            csv_queue.put((out_csv, header_lines, t_slice.copy(), v_arr.copy()))
                        else:
                            write_csv_fast(out_csv, header_lines, t_slice, v_arr)
                        saved_targets.append(str(out_csv))

                    if SAVE_NPZ:
                        out_npz = out_base.with_suffix(".npz")
                        write_npz_fast(out_npz, t_slice, v_arr)
                        saved_targets.append(str(out_npz))

                    waveform_s = time.time() - waveform_start_s
                    if avg_waveform_s is None:
                        avg_waveform_s = waveform_s
                    else:
                        avg_waveform_s = 0.8 * avg_waveform_s + 0.2 * waveform_s

                    remaining_wf = N_WF - i
                    eta_s = remaining_wf * avg_waveform_s
                    finish_at = datetime.fromtimestamp(time.time() + eta_s).strftime("%H:%M:%S")
                    elapsed = format_duration(time.time() - run_start_s)
                    eta = format_duration(eta_s)
                    last_progress_summary = (
                        f"last {waveform_s:.2f}s, avg {avg_waveform_s:.2f}s/wf, "
                        f"elapsed {elapsed}, ETA {eta} (finish {finish_at})"
                    )

                    # In compact progress mode, keep timing/ETA before filenames
                    # so terminal-width truncation does not hide run status.
                    if compact_progress_mode():
                        saved_msg = (
                            f"[{i}/{N_WF}] Saved | {last_progress_summary} | "
                            f"{' | '.join(progress_target_labels(saved_targets))}"
                        )
                    else:
                        saved_msg = (
                            f"[{i}/{N_WF}] Saved: {' | '.join(saved_targets)} | "
                            f"{last_progress_summary}"
                        )
                    print_progress(saved_msg)
                    break  # success -> next waveform

                except KeyboardInterrupt:
                    stop_requested = True
                    print("\nStop requested (Ctrl+C). Finalizing pending CSV writes...")
                    try:
                        inst.write("ACQuire:STATE STOP")
                    except Exception:
                        pass
                    break

                except TimeoutError as e:
                    attempts += 1
                    print(f"⚠️  Error (attempt {attempts}/{MAX_RETRIES_PER_WF}): {e}")
                    # Try to stop/clear acquisition and re-arm without reconnect first.
                    try:
                        inst.write("ACQuire:STATE STOP")
                    except Exception:
                        pass
                    if attempts >= MAX_RETRIES_PER_WF:
                        raise
                    continue

                except BaseException as e:
                    attempts += 1

                    # Treat connection drops / invalid sessions as recoverable:
                    is_conn_drop = _is_winerror_10054(e) or _is_invalid_session(e) or isinstance(
                        e, (ConnectionResetError, getattr(pyvisa.errors, "VisaIOError", Exception))
                    )

                    if not is_conn_drop:
                        # Unknown error -> re-raise immediately
                        raise

                    print(f"⚠️  Connection/IO error (attempt {attempts}/{MAX_RETRIES_PER_WF}): {e}")

                    # Ensure the old handle is not reused
                    close_quiet(inst)
                    inst = None

                    if attempts >= MAX_RETRIES_PER_WF:
                        raise

                    # Reconnect and reinitialize the scope settings & cached constants
                    inst, resource, idn = reconnect(resource, MODEL_MATCH, timeout_ms=20000, verbose=False)

                    # Re-apply config (needed after a fresh session)
                    try:
                        inst.write("HEADER 0")
                    except Exception:
                        pass
                    try:
                        inst.write("*CLS")
                    except Exception:
                        pass
                    try:
                        inst.clear()
                    except Exception:
                        pass

                    if SET_BANDWIDTH:
                        try:
                            set_channel_bandwidth(inst, SAVE_SOURCE, BANDWIDTH_OPTION)
                        except Exception:
                            pass

                    setup_scope(inst)
                    const = get_preamble_constants(inst, SAVE_SOURCE)
                    t_arr = build_time_array(const)
                    header_lines = build_header_lines(inst, idn, const, SAVE_SOURCE)
                    continue

            if stop_requested:
                break

        if SAVE_CSV and ASYNC_CSV_WRITER and csv_queue is not None and csv_writer_thread is not None:
            csv_queue.put(None)
            csv_queue.join()
            csv_writer_thread.join(timeout=10)
            if csv_writer_state and csv_writer_state["error"] is not None:
                err = csv_writer_state["error"]
                raise RuntimeError(f"CSV writer failed: {err}") from err
            csv_queue = None
            csv_writer_thread = None

        if stop_requested:
            print("\nStopped.")
        else:
            print("\nDone.")

    except KeyboardInterrupt:
        stop_requested = True
        print("\nStop requested (Ctrl+C). Finalizing pending CSV writes...")

    finally:
        if SAVE_CSV and ASYNC_CSV_WRITER and csv_queue is not None and csv_writer_thread is not None:
            try:
                if csv_writer_thread.is_alive():
                    csv_queue.put(None)
                    csv_queue.join()
                    csv_writer_thread.join(timeout=10)
            except Exception:
                pass
        if inst is not None:
            try:
                inst.clear()
            except Exception:
                pass
        close_quiet(inst)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
