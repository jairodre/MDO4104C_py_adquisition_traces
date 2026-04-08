from __future__ import annotations

import time
import threading
from pathlib import Path
from queue import Queue
from typing import Dict, Tuple, List, Union, Optional
from datetime import datetime

import numpy as np
import pyvisa


# ========================= USER SETTINGS =========================
USE_DIRECT_RESOURCE = True
DIRECT_RESOURCE = "TCPIP0::169.254.2.219::INSTR"

MODEL_MATCH = "MDO4104C"

OUTDIR = Path("folderpulses")

N_WF = 20
RECORD_LENGTH = 10_000

TRIG_SOURCE = "CH4"
TRIG_LEVEL_V = 0.1
TRIG_SLOPE = "RISE"        # RISE or FALL
TRIG_COUPLING = "DC"       # DC/AC/HFREJ/LFREJ/NOISEREJ (depends on scope)

SAVE_SOURCE = "CH4"
SET_BANDWIDTH = True
BANDWIDTH_OPTION = "FULL"
# BANDWIDTH_OPTION = "20MHz"
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

# If the direct VISA resource disappears (VI_ERROR_RSRC_NFOUND), try rediscovering the scope.
REDISCOVER_ON_RSRC_NFOUND = True




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
        "20MHZ": 20e6,
        "250MHZ": 250e6,
    }

    if opt in mapping:
        target = mapping[opt]
    else:
        try:
            target = float(opt)
        except ValueError as e:
            raise ValueError(
                f"Unknown bandwidth option '{option}'. Use FULL, 20MHz, 250MHz, or numeric Hz like '2e7'."
            ) from e

    if target == "FULL":
        inst.write(f"{ch}:BANDWIDTH FULL")
    else:
        inst.write(f"{ch}:BANDWIDTH {float(target)}")

    rb = safe_query(inst, f"{ch}:BANDWIDTH?", default="")
    if rb:
        print(f"{ch} bandwidth readback: {rb}")


def setup_scope(inst):
    inst.write("ACQuire:STATE STOP")
    try:
        inst.write("HORizontal:MODe MANual")
    except Exception:
        pass
    inst.write(f"HORizontal:RECOrdlength {RECORD_LENGTH}")
    rec_readback = qi(inst, "HORizontal:RECOrdlength?", RECORD_LENGTH)
    if rec_readback != RECORD_LENGTH:
        print(f"Warning: requested RECORD_LENGTH={RECORD_LENGTH}, scope applied {rec_readback}.")

    inst.write("TRIGger:A:TYPe EDGe")
    inst.write(f"TRIGger:A:EDGE:SOUrce {TRIG_SOURCE}")
    inst.write(f"TRIGger:A:EDGE:SLOPe {TRIG_SLOPE}")
    inst.write(f"TRIGger:A:EDGE:COUPling {TRIG_COUPLING}")
    inst.write(f"TRIGger:A:LEVel:{TRIG_SOURCE} {TRIG_LEVEL_V}")

    inst.write("ACQuire:STOPAfter SEQuence")
    inst.write("ACQuire:MODe SAMple")


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

    # WFMOutpre:NR_PT? may report display-decimated points on some firmware.
    # Use horizontal record length + DATA:STOP readback to force full transfer span.
    rec_len_scope = qi(inst, "HORizontal:RECOrdlength?", RECORD_LENGTH)
    target_npt = min(rec_len_scope, RECORD_LENGTH)
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
    print(f"Readback: DATA:RESolution={data_res}, BYT_NR={byt_nr}, XINCR={xinc}, YMULT={ymult}")


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
            inst = connect(resource)
        except BaseException as e:
            if _is_rsrc_nfound(e):
                print(f"\nCould not open VISA resource: {resource}")
                print("Check scope power/network and run again.")
                return
            raise
        idn = safe_query(inst, "*IDN?", default="").strip()
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

        for i in range(1, N_WF + 1):
            attempts = 0
            while True:
                try:
                    if SAVE_CSV and csv_writer_state and csv_writer_state["error"] is not None:
                        err = csv_writer_state["error"]
                        raise RuntimeError(f"CSV writer failed: {err}") from err

                    status = f"[{i}/{N_WF}] Waiting for trigger on {TRIG_SOURCE} @ {TRIG_LEVEL_V} V ..."
                    print("\r" + status.ljust(140), end="\r", flush=True)
                    arm_and_wait(inst, timeout_s=ACQ_TIMEOUT_S, poll_s=POLL_S)

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

                    saved_msg = f"[{i}/{N_WF}] Saved: {' | '.join(saved_targets)}"
                    print("\r" + saved_msg.ljust(140), end="\r", flush=True)
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
