# MDO4104C_py_adquisition_traces

Code for adquiring traces from oscilloscope MDO4104C via Ethernet connection. A lot of room for improvements. Many part were written by ChatGPT.

Things for setting up configurations on oscilloscope:

Change to your oscilloscope IP address: `DIRECT_RESOURCE = "TCPIP0::169.254.X.XXX::INSTR"`

Model of the device used (needed for detect the osiclloscope via pyvisa): `MODEL_MATCH = "MDO4104C"`

Output folder to save your traces: `OUTDIR = Path("folderpulses")`

Number of waveforms to save: `N_WF = 2000`

Number of points (sampling) in your trace: `RECORD_LENGTH = 10_000`

Acquisition mode:
```
ACQUISITION_MODE = "TRIGGERED"              # Use trigger channel/source
# ACQUISITION_MODE = "AUTO_UNTRIGGERED_ROLL" # No required trigger
```

For auto untriggered roll mode, Tektronix MDO4000 scopes use trigger `AUTO` mode. True roll display requires a slow `HORIZONTAL_SCALE_S`, typically `0.040` seconds/div or slower.

Horizontal and vertical scale:
```
HORIZONTAL_SCALE_S = None          # seconds/div, e.g. "10us", "100ms", 1e-6
HORIZONTAL_DELAY_S = None          # seconds, e.g. "0s", "50us", 0.0
HORIZONTAL_POSITION_PCT = None     # percent, e.g. 50.0

SAVE_VERTICAL_SCALE_V = None       # volts/div, e.g. "5mV", "100mV", 0.05
SAVE_VERTICAL_OFFSET_V = None      # volts, e.g. "0V", "25mV", 0.0
SAVE_VERTICAL_POSITION_DIV = None  # divisions, e.g. 0.0
```

These settings are applied to `SAVE_SOURCE` before data is acquired. Use `None` to keep the value already set on the oscilloscope.

For horizontal scale, the script snaps unsupported values to the nearest observed scope scale: `400ps`, then `1, 2, 4, 10, 20, 40, 100, 200, 400` in `ns`, `us`, `ms`, and `s`, plus `1ks`. For example, `"300us"` becomes `400 us/div`. Voltage scale is sent as entered after unit parsing, with only a range check from `1 mV/div` to `10 V/div`.

Trigger channel source and level: 
```
TRIG_SOURCE = "CH2"        # Channel used as trigger
TRIG_LEVEL_V = 0.1         # Trigger level in volts
TRIG_SLOPE = "RISE"        # RISE or FALL
TRIG_COUPLING = "DC"       # DC/AC/HFREJ/LFREJ/NOISEREJ (depends on scope)
```

Traces saved from selected channel (for example you are doing trigger in CH2 but your signal is in CH4): `SAVE_SOURCE = "CH4"`

Set bandwidth: 
```
SET_BANDWIDTH = True
BANDWIDTH_OPTION = "FULL"    # FULL/1GHz, 250MHz, or 20MHz
```

You can also try experimental numeric values like `"100MHz"`. The script sends the request and prints the scope readback, but the MDO4104C may reject it or coerce it to one of the displayed bandwidth options.
