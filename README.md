# MDO4104C_py_adquisition_traces

Code for adquiring traces from oscilloscope MDO4104C via Ethernet connection. A lot of room for improvements. Many part were written by ChatGPT.

Things for setting up configurations on oscilloscope:

Change to your oscilloscope IP address: `DIRECT_RESOURCE = "TCPIP0::169.254.X.XXX::INSTR"`

Model of the device used (needed for detect the osiclloscope via pyvisa): `MODEL_MATCH = "MDO4104C"`

Output folder to save your traces: `OUTDIR = Path("folderpulses")`

Number of waveforms to save: `N_WF = 2000`

Number of points (sampling) in your trace: `RECORD_LENGTH = 10_000`

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
BANDWIDTH_OPTION = "FULL"
```
