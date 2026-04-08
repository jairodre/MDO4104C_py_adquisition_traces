# MDO4104C_py_adquisition_traces

Code for adquiring reading from oscilloscope MDO4104C via Ethernet connection.

Things for setting up configurations on oscilloscope:

Modify this for your oscilloscope IP address: `DIRECT_RESOURCE = "TCPIP0::169.254.X.XXX::INSTR"`

Model of the device used: `MODEL_MATCH = "MDO4104C"`

Output folder to save your traces: `OUTDIR = Path("folderpulses")`

Number of waveforms to save: `N_WF = 20`

Number of points (sampling) in your trace: `RECORD_LENGTH = 10_000`

Triger channel source and level: 
`
TRIG_SOURCE = "CH2"
TRIG_LEVEL_V = 0.1         # Trigger level in volts
TRIG_SLOPE = "RISE"        # RISE or FALL
TRIG_COUPLING = "DC"       # DC/AC/HFREJ/LFREJ/NOISEREJ (depends on scope)
`

Traces saved from selected channel (for example you are doing trigger in CH2 but your signal is in CH4): `SAVE_SOURCE = "CH4"`

Set bandwidth: 
`
SET_BANDWIDTH = True
BANDWIDTH_OPTION = "FULL"
`
