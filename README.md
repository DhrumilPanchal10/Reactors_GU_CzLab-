# Reactors HMI — OPC-UA Control, Logging & Visualization  
**Stage 1 + Stage 2 Complete**

---

## Overview
This project implements a **Human–Machine Interface (HMI)** for a bioreactor laboratory setup.

The system allows operators and researchers to:

- Monitor live sensor data from reactors  
- Control actuators in real time  
- Log experiment data continuously  
- Visualize experiment data as it is being recorded  

The system communicates with a **Programmable Logic Controller (PLC)** via the **OPC-UA** standard.  
For development and testing, the PLC is currently represented by a **mock OPC-UA server** that mirrors the required address space. The same architecture is designed to connect directly to real lab hardware once final NodeIds are available.

**Key design principle:**  
➡️ *Live control, data logging, and visualization are fully decoupled.*

---

## System Architecture:
                    +-----------------------+
                    |      Web HMI (UI)     |
                    |    (Streamlit app)    |
                    |  - Live control (S1)  |
                    |  - Plots & selection  |
                    +-----------+-----------+
                                |
                                | OPC-UA (read / write / methods)
                                |
                    +-----------v-----------+
                    |   PLC / OPC-UA Server  |
                    | (mock server in dev)   |
                    | - Sensors: pH, DO,     |
                    |   Biomass (10 ch)      |
                    | - Actuators: pwm0..3   |
                    | - Methods: set_pairing |
                    +-----------+-----------+
                                |
                                | polled by
                                |
                    +-----------v-----------+
                    |     Sampler worker     |
                    | (background process)   |
                    | - polls OPC-UA at N s  |
                    | - inserts rows to DB   |
                    +-----------+-----------+
                                |
                                | writes
                                |
                    +-----------v-----------+
                    |     SQLite Database    |
                    | - experiments table    |
                    | - samples table        |
                    +------------------------+
This architecture ensures:
- UI stability (no async work inside Streamlit)
- Reliable long-running experiment logging
- Easy transition from mock PLC to real hardware

---

## Implemented Stages

### Stage 1 — Live Control (Complete)
Stage 1 focuses on **real-time operation during experiments**.

**Features**
- Live reads of:
  - pH  
  - Dissolved Oxygen (DO)  
  - All **10 biomass wavelength channels**
- Real-time actuator control for `pwm0`:
  - `method`
  - `time_on`
  - `time_off`
  - `lb`
  - `ub`
  - `setpoint`
- Direct invocation of OPC-UA methods:
  - `set_pairing`
  - `unpair`
- Reactor-scoped hierarchy (no data mixing between reactors)

The UI displays the relevant OPC-UA address space and confirms all writes by reading values back from the server.

---

### Stage 2 — Logging & Visualization (Complete)
Stage 2 adds **persistent data storage** and **realtime visualization**.

#### Logging
- A dedicated sampler process continuously polls OPC-UA
- Data is written to a SQLite database using an **experiment-based schema**
- Logged signals include:
  - All biomass wavelengths
  - pH and DO
  - Numeric actuator parameters

#### Visualization
- Realtime, auto-refreshing time-series plots driven from **SQLite**  
  *(not directly from OPC-UA)*
- Experiment selection
- Configurable time window
- Selectable signal channels
- Access to raw sample data for verification

This architecture supports long-running experiments without tying logging to the UI lifecycle.

---

## Current Status
- Reactor **R0** is fully implemented and validated  
- Reactors **R1 and R2** are structured as placeholders and can be enabled once final NodeIds are provided  
- The system is fully functional without physical hardware using the mock OPC-UA server  

---

## Known Limitations (By Design)
- R1/R2 are disabled until official NodeIds are confirmed  
- Only `pwm0` is enabled; `pwm1–pwm3` can be added using the same pattern  
- Calibration workflows are deferred to **Stage 3** (low priority)  

---

