# Reactors HMI — OPC-UA Control & Monitoring System  
**Stage 1 + Stage 2 Implementation**

## Overview
This project is a **Human–Machine Interface (HMI)** for a bioreactor laboratory setup.  
It provides a single dashboard that allows operators and researchers to **monitor live reactor data**, **control actuators in real time**, and **record and visualize experiments over time**.

The system communicates with a **PLC using the OPC-UA standard**, which is widely used in industrial and laboratory automation.  
For development and testing, the PLC is currently represented by a **mock OPC-UA server** that mirrors the lab’s required address space. Once real hardware is available, the same interface can be pointed directly to the physical PLC without architectural changes.

## Why This Matters
Bioreactor experiments require precise control and generate large volumes of data.  
This HMI replaces fragmented or manual workflows with a single operator-facing interface, clear separation between live control and data logging, and a design that supports long-running experiments and direct scaling to real lab hardware.

## High-Level Architecture

```
UI (Web Dashboard)
        │
        │ OPC-UA (read/write/method calls)
        ▼
PLC / OPC-UA Server
        │
        │ Continuous sampling
        ▼
Sampler Service
        │
        ▼
SQLite Database
```

## Features

### Stage 1 — Live Control
Stage 1 focuses on **real-time operation during experiments**.

- Live monitoring of pH, dissolved oxygen (DO), and biomass sensor values
- Real-time actuator control via OPC-UA:
  - setpoint
  - lower and upper bounds (lb, ub)
  - time_on and time_off
  - control method
- Direct invocation of OPC-UA methods such as `set_pairing`
- Immediate feedback: values written to the PLC are read back and displayed

This is the primary interface an operator would use while an experiment is running.

### Stage 2 — Logging & Historical Visualization
Stage 2 adds **persistent data storage and analysis**.

- A dedicated background sampler continuously reads data from OPC-UA
- All data is stored in a lightweight SQLite database
- Logged signals include:
  - All 10 biomass wavelength channels
  - pH and DO
  - Numeric actuator parameters
- The dashboard provides:
  - Time-windowed plots
  - Selectable biomass channels
  - Latest recorded values per reactor

Logging is decoupled from the UI so experiments continue recording even if the dashboard is closed.

## Current Status
- Reactor R0 is fully implemented and validated
- The system is reactor-agnostic by design
- Reactors R1 and R2 can be enabled once final OPC-UA NodeIds are confirmed
- Development is hardware-independent using a mock OPC-UA server

## Remaining Work
- Enable additional actuators (pwm1–pwm3)
- Finalize and activate R1 and R2 NodeIds
- Optionally log non-numeric actuator states such as control modes
- Optional authentication and role separation

## How to Run

### Start the OPC-UA server (mock PLC)
```bash
python mock_server.py
```

### Start the sampler (continuous logging)
```bash
python sampler.py 1
```

### Start the dashboard
```bash
streamlit run app.py
```

The dashboard opens in a web browser and provides both live control and historical views.

## Technology Stack
- Python
- OPC-UA (asyncua)
- Streamlit (web UI)
- SQLite (experiment logging)

## Design Philosophy
- Clear separation of live control, logging, and visualization
- Industrial-style architecture
- Safe development without hardware dependency
- Ready to transition to real PLC hardware with minimal changes

## Summary
This project delivers a complete **Stage 1 + Stage 2 HMI** for a multi-reactor system with live control, continuous logging, and historical visualization. It provides a strong foundation for future calibration, automation, and experiment management.
