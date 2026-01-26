# variable_map.py
"""
Authoritative OPC-UA variable mapping for Reactor R0.

This file exists to make the mapping between:
- NodeIds (ns=2;i=...)
- logical meaning (sensor/actuator/channel)
explicit and auditable.

This matches the requirements document exactly for R0.
"""

from client import VariableInfo

def reactor_map_R0():
    """
    Returns:
        Dict[str, VariableInfo]
        key   = nodeid string (e.g. "ns=2;i=3")
        value = VariableInfo metadata
    """

    return {
        # ---- pH sensor ----
        "ns=2;i=3": VariableInfo(
            reactor="R0",
            kind="sensor",
            group="ph",
            channel="pH",
            nodeid="ns=2;i=3",
        ),
        "ns=2;i=4": VariableInfo(
            reactor="R0",
            kind="sensor",
            group="ph",
            channel="oC",
            nodeid="ns=2;i=4",
        ),

        # ---- DO sensor ----
        "ns=2;i=6": VariableInfo(
            reactor="R0",
            kind="sensor",
            group="do",
            channel="ppm",
            nodeid="ns=2;i=6",
        ),
        "ns=2;i=7": VariableInfo(
            reactor="R0",
            kind="sensor",
            group="do",
            channel="oC",
            nodeid="ns=2;i=7",
        ),

        # ---- Biomass sensor (10 wavelengths) ----
        "ns=2;i=9": VariableInfo("R0", "sensor", "biomass", "415", "ns=2;i=9"),
        "ns=2;i=10": VariableInfo("R0", "sensor", "biomass", "445", "ns=2;i=10"),
        "ns=2;i=11": VariableInfo("R0", "sensor", "biomass", "480", "ns=2;i=11"),
        "ns=2;i=12": VariableInfo("R0", "sensor", "biomass", "515", "ns=2;i=12"),
        "ns=2;i=13": VariableInfo("R0", "sensor", "biomass", "555", "ns=2;i=13"),
        "ns=2;i=14": VariableInfo("R0", "sensor", "biomass", "590", "ns=2;i=14"),
        "ns=2;i=15": VariableInfo("R0", "sensor", "biomass", "630", "ns=2;i=15"),
        "ns=2;i=16": VariableInfo("R0", "sensor", "biomass", "680", "ns=2;i=16"),
        "ns=2;i=17": VariableInfo("R0", "sensor", "biomass", "clear", "ns=2;i=17"),
        "ns=2;i=18": VariableInfo("R0", "sensor", "biomass", "nir", "ns=2;i=18"),

        # ---- pwm0 actuator (ControlMethod variables) ----
        "ns=2;i=23": VariableInfo("R0", "actuator", "pwm0", "method", "ns=2;i=23"),
        "ns=2;i=24": VariableInfo("R0", "actuator", "pwm0", "time_on", "ns=2;i=24"),
        "ns=2;i=25": VariableInfo("R0", "actuator", "pwm0", "time_off", "ns=2;i=25"),
        "ns=2;i=26": VariableInfo("R0", "actuator", "pwm0", "lb", "ns=2;i=26"),
        "ns=2;i=27": VariableInfo("R0", "actuator", "pwm0", "ub", "ns=2;i=27"),
        "ns=2;i=28": VariableInfo("R0", "actuator", "pwm0", "setpoint", "ns=2;i=28"),
    }


def method_ids_R0():
    """
    OPC-UA method NodeIds for R0 (from requirements).
    """
    return {
        "set_pairing": "ns=2;i=232",
        "unpair": "ns=2;i=235",
    }