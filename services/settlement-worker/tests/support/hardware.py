"""The machine a measured number ran on (D-024, D-067): CPU model, logical CPUs and OS."""

import os
import platform
import sys


def cpu_model() -> str:
    if sys.platform == "win32":
        import winreg

        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0") as key:
            return str(winreg.QueryValueEx(key, "ProcessorNameString")[0]).strip()
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as cpuinfo:
            for line in cpuinfo:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def summary_fields() -> str:
    """cpu is quoted: model names contain spaces."""
    return f'cpu="{cpu_model()}" logical_cpus={os.cpu_count()} os={platform.platform()}'
