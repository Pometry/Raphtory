from __future__ import annotations
import platform
from packaging import tags


OS_MAC = 'Darwin'
OS_LINUX = 'Linux'
OS_X64 = 'x64'
OS_AARCH64 = 'aarch64'


def getOS() -> str:
    platform_system = platform.system()
    if platform_system in (OS_MAC, OS_LINUX):
        return platform_system
    else:
        raise Exception("Unsupported OS. Cannot install.")


def getArch() -> str:
    if platform.machine() == "x86_64":
        return OS_X64
    elif platform.machine() == 'arm64':
        return OS_AARCH64
    else:
        raise Exception("Unsupported Architecture. Cannot install.")


def get_platform_tag() -> str:
    if getOS() == OS_MAC:
        if getArch() == OS_X64:
            return str(next(tags.mac_platforms((10, 11))))
        else:
            return str(next(tags.mac_platforms((11, 0))))
    else:
        return str(next(tags.sys_tags()).platform)
