from dataclasses import dataclass


@dataclass(frozen=True)
class SettingsIntervalCallback:
    chat_id: int
    seconds: int


def parse_settings_interval(data: str) -> SettingsIntervalCallback | None:
    # format: settings:interval:<chat_id>:<seconds>
    parts = data.split(":")
    if len(parts) != 4:
        return None
    if parts[0] != "settings" or parts[1] != "interval":
        return None
    try:
        return SettingsIntervalCallback(chat_id=int(parts[2]), seconds=int(parts[3]))
    except ValueError:
        return None

