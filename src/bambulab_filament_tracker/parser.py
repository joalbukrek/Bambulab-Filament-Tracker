from __future__ import annotations

import math
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .db import normalize_color


FLOAT_RE = re.compile(r"-?\d+(?:\.\d+)?")
COMMENT_KV_RE = re.compile(r"^;\s*([^=:]+?)\s*[:=]\s*(.*?)\s*$")
TOOL_RE = re.compile(r"^T(\d+)\b")
GCODE_E_RE = re.compile(r"\bE(-?\d+(?:\.\d+)?)\b")


@dataclass
class FilamentUse:
    slicer_index: int
    used_g: float
    length_mm: Optional[float] = None
    volume_cm3: Optional[float] = None
    material: str = ""
    color_hex: str = ""
    name: str = ""


@dataclass
class UsageReport:
    source_path: str
    plate_index: Optional[int]
    filaments: List[FilamentUse]
    total_used_g: float


def parse_usage_file(path: Path, plate_index: Optional[int] = None) -> UsageReport:
    suffixes = [suffix.lower() for suffix in path.suffixes]
    if ".3mf" in suffixes:
        return parse_3mf(path, plate_index=plate_index)
    text = path.read_text(encoding="utf-8", errors="replace")
    filaments = parse_gcode_usage(text)
    return UsageReport(str(path), plate_index, filaments, sum(item.used_g for item in filaments))


def parse_3mf(path: Path, plate_index: Optional[int] = None) -> UsageReport:
    with zipfile.ZipFile(path) as archive:
        gcode_name = select_plate_gcode(archive.namelist(), plate_index)
        if not gcode_name:
            raise ValueError("No Metadata/plate_*.gcode file found inside 3MF archive")
        text = archive.read(gcode_name).decode("utf-8", errors="replace")
        filaments = parse_gcode_usage(text)
        selected_plate = plate_index
        if selected_plate is None:
            selected_plate = plate_index_from_name(gcode_name)
        return UsageReport(str(path), selected_plate, filaments, sum(item.used_g for item in filaments))


def select_plate_gcode(names: Sequence[str], plate_index: Optional[int]) -> Optional[str]:
    candidates = sorted(
        name
        for name in names
        if name.lower().startswith("metadata/plate_") and name.lower().endswith(".gcode")
    )
    if not candidates:
        candidates = sorted(name for name in names if name.lower().endswith(".gcode"))
    if plate_index is None:
        return candidates[0] if candidates else None

    expected = "metadata/plate_%s.gcode" % (plate_index + 1)
    for name in candidates:
        if name.lower() == expected:
            return name
    raise ValueError("Plate index %s was requested, but %s was not found" % (plate_index, expected))


def plate_index_from_name(name: str) -> Optional[int]:
    match = re.search(r"plate_(\d+)\.gcode$", name, re.IGNORECASE)
    if not match:
        return None
    return int(match.group(1)) - 1


def parse_gcode_usage(text: str) -> List[FilamentUse]:
    metadata = extract_comment_metadata(text)
    weights = first_number_list(
        metadata,
        "filament used [g]",
        "total filament used [g]",
        "total filament weight [g]",
        "filament_used_g",
        "extruded_weight",
        "filament weight",
    )
    lengths = first_number_list(
        metadata,
        "filament used [mm]",
        "total filament length [mm]",
        "filament_used_mm",
        "used_filament",
    )
    volumes = first_number_list(
        metadata,
        "filament used [cm3]",
        "total filament volume [cm3]",
        "filament_used_cm3",
        "extruded_volume",
    )
    densities = first_number_list(metadata, "filament_density", "density")
    diameters = first_number_list(metadata, "filament_diameter", "diameter")
    materials = first_text_list(metadata, "filament_type", "filament_types")
    colors = first_text_list(metadata, "filament_colour", "filament_color", "filament_colours", "filament_colors")
    names = first_text_list(metadata, "filament_settings_id", "filament_preset", "filament_notes")

    if not weights:
        weights = weights_from_volume_or_length(lengths, volumes, densities, diameters)
    if not weights:
        fallback_lengths = estimate_extrusion_by_tool(text)
        if fallback_lengths:
            max_index = max(fallback_lengths)
            lengths = [fallback_lengths.get(index, 0.0) for index in range(max_index + 1)]
            weights = weights_from_volume_or_length(lengths, volumes, densities, diameters)

    if not weights:
        raise ValueError("Could not find filament usage in G-code metadata")

    count = len(weights)
    filaments: List[FilamentUse] = []
    for index in range(count):
        used_g = weights[index]
        if used_g <= 0:
            continue
        filaments.append(
            FilamentUse(
                slicer_index=index,
                used_g=used_g,
                length_mm=value_at(lengths, index),
                volume_cm3=value_at(volumes, index),
                material=text_at(materials, index),
                color_hex=normalize_color(text_at(colors, index)),
                name=text_at(names, index),
            )
        )

    if not filaments:
        raise ValueError("Filament usage was found, but every filament had zero grams")
    return filaments


def extract_comment_metadata(text: str) -> Dict[str, str]:
    metadata: Dict[str, str] = {}
    for line in text.splitlines():
        match = COMMENT_KV_RE.match(line.strip())
        if not match:
            continue
        key = normalize_key(match.group(1))
        metadata[key] = match.group(2).strip()
    return metadata


def normalize_key(key: str) -> str:
    return re.sub(r"\s+", " ", key.strip().lower().replace("_", " "))


def first_number_list(metadata: Dict[str, str], *keys: str) -> List[float]:
    for key in keys:
        normalized = normalize_key(key)
        if normalized in metadata:
            values = parse_number_list(metadata[normalized])
            if values:
                return values
    return []


def first_text_list(metadata: Dict[str, str], *keys: str) -> List[str]:
    for key in keys:
        normalized = normalize_key(key)
        if normalized in metadata:
            values = parse_text_list(metadata[normalized])
            if values:
                return values
    return []


def parse_number_list(value: str) -> List[float]:
    return [float(match.group(0)) for match in FLOAT_RE.finditer(value)]


def parse_text_list(value: str) -> List[str]:
    cleaned = value.strip()
    if not cleaned:
        return []
    if ";" in cleaned:
        parts = cleaned.split(";")
    else:
        parts = cleaned.split(",")
    return [part.strip().strip('"').strip("'") for part in parts if part.strip()]


def weights_from_volume_or_length(
    lengths_mm: Sequence[float],
    volumes_cm3: Sequence[float],
    densities: Sequence[float],
    diameters: Sequence[float],
) -> List[float]:
    if volumes_cm3 and densities:
        return [
            volume * density_for_index(densities, index)
            for index, volume in enumerate(volumes_cm3)
        ]

    if not lengths_mm or not densities:
        return []

    weights: List[float] = []
    for index, length in enumerate(lengths_mm):
        diameter = diameter_for_index(diameters, index)
        area_mm2 = math.pi * (diameter / 2.0) ** 2
        volume_cm3 = length * area_mm2 / 1000.0
        weights.append(volume_cm3 * density_for_index(densities, index))
    return weights


def density_for_index(densities: Sequence[float], index: int) -> float:
    if not densities:
        return 1.24
    if index < len(densities):
        return densities[index]
    return densities[-1]


def diameter_for_index(diameters: Sequence[float], index: int) -> float:
    if not diameters:
        return 1.75
    if index < len(diameters):
        return diameters[index]
    return diameters[-1]


def value_at(values: Sequence[float], index: int) -> Optional[float]:
    if index < len(values):
        return values[index]
    return None


def text_at(values: Sequence[str], index: int) -> str:
    if index < len(values):
        return values[index]
    return ""


def estimate_extrusion_by_tool(text: str) -> Dict[int, float]:
    active_tool = 0
    relative_e = True
    last_absolute_e = 0.0
    totals: Dict[int, float] = {}

    for raw_line in text.splitlines():
        line = raw_line.split(";", 1)[0].strip()
        if not line:
            continue
        tool_match = TOOL_RE.match(line)
        if tool_match:
            active_tool = int(tool_match.group(1))
            last_absolute_e = 0.0
            continue
        if line.startswith("M82"):
            relative_e = False
            last_absolute_e = 0.0
            continue
        if line.startswith("M83"):
            relative_e = True
            continue
        if line.startswith("G92") and "E" in line:
            e_match = GCODE_E_RE.search(line)
            last_absolute_e = float(e_match.group(1)) if e_match else 0.0
            continue
        if not (line.startswith("G0") or line.startswith("G1")):
            continue
        e_match = GCODE_E_RE.search(line)
        if not e_match:
            continue
        e_value = float(e_match.group(1))
        if relative_e:
            extrusion = e_value
        else:
            extrusion = e_value - last_absolute_e
            last_absolute_e = e_value
        if extrusion > 0:
            totals[active_tool] = totals.get(active_tool, 0.0) + extrusion
    return totals


def parse_manual_mapping(value: str) -> Dict[int, int]:
    """Parse a mapping like `0:2,1:4` as slicer-index to 1-based AMS slot."""
    mapping: Dict[int, int] = {}
    if not value.strip():
        return mapping
    for part in value.split(","):
        if ":" not in part:
            raise ValueError("Invalid mapping part %r. Expected slicer_index:ams_slot" % part)
        key, slot = part.split(":", 1)
        mapping[int(key.strip())] = int(slot.strip())
    return mapping


def bambu_ams_mapping_to_slots(mapping: Optional[Iterable[int]]) -> Dict[int, Optional[int]]:
    """Convert Bambu zero-based `ams_mapping` entries to user-facing AMS slots."""
    if mapping is None:
        return {}
    result: Dict[int, Optional[int]] = {}
    for slicer_index, raw_slot in enumerate(mapping):
        if raw_slot is None or int(raw_slot) < 0:
            result[slicer_index] = None
        else:
            result[slicer_index] = int(raw_slot) + 1
    return result


def usage_by_slot(
    filaments: Sequence[FilamentUse],
    slicer_to_slot: Dict[int, Optional[int]],
    fallback_slot: Optional[int] = None,
) -> List[Tuple[FilamentUse, Optional[int]]]:
    rows: List[Tuple[FilamentUse, Optional[int]]] = []
    for filament in filaments:
        slot = slicer_to_slot.get(filament.slicer_index, fallback_slot)
        rows.append((filament, slot))
    return rows
