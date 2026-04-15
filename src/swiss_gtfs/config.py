"""Typed configuration dataclasses for each pipeline stage.

Each CLI entry point converts its argparse.Namespace into the appropriate
config object before passing anything to business-logic modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

VALID_SCALES = ("canton", "agglomeration", "district", "commune")


def _validate_scale(scale: str) -> None:
    if scale not in VALID_SCALES:
        raise ValueError(f"scale must be one of {VALID_SCALES}, got '{scale}'")


@dataclass
class FilterConfig:
    """Configuration for the GTFS spatial-filtering stage."""

    scale: str
    in_dir: str = "Data"
    out_dir: str = "Data"
    geodata_dir: str = "geodata/"

    def __post_init__(self) -> None:
        _validate_scale(self.scale)

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "FilterConfig":
        geodata_dir = getattr(args, "geodata_dir", "geodata/")
        return cls(
            scale=args.scale,
            in_dir=getattr(args, "in_path", "Data"),
            out_dir=getattr(args, "out_dir", "Data"),
            geodata_dir=geodata_dir,
        )


@dataclass
class GTFSSourceConfig:
    """Configuration for downloading / resolving raw GTFS data."""

    data_dir: str = "Data"
    gtfs_version: str | None = None
    gtfs_path: str | None = None
    refresh_gtfs: bool = False

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "GTFSSourceConfig":
        return cls(
            data_dir=getattr(args, "data_dir", "Data"),
            gtfs_version=getattr(args, "gtfs_version", None),
            gtfs_path=getattr(args, "gtfs_path", None),
            refresh_gtfs=getattr(args, "refresh_gtfs", False),
        )


@dataclass
class BoundaryConfig:
    """Configuration for boundary shapefile management."""

    geodata_dir: str = "geodata/"
    refresh_boundaries: bool = False

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "BoundaryConfig":
        return cls(
            geodata_dir=getattr(args, "geodata_dir", "geodata/"),
            refresh_boundaries=getattr(args, "refresh_boundaries", False),
        )


@dataclass
class GraphConfig:
    """Configuration for the graph-building stage."""

    scale: str
    start_time: str = "07:00:00"
    end_time: str = "10:00:00"
    directed: bool = True
    save_format: str = "both"
    calendar_start: str | None = None
    calendar_end: str | None = None
    output_dir: str = "outputs/graphs"
    skip_existing: bool = True

    def __post_init__(self) -> None:
        _validate_scale(self.scale)
        if self.save_format not in ("gpkg", "graphml", "both"):
            raise ValueError(f"save_format must be 'gpkg', 'graphml', or 'both'")

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "GraphConfig":
        return cls(
            scale=args.scale,
            start_time=getattr(args, "start_time", "07:00:00"),
            end_time=getattr(args, "end_time", "10:00:00"),
            directed=not getattr(args, "undirected", False),
            save_format=getattr(args, "save_format", "both"),
            calendar_start=getattr(args, "calendar_start", None),
            calendar_end=getattr(args, "calendar_end", None),
            output_dir=getattr(args, "output_dir", "outputs/graphs"),
            skip_existing=not getattr(args, "force", False),
        )


@dataclass
class FeatureConfig:
    """Configuration for the feature-extraction stage."""

    scale: str
    graphs_dir: str = "outputs/graphs"
    diagrams_dir: str = "outputs/diagrams"
    features_dir: str = "outputs/features"
    time_window: tuple[str, str] = field(default_factory=lambda: ("07:00:00", "10:00:00"))
    skip_existing: bool = True

    def __post_init__(self) -> None:
        _validate_scale(self.scale)

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "FeatureConfig":
        return cls(
            scale=args.scale,
            graphs_dir=getattr(args, "graphs_dir", "outputs/graphs"),
            diagrams_dir=getattr(args, "diagrams_dir", "outputs/diagrams"),
            features_dir=getattr(args, "features_dir", "outputs/features"),
            skip_existing=not getattr(args, "force", False),
        )


@dataclass
class AnalysisConfig:
    """Configuration for the analysis / clustering stage."""

    scale: str
    features_dir: str = "outputs/features"
    geodata_dir: str = "geodata/"
    output_dir: str = "outputs/analysis"
    n_clusters: int = 5
    method: str = "kmeans"
    random_state: int = 42

    def __post_init__(self) -> None:
        _validate_scale(self.scale)

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "AnalysisConfig":
        return cls(
            scale=args.scale,
            features_dir=getattr(args, "features_dir", "outputs/features"),
            geodata_dir=getattr(args, "geodata_dir", "geodata/"),
            output_dir=getattr(args, "output_dir", "outputs/analysis"),
            n_clusters=getattr(args, "n_clusters", 5),
            method=getattr(args, "method", "kmeans"),
            random_state=getattr(args, "random_state", 42),
        )
