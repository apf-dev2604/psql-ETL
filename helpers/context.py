from dataclasses import dataclass
from typing import Any, Optional

from .reports import ReportPaths
from .models import BrandConfig


@dataclass(frozen=True)
class MigrationContext:
    """Small runtime container shared by the migration engine.

    This is intentionally simple: it groups the brand config, adapter, report
    paths, PHT-aware date-window bounds, and dry-run flag for one execution.
    Keeping these values in one object makes functions easier to pass around
    without changing the brand-specific mapping logic.
    """

    adapter: Any
    config: BrandConfig
    paths: ReportPaths
    from_dt: Optional[str]
    until_dt: Optional[str]
    dry_run: bool

    @property
    def brand_key(self) -> str:
        return str(getattr(self.config, "BRAND_KEY", "unknown"))

    @property
    def brand_name(self) -> str:
        return str(getattr(self.config, "BRAND", self.brand_key))
