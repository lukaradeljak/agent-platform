"""
Onboarding Clients Agent.

Polls HubSpot every 5 minutes for deals in "Cierre ganado".
For each new deal: creates Supabase Auth user, sends welcome email, marks deal active.
"""

import sys
import os
from typing import Any

# Add tools directory to path so the tools' relative imports work
_TOOLS_DIR = os.path.join(os.path.dirname(__file__), "tools")
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from agents.base_agent import BaseAgent


class OnboardingClientsAgent(BaseAgent):
    name = "onboarding_clients"

    def run(self) -> dict[str, Any]:
        from poll_hubspot import main

        stats = main() or {}
        return {
            "deals_found":     stats.get("deals_found", 0),
            "deals_processed": stats.get("deals_processed", 0),
            "emails_sent":     stats.get("emails_sent", 0),
            "errors_count":    stats.get("errors_count", 0),
        }
