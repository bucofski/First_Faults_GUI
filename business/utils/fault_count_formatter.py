"""Format FaultCountService output as Plotly chart divs for the frontend."""

import plotly.graph_objects as go
from plotly.offline import plot

from business.core.fault_count_service import DailyFaultCounts, FaultCountService


class FaultCountFormatter:
    """
    Turns DailyFaultCounts into Plotly chart divs ready for the frontend.

    Usage
    -----
    formatter = FaultCountFormatter()
    charts    = formatter.get_charts()   # pass to template

    Or with pre-fetched data:
    counts  = FaultCountService().get_yesterday_counts()
    charts  = FaultCountFormatter().get_charts(counts)
    """

    def __init__(self, service: FaultCountService | None = None):
        self._service = service or FaultCountService()

    def get_charts(self, counts: DailyFaultCounts | None = None) -> dict[str, str]:
        """
        Return dict with keys:
            reference_date  str
            by_hour         Plotly div (first chart — include_plotlyjs='cdn')
            by_plc          Plotly div (subsequent chart — include_plotlyjs=False)
        """
        data = counts or self._service.get_yesterday_counts()
        return {
            "reference_date": str(data.reference_date),
            "by_hour": self._by_hour_chart(data, first_chart=True),
            "by_plc":  self._by_plc_chart(data),
        }

    # ------------------------------------------------------------------

    @staticmethod
    def _by_hour_chart(data: DailyFaultCounts, first_chart: bool = True) -> str:
        hours  = [h.hour for h in data.by_hour]
        counts = [h.fault_count for h in data.by_hour]

        fig = go.Figure()
        fig.add_bar(
            x=hours,
            y=counts,
            text=[str(v) for v in counts],
            textposition="outside",
            marker_color="#1f77b4",
        )
        fig.update_layout(
            title=f"Faults per hour — {data.reference_date}",
            xaxis_title="Hour (Brussels)",
            yaxis_title="Fault count",
            xaxis=dict(tickmode="linear", tick0=0, dtick=1),
            margin=dict(t=50, r=20, b=60, l=60),
            height=400,
        )
        return plot(fig,
                    include_plotlyjs="cdn" if first_chart else False,
                    output_type="div")

    @staticmethod
    def _by_plc_chart(data: DailyFaultCounts) -> str:
        plcs   = [p.plc_name     for p in data.by_plc]
        counts = [p.fault_count  for p in data.by_plc]

        fig = go.Figure()
        fig.add_bar(
            x=plcs,
            y=counts,
            text=[str(v) for v in counts],
            textposition="outside",
            marker_color="#ff7f0e",
        )
        fig.update_layout(
            title=f"Faults per PLC — {data.reference_date}",
            xaxis_title="PLC",
            yaxis_title="Fault count",
            margin=dict(t=50, r=20, b=80, l=60),
            height=400,
        )
        return plot(fig, include_plotlyjs=False, output_type="div")