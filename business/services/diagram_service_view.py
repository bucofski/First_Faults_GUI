import plotly.graph_objects as go
from plotly.offline import plot

from business.services.fault_count_service import FaultCountService
from data.repositories.snapshot_repository import SnapshotRepository


class DiagramService:
    @staticmethod
    def grouped_bar_chart_html():
        repo = SnapshotRepository()
        snapshot_date, rows = repo.get_latest_hour_snapshot()

        if rows:
            hours  = [f"{h:02d}h" for h, _ in rows]
            values = [c            for _, c in rows]
            title  = f"Faults per hour — {snapshot_date}"
        else:
            counts = FaultCountService().get_yesterday_counts()
            hours  = [f"{h.hour:02d}h" for h in counts.by_hour]
            values = [h.fault_count    for h in counts.by_hour]
            title  = f"Faults per hour — {counts.reference_date} (live)"

        fig = go.Figure()
        fig.add_bar(
            x=hours,
            y=values,
            text=[str(v) for v in values],
            textposition='outside',
            marker_color='#1f77b4',
        )
        fig.update_layout(
            margin=dict(t=50, r=20, b=60, l=60),
            yaxis_title='Fault count',
            xaxis_title='Hour (Brussels)',
            uniformtext=dict(mode='hide', minsize=10),
            height=450,
            title=title,
        )
        return plot(fig, include_plotlyjs='cdn', output_type='div')

    @staticmethod
    def pie_chart_html():
        repo = SnapshotRepository()
        snapshot_date, rows = repo.get_latest_plc_snapshot()

        if rows:
            labels = [plc   for plc, _ in rows]
            values = [count for _, count in rows]
            title  = f"Faults per PLC — {snapshot_date}"
        else:
            counts = FaultCountService().get_yesterday_counts()
            labels = [p.plc_name    for p in counts.by_plc]
            values = [p.fault_count for p in counts.by_plc]
            title  = f"Faults per PLC — {counts.reference_date} (live)"

        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            textinfo='label+percent',
            insidetextorientation='radial',
        )])
        fig.update_layout(
            margin=dict(t=40, r=20, b=40, l=20),
            height=450,
            title=title,
        )
        return plot(fig, include_plotlyjs=False, output_type='div')

    @staticmethod
    def grouped_bar_chart_2_html(
        recent_days:   int = 7,
        baseline_days: int = 30,
        top_n:         int = 10,
    ) -> str:
        repo = SnapshotRepository()
        snapshot_date, rows = repo.get_latest_top_risers(recent_days, baseline_days, top_n)

        if rows:
            labels     = [f"{r['mnemonic']} ({r['plc_name']})" for r in rows]
            delta_pcts = [r['delta_pct']                        for r in rows]
            custom     = [[r['recent_count'], r['baseline_count']] for r in rows]
            ref_label  = str(snapshot_date)
        else:
            live = FaultCountService().get_top_risers(
                recent_days=recent_days, baseline_days=baseline_days, top_n=top_n,
            )
            labels     = [f"{r.mnemonic} ({r.plc_name})" for r in live]
            delta_pcts = [r.delta_pct                     for r in live]
            custom     = [[r.recent_count, r.baseline_count] for r in live]
            ref_label  = "live"

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=delta_pcts,
            y=labels,
            orientation='h',
            customdata=custom,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Change: +%{x:.1f}%<br>"
                "Recent count: %{customdata[0]}<br>"
                "Baseline count: %{customdata[1]}"
                "<extra></extra>"
            ),
            marker_color='#d62728',
        ))
        fig.update_layout(
            title=f"Top risers — {ref_label}, recent {recent_days}d vs baseline {baseline_days}d",
            xaxis_title="% increase (daily rate)",
            yaxis=dict(autorange="reversed"),
            margin=dict(t=50, r=20, b=60, l=220),
            height=max(300, top_n * 35),
        )
        return plot(fig, include_plotlyjs=False, output_type='div')