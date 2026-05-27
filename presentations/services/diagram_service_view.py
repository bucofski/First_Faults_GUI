from datetime import date

import plotly.graph_objects as go
from plotly.offline import plot

from business.core.fault_count_service import FaultCountService
from data.repositories.snapshot_repository import SnapshotRepository


class DiagramService:
    """
    Build interactive Plotly charts as HTML ``<div>`` strings for the web UI.

    Each method follows a **snapshot-first** strategy:

    1. Try to load pre-computed weekly snapshot data from
       :class:`~data.repositories.snapshot_repository.SnapshotRepository`.
    2. Fall back to a live query via
       :class:`~business.core.fault_count_service.FaultCountService` when
       no snapshot exists for the requested date.

    All methods return a self-contained HTML string (``output_type='div'``)
    that can be embedded directly in a Jinja2 template.  Plotly.js is
    included via CDN only in :meth:`grouped_bar_chart_html`; all other
    methods pass ``include_plotlyjs=False`` because the CDN script is
    already on the page.
    """

    _repo = SnapshotRepository()
    _fc_service = FaultCountService()

    def grouped_bar_chart_html(self, reference_date: date | None = None) -> str:
        """
        Build a bar chart of fault counts grouped by hour of day.

        Uses the hour-level snapshot for ``reference_date``.  The x-axis
        shows hours formatted as ``"00h"``–``"23h"`` (Brussels time); the
        y-axis shows the fault count.  Count labels are rendered above each
        bar.

        This method includes the Plotly.js CDN script tag (``include_plotlyjs='cdn'``).
        It should be the *first* chart added to a page; all other charts on
        the same page should use :attr:`include_plotlyjs=False`.

        Parameters
        ----------
        reference_date:
            The Monday of the week to display.  ``None`` falls back to the
            most recent available snapshot, then to yesterday's live data.

        Returns
        -------
        str
            An HTML ``<div>`` containing the interactive bar chart.
        """
        snapshot_date, rows = self._repo.get_latest_hour_snapshot(reference_date=reference_date)
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

    def pie_chart_html(self, reference_date: date | None = None) -> str:
        """
        Build a pie chart of fault counts per PLC.

        Each slice represents one PLC; the label shows both the PLC name
        and its percentage share of total faults for the reference week.

        Parameters
        ----------
        reference_date:
            The Monday of the week to display.  ``None`` falls back to the
            most recent available snapshot, then to yesterday's live data.

        Returns
        -------
        str
            An HTML ``<div>`` containing the interactive pie chart.
        """
        snapshot_date, rows = self._repo.get_latest_plc_snapshot(reference_date=reference_date)
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

    def pie_chart_window_html(self, days: int = 7) -> str:
        """
        Build a pie chart of fault counts per PLC over a rolling time window.

        Unlike :meth:`pie_chart_html`, this method always uses live data for
        the last ``days`` days rather than a weekly snapshot.

        Parameters
        ----------
        days:
            Number of days to look back from today.  Defaults to 7.

        Returns
        -------
        str
            An HTML ``<div>`` containing the interactive pie chart, or a
            plain ``<p>`` tag when no data is available.
        """
        start_date, counts = self._fc_service.get_plc_counts_window(days=days)
        if not counts:
            return "<p>No fault data available for the past week.</p>"

        labels = [p.plc_name    for p in counts]
        values = [p.fault_count for p in counts]

        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            textinfo='label+percent',
            insidetextorientation='radial',
        )])
        fig.update_layout(
            margin=dict(t=40, r=20, b=40, l=20),
            height=450,
            title=f"Faults per PLC — last {days}d (from {start_date})",
        )
        return plot(fig, include_plotlyjs=False, output_type='div')

    def grouped_bar_chart_2_html(
        self,
        recent_days:   int = 7,
        baseline_days: int = 30,
        top_n:         int = 10,
        reference_date: date | None = None,
    ) -> str:
        """
        Build a horizontal bar chart of the top-N fault risers.

        A *riser* is a fault whose daily occurrence rate in the last
        ``recent_days`` is significantly higher than its rate over the
        ``baseline_days`` window.  The x-axis shows the percentage increase
        in daily rate; hover tooltips reveal the raw recent and baseline
        counts.

        Parameters
        ----------
        recent_days:
            Length of the short comparison window in days.  Default: 7.
        baseline_days:
            Length of the baseline window in days.  Default: 30.
        top_n:
            Maximum number of risers to display.  Default: 10.
        reference_date:
            The Monday of the week to display.  ``None`` uses the latest
            available snapshot, then falls back to live data.

        Returns
        -------
        str
            An HTML ``<div>`` with the horizontal bar chart, or a plain
            ``<p>`` tag when no data is available.
        """
        snapshot_date, rows = self._repo.get_latest_top_risers(recent_days, baseline_days, top_n, reference_date=reference_date)

        if rows:
            labels     = [f"{r['mnemonic']} ({r['plc_name']})" for r in rows]
            delta_pcts = [r['delta_pct']                        for r in rows]
            custom     = [[r['recent_count'], r['baseline_count']] for r in rows]
            ref_label  = str(snapshot_date)
        else:
            live = self._fc_service.get_top_risers(
                recent_days=recent_days, baseline_days=baseline_days, top_n=top_n,
            )
            if not live:
                return "<p>No top risers data available yet.</p>"
            labels     = [f"{r.mnemonic} ({r.plc_name})" for r in live]
            delta_pcts = [r.delta_pct                    for r in live]
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

    def repeat_offenders_html(self, days: int = 30, top_n: int = 10, reference_date: date | None = None) -> str:
        """
        Build a horizontal bar chart of the top repeat-offender faults.

        A *repeat offender* is a fault that appeared many times within a
        single hour, indicating a rapidly cycling alarm.  The x-axis shows
        the maximum number of times the fault appeared in any one hour within
        the ``days`` window.

        Parameters
        ----------
        days:
            Rolling window in days used to determine repeat offenders.
            Default: 30.
        top_n:
            Maximum number of faults to display.  Default: 10.
        reference_date:
            The Monday of the week to display.  ``None`` uses the latest
            available snapshot.

        Returns
        -------
        str
            An HTML ``<div>`` with the chart, or a plain ``<p>`` tag when
            no snapshot is available.
        """
        snapshot_date, rows = self._repo.get_latest_repeat_offenders(days_window=days, top_n=top_n, reference_date=reference_date)

        if not rows:
            return "<p>No repeat offender snapshot available for this week yet.</p>"

        labels = [f"{mnemonic} ({plc})" for mnemonic, plc, _ in rows]
        counts = [c for _, _, c in rows]
        title  = f"Repeat offenders — {snapshot_date} (last {days}d)"

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=counts,
            y=labels,
            orientation='h',
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Max occurrences in 1h: %{x}"
                "<extra></extra>"
            ),
            marker_color='#ff7f0e',
        ))
        fig.update_layout(
            title=title,
            xaxis_title="Max times fault appeared in a single hour",
            yaxis=dict(autorange="reversed"),
            margin=dict(t=50, r=20, b=60, l=220),
            height=max(300, top_n * 35),
        )
        return plot(fig, include_plotlyjs=False, output_type='div')

    def long_term_trend_html(self, top_n: int = 10) -> str:
        """
        Build a multi-line chart of the top climbing faults over 52 weeks.

        Each line represents one fault (identified by mnemonic + PLC).  The
        x-axis shows ISO week labels; the y-axis shows the weekly fault count.
        The hover tooltip includes the average weekly climb rate.

        Parameters
        ----------
        top_n:
            Number of top-climbing faults to include.  Default: 10.

        Returns
        -------
        str
            An HTML ``<div>`` with the line chart, or a plain ``<p>`` tag
            when the daily snapshot job has not run yet.
        """
        climbers = self._repo.get_top_climbers(top_n=top_n)

        if not climbers:
            return "<p>No trend data yet — run the daily snapshot job first.</p>"

        fig = go.Figure()
        for entry in climbers:
            label  = f"{entry['mnemonic']} ({entry['plc_name']})"
            weeks  = [str(w) for w, _ in entry['weeks']]
            counts = [c       for _, c in entry['weeks']]
            fig.add_trace(go.Scatter(
                x=weeks,
                y=counts,
                mode='lines+markers',
                name=label,
                hovertemplate=(
                    f"<b>{label}</b><br>"
                    "Week: %{x}<br>"
                    "Faults: %{y}<br>"
                    f"Climb: +{entry['climb']:.1f}/week"
                    "<extra></extra>"
                ),
            ))

        fig.update_layout(
            title=f"Top {top_n} climbing faults (weekly, last 52 weeks)",
            xaxis_title="Week",
            yaxis_title="Faults per week",
            legend=dict(orientation='h', y=-0.3),
            margin=dict(t=50, r=20, b=120, l=60),
            height=500,
        )
        return plot(fig, include_plotlyjs=False, output_type='div')

    def mtbf_html(self, days: int = 30, reference_date: date | None = None) -> str:
        """
        Build a horizontal bar chart of Mean Time Between Failures per PLC.

        MTBF is expressed in hours.  A longer bar means the PLC is more
        stable (fewer faults per hour).  The hover tooltip also shows the
        raw fault count used in the calculation.

        Parameters
        ----------
        days:
            Rolling window in days used to compute MTBF.  Default: 30.
        reference_date:
            The Monday of the week to display.  ``None`` uses the latest
            available snapshot.

        Returns
        -------
        str
            An HTML ``<div>`` with the MTBF chart, or a plain ``<p>`` tag
            when no snapshot is available.
        """
        snapshot_date, rows = self._repo.get_latest_mtbf(days_window=days, reference_date=reference_date)

        if not rows:
            return "<p>No MTBF snapshot available for this week yet.</p>"

        plcs         = [r[0] for r in rows]
        avg_hours    = [r[1] for r in rows]
        fault_counts = [r[2] for r in rows]
        title        = f"MTBF per PLC — {snapshot_date} (last {days}d)"

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=avg_hours,
            y=plcs,
            orientation='h',
            customdata=fault_counts,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Avg between faults: %{x:.1f}h<br>"
                "Fault count: %{customdata}"
                "<extra></extra>"
            ),
            marker_color='#2ca02c',
        ))
        fig.update_layout(
            title=title,
            xaxis_title="Avg hours between faults (higher = more stable)",
            yaxis=dict(autorange="reversed"),
            margin=dict(t=50, r=20, b=60, l=120),
            height=max(300, len(plcs) * 35),
        )
        return plot(fig, include_plotlyjs=False, output_type='div')

    def heatmap_html(self, plc_name: str, days: int = 30) -> str:
        """
        Build a fault heatmap for a single PLC.

        The heatmap shows fault intensity across all hours of the day (x-axis)
        and all dates in the last ``days`` days (y-axis).  The colour scale
        goes from yellow (few faults) to red (many faults).

        Parameters
        ----------
        plc_name:
            The exact PLC name as stored in the database.
        days:
            Number of days to include.  Default: 30.

        Returns
        -------
        str
            An HTML ``<div>`` containing the interactive heatmap.
        """
        data = self._fc_service.get_heatmap_data(plc_name=plc_name, days=days)

        fig = go.Figure(data=go.Heatmap(
            z=data.counts,
            x=[f"{h:02d}h" for h in range(24)],
            y=data.date_labels,
            colorscale="YlOrRd",
            hovertemplate="Date: %{y}<br>Hour: %{x}<br>Faults: %{z}<extra></extra>",
        ))
        fig.update_layout(
            title=f"Fault heatmap — {plc_name} (last {days} days)",
            xaxis_title="Hour (Brussels)",
            yaxis_title="Date",
            margin=dict(t=50, r=20, b=60, l=100),
            height=max(300, days * 18),
        )
        return plot(fig, include_plotlyjs=False, output_type='div')
