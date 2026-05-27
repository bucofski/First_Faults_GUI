"""Generate a multi-page PDF containing all diagram charts as images."""

from __future__ import annotations

from datetime import date, datetime
from io import BytesIO

import plotly.graph_objects as go
from business.core.fault_count_service import FaultCountService
from data.repositories.snapshot_repository import SnapshotRepository


class DiagramPdfService:
    """
    Build a multi-page landscape PDF containing all fault-analysis charts.

    This service mirrors the chart logic of
    :class:`~presentations.services.diagram_service_view.DiagramService` but
    produces :class:`plotly.graph_objects.Figure` objects instead of HTML
    strings.  Each figure is rasterised to a PNG via Kaleido
    (``fig.to_image``), and the PNGs are assembled into a ReportLab PDF
    (2 charts per A4 landscape page).

    The separation from ``DiagramService`` is intentional: the web UI needs
    lightweight HTML ``<div>`` strings, while PDF export needs pixel-perfect
    PNG renders that require the full ``go.Figure`` object.
    """

    _repo = SnapshotRepository()
    _fc_service = FaultCountService()

    # ------------------------------------------------------------------
    # Figure builders (return go.Figure, no HTML)
    # ------------------------------------------------------------------

    def _faults_per_hour_fig(self, reference_date: date | None = None) -> go.Figure:
        """
        Build a bar chart figure of fault counts per hour of day.

        Parameters
        ----------
        reference_date:
            The Monday of the week to display.  Falls back to the latest
            snapshot, then to yesterday's live data.

        Returns
        -------
        plotly.graph_objects.Figure
        """
        snapshot_date, rows = self._repo.get_latest_hour_snapshot(reference_date=reference_date)
        if rows:
            hours = [f"{h:02d}h" for h, _ in rows]
            values = [c for _, c in rows]
            title = f"Faults per hour — {snapshot_date}"
        else:
            counts = self._fc_service.get_yesterday_counts()
            hours = [f"{h.hour:02d}h" for h in counts.by_hour]
            values = [h.fault_count for h in counts.by_hour]
            title = f"Faults per hour — {counts.reference_date} (live)"

        fig = go.Figure()
        fig.add_bar(x=hours, y=values, text=[str(v) for v in values],
                    textposition='outside', marker_color='#1f77b4')
        fig.update_layout(title=title, yaxis_title='Fault count',
                          xaxis_title='Hour (Brussels)', height=400, width=700,
                          margin=dict(t=50, r=20, b=60, l=60))
        return fig

    def _faults_per_plc_fig(self, reference_date: date | None = None) -> go.Figure:
        """
        Build a pie chart figure of fault counts per PLC.

        Parameters
        ----------
        reference_date:
            The Monday of the week to display.  Falls back to the latest
            snapshot, then to yesterday's live data.

        Returns
        -------
        plotly.graph_objects.Figure
        """
        snapshot_date, rows = self._repo.get_latest_plc_snapshot(reference_date=reference_date)
        if rows:
            labels = [plc for plc, _ in rows]
            values = [count for _, count in rows]
            title = f"Faults per PLC — {snapshot_date}"
        else:
            counts = self._fc_service.get_yesterday_counts()
            labels = [p.plc_name for p in counts.by_plc]
            values = [p.fault_count for p in counts.by_plc]
            title = f"Faults per PLC — {counts.reference_date} (live)"

        fig = go.Figure(data=[go.Pie(labels=labels, values=values,
                                     textinfo='label+percent')])
        fig.update_layout(title=title, height=400, width=700,
                          margin=dict(t=50, r=20, b=40, l=20))
        return fig

    def _top_risers_fig(self, reference_date: date | None = None) -> go.Figure:
        """
        Build a horizontal bar chart figure of the top fault risers.

        Parameters
        ----------
        reference_date:
            The Monday of the week to display.  Falls back to the latest
            snapshot, then to live data.

        Returns
        -------
        plotly.graph_objects.Figure
        """
        snapshot_date, rows = self._repo.get_latest_top_risers(reference_date=reference_date)
        if rows:
            labels = [f"{r['mnemonic']} ({r['plc_name']})" for r in rows]
            delta_pcts = [r['delta_pct'] for r in rows]
            ref_label = str(snapshot_date)
        else:
            live = self._fc_service.get_top_risers()
            labels = [f"{r.mnemonic} ({r.plc_name})" for r in live]
            delta_pcts = [r.delta_pct for r in live]
            ref_label = "live"

        fig = go.Figure()
        fig.add_trace(go.Bar(x=delta_pcts, y=labels, orientation='h',
                             marker_color='#d62728'))
        fig.update_layout(title=f"Top risers — {ref_label}",
                          xaxis_title="% increase", yaxis=dict(autorange="reversed"),
                          height=400, width=700,
                          margin=dict(t=50, r=20, b=60, l=220))
        return fig

    def _mtbf_fig(self, reference_date: date | None = None) -> go.Figure:
        """
        Build a horizontal bar chart figure of MTBF per PLC.

        Parameters
        ----------
        reference_date:
            The Monday of the week to display.  Falls back to the latest
            snapshot, then to live data.

        Returns
        -------
        plotly.graph_objects.Figure
        """
        snapshot_date, rows = self._repo.get_latest_mtbf(reference_date=reference_date)
        if rows:
            plcs = [r[0] for r in rows]
            avg_hours = [r[1] for r in rows]
            title = f"MTBF per PLC — {snapshot_date}"
        else:
            live = self._fc_service.get_mtbf_per_plc()
            plcs = [r.plc_name for r in live]
            avg_hours = [r.avg_hours for r in live]
            title = "MTBF per PLC (live)"

        fig = go.Figure()
        fig.add_trace(go.Bar(x=avg_hours, y=plcs, orientation='h',
                             marker_color='#2ca02c'))
        fig.update_layout(title=title, xaxis_title="Avg hours between faults",
                          yaxis=dict(autorange="reversed"),
                          height=400, width=700,
                          margin=dict(t=50, r=20, b=60, l=120))
        return fig

    def _repeat_offenders_fig(self, reference_date: date | None = None) -> go.Figure:
        """
        Build a horizontal bar chart figure of the top repeat-offender faults.

        Parameters
        ----------
        reference_date:
            The Monday of the week to display.  Falls back to the latest
            snapshot, then to live data.

        Returns
        -------
        plotly.graph_objects.Figure
        """
        snapshot_date, rows = self._repo.get_latest_repeat_offenders(reference_date=reference_date)
        if rows:
            labels = [f"{m} ({p})" for m, p, _ in rows]
            counts = [c for _, _, c in rows]
            title = f"Repeat offenders — {snapshot_date}"
        else:
            live = self._fc_service.get_repeat_offenders()
            labels = [f"{r.mnemonic} ({r.plc_name})" for r in live]
            counts = [r.max_per_hour for r in live]
            title = "Repeat offenders (live)"

        fig = go.Figure()
        fig.add_trace(go.Bar(x=counts, y=labels, orientation='h',
                             marker_color='#ff7f0e'))
        fig.update_layout(title=title, xaxis_title="Max per hour",
                          yaxis=dict(autorange="reversed"),
                          height=400, width=700,
                          margin=dict(t=50, r=20, b=60, l=220))
        return fig

    def _long_term_trend_fig(self) -> go.Figure | None:
        """
        Build a multi-line chart figure of the top climbing faults over 52 weeks.

        Returns
        -------
        plotly.graph_objects.Figure or None
            ``None`` when the daily snapshot job has not produced any data yet.
        """
        climbers = self._repo.get_top_climbers(top_n=10)
        if not climbers:
            return None

        fig = go.Figure()
        for entry in climbers:
            label = f"{entry['mnemonic']} ({entry['plc_name']})"
            weeks = [str(w) for w, _ in entry['weeks']]
            counts = [c for _, c in entry['weeks']]
            fig.add_trace(go.Scatter(x=weeks, y=counts, mode='lines+markers',
                                     name=label))
        fig.update_layout(title="Top 10 climbing faults (weekly)",
                          xaxis_title="Week", yaxis_title="Faults per week",
                          legend=dict(orientation='h', y=-0.3),
                          height=450, width=700,
                          margin=dict(t=50, r=20, b=120, l=60))
        return fig

    # ------------------------------------------------------------------
    # PDF generation
    # ------------------------------------------------------------------

    def generate_pdf(self, reference_date: date | None = None) -> BytesIO:
        """
        Render all fault-analysis charts into a multi-page landscape PDF.

        Steps:

        1. Build each chart as a :class:`plotly.graph_objects.Figure`.
        2. Rasterise every figure to a PNG at 2× scale via Kaleido
           (``fig.to_image``).
        3. Assemble the PNGs into a ReportLab ``SimpleDocTemplate``
           (landscape A4, 2 charts per page).

        Parameters
        ----------
        reference_date:
            The Monday of the week whose data should appear in the PDF.
            ``None`` uses the latest available snapshot for each chart.

        Returns
        -------
        io.BytesIO
            An in-memory buffer positioned at byte 0, ready to be passed
            directly to :func:`flask.send_file`.

        Raises
        ------
        RuntimeError
            If ``reportlab`` is not installed.
        """
        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib import colors
            from reportlab.platypus import (
                SimpleDocTemplate, Image, Paragraph, Spacer, PageBreak,
            )
        except ImportError as e:
            raise RuntimeError("PDF export requires 'reportlab'.") from e

        figures = [
            self._faults_per_hour_fig(reference_date),
            self._faults_per_plc_fig(reference_date),
            self._top_risers_fig(reference_date),
            self._mtbf_fig(reference_date),
            self._repeat_offenders_fig(reference_date),
        ]
        trend_fig = self._long_term_trend_fig()
        if trend_fig is not None:
            figures.append(trend_fig)

        # Convert each figure to a PNG in memory
        chart_images: list[BytesIO] = []
        for fig in figures:
            img_bytes = fig.to_image(format="png", scale=2)
            buf = BytesIO(img_bytes)
            chart_images.append(buf)

        # Build PDF
        pdf_buf = BytesIO()
        page_w, page_h = landscape(A4)
        doc = SimpleDocTemplate(
            pdf_buf, pagesize=landscape(A4),
            title="Diagrams Export",
            leftMargin=30, rightMargin=30, topMargin=30, bottomMargin=30,
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle("DTitle", parent=styles["Title"],
                                     fontSize=16, spaceAfter=6)
        meta_style = ParagraphStyle("DMeta", parent=styles["Normal"],
                                    fontSize=9, textColor=colors.grey, spaceAfter=12)

        date_label = str(reference_date) if reference_date else "latest"
        elements = [
            Paragraph("Diagrams Export", title_style),
            Paragraph(f"Reference date: {date_label} &nbsp;|&nbsp; "
                      f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", meta_style),
            Spacer(1, 10),
        ]

        img_width = page_w - 60  # margins
        img_height = img_width * 0.55

        for i, img_buf in enumerate(chart_images):
            elements.append(Image(img_buf, width=img_width, height=img_height))
            elements.append(Spacer(1, 8))
            # Page break after every 2 charts (2 per page)
            if i % 2 == 1 and i < len(chart_images) - 1:
                elements.append(PageBreak())

        doc.build(elements)
        pdf_buf.seek(0)
        return pdf_buf
