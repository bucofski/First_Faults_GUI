"""Generate a multi-page PDF containing all diagram charts as images."""

from __future__ import annotations

from datetime import date, datetime
from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from business.core.fault_count_service import FaultCountService
from data.repositories.snapshot_repository import SnapshotRepository


class DiagramPdfService:
    _repo = SnapshotRepository()
    _fc_service = FaultCountService()

    # ------------------------------------------------------------------
    # Figure builders — return (matplotlib.figure.Figure, BytesIO of PNG)
    # ------------------------------------------------------------------

    def _fig_to_png(self, fig) -> BytesIO:
        buf = BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
        plt.close(fig)
        buf.seek(0)
        return buf

    def _faults_per_hour_png(self, reference_date: date | None = None) -> BytesIO:
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

        fig, ax = plt.subplots(figsize=(10, 4))
        ax.bar(hours, values, color="#1f77b4")
        ax.set_title(title)
        ax.set_xlabel("Hour (Brussels)")
        ax.set_ylabel("Fault count")
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()
        return self._fig_to_png(fig)

    def _faults_per_plc_png(self, reference_date: date | None = None) -> BytesIO:
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

        fig, ax = plt.subplots(figsize=(7, 5))
        ax.pie(values, labels=labels, autopct="%1.1f%%")
        ax.set_title(title)
        fig.tight_layout()
        return self._fig_to_png(fig)

    def _top_risers_png(self, reference_date: date | None = None) -> BytesIO:
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

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(labels[::-1], delta_pcts[::-1], color="#d62728")
        ax.set_title(f"Top risers — {ref_label}")
        ax.set_xlabel("% increase")
        fig.tight_layout()
        return self._fig_to_png(fig)

    def _mtbf_png(self, reference_date: date | None = None) -> BytesIO:
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

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(plcs[::-1], avg_hours[::-1], color="#2ca02c")
        ax.set_title(title)
        ax.set_xlabel("Avg hours between faults")
        fig.tight_layout()
        return self._fig_to_png(fig)

    def _repeat_offenders_png(self, reference_date: date | None = None) -> BytesIO:
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

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.barh(labels[::-1], counts[::-1], color="#ff7f0e")
        ax.set_title(title)
        ax.set_xlabel("Max per hour")
        fig.tight_layout()
        return self._fig_to_png(fig)

    def _long_term_trend_png(self) -> BytesIO | None:
        climbers = self._repo.get_top_climbers(top_n=10)
        if not climbers:
            return None

        fig, ax = plt.subplots(figsize=(10, 5))
        for entry in climbers:
            label = f"{entry['mnemonic']} ({entry['plc_name']})"
            weeks = [str(w) for w, _ in entry['weeks']]
            counts = [c for _, c in entry['weeks']]
            ax.plot(weeks, counts, marker="o", label=label)
        ax.set_title("Top 10 climbing faults (weekly)")
        ax.set_xlabel("Week")
        ax.set_ylabel("Faults per week")
        ax.tick_params(axis="x", rotation=45)
        ax.legend(loc="upper left", fontsize=7, ncol=2)
        fig.tight_layout()
        return self._fig_to_png(fig)

    # ------------------------------------------------------------------
    # PDF generation
    # ------------------------------------------------------------------

    def generate_pdf(self, reference_date: date | None = None) -> BytesIO:
        """Render all diagrams to a multi-page landscape PDF."""
        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib import colors
            from reportlab.platypus import (
                SimpleDocTemplate, Image, Paragraph, Spacer, PageBreak,
            )
        except ImportError as e:
            raise RuntimeError("PDF export requires 'reportlab'.") from e

        chart_images: list[BytesIO] = [
            self._faults_per_hour_png(reference_date),
            self._faults_per_plc_png(reference_date),
            self._top_risers_png(reference_date),
            self._mtbf_png(reference_date),
            self._repeat_offenders_png(reference_date),
        ]
        trend_png = self._long_term_trend_png()
        if trend_png is not None:
            chart_images.append(trend_png)

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

        img_width = page_w - 60
        img_height = img_width * 0.55

        for i, img_buf in enumerate(chart_images):
            elements.append(Image(img_buf, width=img_width, height=img_height))
            elements.append(Spacer(1, 8))
            if i % 2 == 1 and i < len(chart_images) - 1:
                elements.append(PageBreak())

        doc.build(elements)
        pdf_buf.seek(0)
        return pdf_buf
