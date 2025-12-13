# ... existing code ...
from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import List

from data.model.models import InterlockNode


class PdfGenerator:
    @staticmethod
    def generate_interlock(items: List[InterlockNode]) -> BytesIO:
        """
        Build a PDF in-memory and return a BytesIO positioned at 0 (ready for Flask send_file()).

        This function intentionally does NOT depend on Flask request context (flash/redirect).
        Callers (routes) should handle errors and user messaging.
        """
        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        except ImportError as e:
            raise RuntimeError(
                "PDF export requires the 'reportlab' dependency, but it is not available."
            ) from e

        # Flatten the tree into rows, keeping the same "table tree" grouping via indentation
        def flatten(nodes: List[InterlockNode], depth: int = 0) -> List[list]:
            rows: List[list] = []
            for n in nodes or []:
                indent = "&nbsp;" * (depth * 6)  # visual tree indent that works in ReportLab Paragraph
                caret = "▶ " if (getattr(n, "children", None) or []) else "• "
                level = getattr(n, "level", "") or ""
                msg = getattr(n, "interlock_message", "") or "N/A"

                # Conditions: keep them grouped per node (like UI), but in one cell to fit paper
                conds = getattr(n, "conditions", None) or []
                if conds:
                    cond_text = "<br/>".join(
                        (getattr(c, "message", None) or "").strip() or "N/A" for c in conds
                    )
                else:
                    cond_text = "-"

                rows.append([
                    f"{indent}{caret}<b>Level {level}</b> - {msg}",
                    getattr(n, "bsid", None) or "N/A",
                    getattr(n, "plc", None) or "N/A",
                    getattr(n, "bit_index", None) or "N/A",
                    getattr(n, "direction", None) or "N/A",
                    str(getattr(n, "timestamp", None) or "N/A"),
                    getattr(n, "status", None) or "N/A",
                    cond_text,
                ])

                children = getattr(n, "children", None) or []
                rows.extend(flatten(children, depth + 1))
            return rows

        rows = flatten(items)

        buf = BytesIO()

        # Margins: keep real printable margins so nothing gets cut off
        doc = SimpleDocTemplate(
            buf,
            pagesize=landscape(A4),
            title="Interlock Tree Export",
            leftMargin=24,
            rightMargin=24,
            topMargin=24,
            bottomMargin=24,
        )

        styles = getSampleStyleSheet()

        # Smaller fonts so everything fits nicely on paper
        title_style = ParagraphStyle(
            "ExportTitle",
            parent=styles["Title"],
            fontName="Helvetica-Bold",
            fontSize=14,
            leading=16,
            spaceAfter=6,
        )
        meta_style = ParagraphStyle(
            "ExportMeta",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=8,
            leading=10,
            textColor=colors.grey,
            spaceAfter=10,
        )
        cell_style = ParagraphStyle(
            "Cell",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=7,
            leading=8,
        )

        title = "Interlock Tree Export"
        subtitle = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

        elements = [
            Paragraph(title, title_style),
            Paragraph(subtitle, meta_style),
            Spacer(1, 6),
        ]

        header = ["Interlock Message", "BSID", "PLC", "BITINDEX", "Direction", "Timestamp", "Status", "Conditions"]
        data = [header]

        if rows:
            for r in rows:
                data.append([
                    Paragraph(str(r[0]), cell_style),
                    Paragraph(str(r[1]), cell_style),
                    Paragraph(str(r[2]), cell_style),
                    Paragraph(str(r[3]), cell_style),
                    Paragraph(str(r[4]), cell_style),
                    Paragraph(str(r[5]), cell_style),
                    Paragraph(str(r[6]), cell_style),
                ])
        else:
            data.append([
                Paragraph("No data", cell_style),
                Paragraph("-", cell_style),
                Paragraph("-", cell_style),
                Paragraph("-", cell_style),
                Paragraph("-", cell_style),
                Paragraph("-", cell_style),
                Paragraph("-", cell_style),
            ])

        # Widths tuned for landscape A4 + margins; keep message/conditions wider
        table = Table(
            data,
            repeatRows=1,
            colWidths=[260, 48, 70, 60, 110, 55, 160],
        )

        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 8),
            ("ALIGN", (1, 0), (-1, -1), "LEFT"),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),

            ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 1), (-1, -1), 7),

            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]))

        elements.append(table)

        doc.build(elements)
        buf.seek(0)
        return buf
