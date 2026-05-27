from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import List

from data.model.models import InterlockNode


class PdfGenerator:
    """
    Generate PDF exports for the Interlock Table Tree.

    This class is intentionally stateless and free of Flask context so it
    can be called from any route or background job without side effects.
    Callers (routes) are responsible for handling validation errors and
    flashing user messages before invoking this generator.
    """

    @staticmethod
    def generate_interlock(items: List[InterlockNode]) -> BytesIO:
        """
        Build an Interlock Tree export as an in-memory landscape A4 PDF.

        The tree is flattened recursively into a tabular layout with
        visual indentation (non-breaking spaces) to preserve the
        parent-child hierarchy on paper.  Each row contains:

        - **Interlock Message** — tree-indented label with level and message.
        - **BSID** — the interlock block sequence ID.
        - **PLC** — the PLC this interlock belongs to.
        - **Direction** — e.g. ``"IN"`` / ``"OUT"``.
        - **Timestamp** — when the fault was recorded.
        - **Status** — current interlock status.
        - **Conditions** — all bit conditions grouped per node, formatted
          as ``[Bit N] message`` lines.

        Column widths are tuned for landscape A4 with 24 pt margins so
        nothing is cut off on standard printers.

        Parameters
        ----------
        items:
            The list of root :class:`~data.model.models.InterlockNode`
            objects returned by
            :meth:`~business.services.analyzer.InterlockService.analyze_interlock`.
            May be empty, in which case a single "No data" row is written.

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
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        except ImportError as e:
            raise RuntimeError(
                "PDF export requires the 'reportlab' dependency, but it is not available."
            ) from e

        def flatten(nodes: List[InterlockNode], depth: int = 0) -> List[list]:
            """
            Recursively flatten the interlock tree into a list of table rows.

            Each level of depth adds 6 non-breaking spaces of indentation to
            the first column so the hierarchy remains visible in the PDF.

            Parameters
            ----------
            nodes:
                The list of :class:`~data.model.models.InterlockNode` objects
                at the current depth level.
            depth:
                Current recursion depth (0 = root).

            Returns
            -------
            list[list]
                A flat list of 7-element rows ready for a ReportLab ``Table``.
            """
            rows: List[list] = []
            for n in nodes or []:
                indent = "&nbsp;" * (depth * 6)
                caret = "▶ " if (getattr(n, "children", None) or []) else "• "
                level = getattr(n, "level", "") or ""
                msg = getattr(n, "interlock_message", "") or "N/A"

                conds = getattr(n, "conditions", None) or []
                if conds:
                    cond_text = "<br/>".join(
                        f"[Bit {getattr(c, 'bit_index', 'N/A')}] {(getattr(c, 'message', None) or '').strip() or 'N/A'}"
                        for c in conds
                    )
                else:
                    cond_text = "-"

                rows.append([
                    f"{indent}{caret}<b>Level {level}</b> - {msg}",
                    getattr(n, "bsid", None) or "N/A",
                    getattr(n, "plc", None) or "N/A",
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

        header = ["Interlock Message", "BSID", "PLC", "Direction", "Timestamp", "Status", "Conditions"]
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
