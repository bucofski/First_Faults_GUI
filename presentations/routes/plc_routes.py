import datetime as dt

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    url_for, get_flashed_messages,
    send_file,
)
from business.services.analyzer import InterlockService
from presentations.services.diagram_service_view import DiagramService
from business.core.fault_count_service import FaultCountService

from presentations.services.pdf_generator import PdfGenerator
from presentations.services.diagram_pdf_service import DiagramPdfService

bp = Blueprint("plc", __name__, url_prefix="/plc")

# Module-level singletons — initialised once at import time so each request
# reuses the same service instance without rebuilding internal state.
service_interlock = InterlockService()
_diagram_service = DiagramService()
_diagram_pdf_service = DiagramPdfService()
_fault_count_service = FaultCountService()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _parse_iso_datetime(value: str | None, field_name: str) -> dt.datetime | None:
    """
    Parse an ISO 8601 datetime string into a :class:`datetime.datetime`.

    Accepts any format supported by :meth:`datetime.fromisoformat`, e.g.
    ``"2024-05-13T08:30"`` or ``"2024-05-13"``.

    Parameters
    ----------
    value:
        The raw string from the request.  ``None`` or an empty string
        returns ``None`` without raising.
    field_name:
        Human-readable name used in the error message, e.g.
        ``"filter_timestamp_start"``.

    Returns
    -------
    datetime.datetime or None

    Raises
    ------
    ValueError
        If ``value`` is non-empty but cannot be parsed as ISO 8601.
    """
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        # Accepts 'YYYY-MM-DDTHH:MM' and many ISO variants.
        return dt.datetime.fromisoformat(value)
    except ValueError as e:
        raise ValueError(f"Invalid {field_name} datetime: {value!r}. Expected format like YYYY-MM-DDTHH:MM.") from e


def _parse_optional_int(value: str, *, field_label: str, error_message: str) -> int | None:
    """
    Parse an optional integer from a form string value.

    An empty or whitespace-only string is treated as "not provided" and
    returns ``None``.  If the value is present but not a valid integer,
    a flash error is added and ``None`` is returned so the caller can
    continue rendering the page with a user-visible message.

    Parameters
    ----------
    value:
        Raw string from ``request.form``.
    field_label:
        Name used internally for identification (not shown to the user).
    error_message:
        The flash message shown to the user when the value is invalid.

    Returns
    -------
    int or None
    """
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        flash(error_message, "error")
        return None


def _read_table_tree_form_params() -> dict:
    """
    Extract and normalise all Table Tree filter fields from ``request.form``.

    Returns a flat dict with the raw string values so that downstream
    helpers can validate and convert them independently.

    Returns
    -------
    dict
        Keys: ``target_bsid_str``, ``top_n_str``,
        ``filter_timestamp_start_raw``, ``filter_timestamp_end_raw``,
        ``filter_condition_message``, ``filter_plc``.
    """
    return {
        "target_bsid_str": request.form.get("target_bsid", ""),
        "top_n_str": request.form.get("top_n", ""),
        "filter_timestamp_start_raw": request.form.get("filter_timestamp_start", "").strip() or None,
        "filter_timestamp_end_raw": request.form.get("filter_timestamp_end", "").strip() or None,
        "filter_condition_message": request.form.get("filter_condition_message", "").strip() or None,
        "filter_plc": request.form.get("filter_plc", "").strip() or None,
    }


def _parse_table_tree_filters_or_redirect(*, redirect_endpoint: str):
    """
    Parse and validate Table Tree filters from the current POST request.

    Reads form fields, validates integer and datetime inputs, and returns
    two dicts on success:

    - ``analyze_kwargs`` — ready to be unpacked into
      :meth:`InterlockService.analyze_interlock`.
    - ``params`` — a subset of the same values encoded as URL query
      parameters for the POST → Redirect → GET pattern.

    On validation failure a flash error is added and a Flask redirect
    response is returned directly so the caller can ``return`` it
    immediately.

    Parameters
    ----------
    redirect_endpoint:
        The Flask endpoint name (e.g. ``"plc.table_tree"``) to redirect
        to on validation error.

    Returns
    -------
    tuple[dict, dict] | flask.Response
        A ``(analyze_kwargs, params)`` tuple on success, or a redirect
        response on failure.
    """
    form = _read_table_tree_form_params()

    target_bsid = _parse_optional_int(
        form["target_bsid_str"],
        field_label="target_bsid",
        error_message="Target BSID must be a valid integer.",
    )
    top_n = _parse_optional_int(
        form["top_n_str"],
        field_label="top_n",
        error_message="Top N must be a valid integer.",
    )

    params: dict = {}
    if target_bsid is not None:
        params["target_bsid"] = target_bsid
    if top_n is not None:
        params["top_n"] = top_n

    try:
        filter_timestamp_start = _parse_iso_datetime(form["filter_timestamp_start_raw"], "filter_timestamp_start")
        filter_timestamp_end = _parse_iso_datetime(form["filter_timestamp_end_raw"], "filter_timestamp_end")
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for(redirect_endpoint, **params))
    if form["filter_timestamp_start_raw"]:
        params["filter_timestamp_start"] = form["filter_timestamp_start_raw"]
    if form["filter_timestamp_end_raw"]:
        params["filter_timestamp_end"] = form["filter_timestamp_end_raw"]
    if form["filter_condition_message"]:
        params["filter_condition_message"] = form["filter_condition_message"]
    if form["filter_plc"]:
        params["filter_plc"] = form["filter_plc"]

    analyze_kwargs = {
        "target_bsid": target_bsid,
        "top_n": top_n,
        "filter_timestamp_start": filter_timestamp_start,
        "filter_timestamp_end": filter_timestamp_end,
        "filter_condition_message": form["filter_condition_message"],
        "filter_plc": form["filter_plc"],
    }
    return analyze_kwargs, params


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@bp.route("/")
def home():
    """
    Render the landing page with a quick summary of last week's faults.

    Calculates the Monday of the previous week as the ``reference_date``
    and passes two pre-rendered Plotly HTML charts to the template:

    - **Top risers** — faults that increased most compared to the baseline.
    - **PLC pie** — fault distribution across all PLCs.
    """
    today = dt.date.today()
    selected_date = today - dt.timedelta(days=today.weekday() + 7)
    top_risers_html = _diagram_service.grouped_bar_chart_2_html(reference_date=selected_date)
    plc_pie_html    = _diagram_service.pie_chart_html(reference_date=selected_date)
    return render_template(
        "home.html",
        title="Home",
        top_risers_html=top_risers_html,
        plc_pie_html=plc_pie_html,
    )


@bp.route("/table")
def table():
    """Render the basic table page (no data loaded on initial visit)."""
    return render_template("table.html", title="Table", data=None)


@bp.route("/about")
def about():
    """Render the static About page."""
    return render_template("about.html", title="About")


@bp.route("/contact")
def contact():
    """Render the static Contact page."""
    return render_template("contact.html", title="Contact")


def _first_monday_of_month_week(year: int, month: int, week: int) -> dt.date:
    """
    Return the Monday that starts the *n*-th week of a given month.

    Week numbering starts at 1 and counts from the first Monday that
    falls in (or after) the 1st of the month.  If ``week`` points past
    the last Monday in the month, the last valid Monday is returned
    instead (clamping behaviour).

    Parameters
    ----------
    year:
        The calendar year, e.g. ``2024``.
    month:
        The calendar month (1–12).
    week:
        The 1-based week index within the month.

    Returns
    -------
    datetime.date
        The Monday for the requested week.

    Examples
    --------
    >>> _first_monday_of_month_week(2024, 5, 1)
    datetime.date(2024, 5, 6)   # first Monday of May 2024
    >>> _first_monday_of_month_week(2024, 5, 2)
    datetime.date(2024, 5, 13)
    """
    # Find the first Monday on or after the 1st of the month
    first_day = dt.date(year, month, 1)
    days_until_monday = (7 - first_day.weekday()) % 7  # 0 if already Monday
    first_monday = first_day + dt.timedelta(days=days_until_monday)
    target = first_monday + dt.timedelta(weeks=week - 1)
    # Clamp to the same month
    last_day = (first_day.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
    if target > last_day:
        target = first_monday + dt.timedelta(weeks=max(0, (last_day - first_monday).days // 7))
    return target


@bp.route("/diagrams")
def diagrams():
    """
    Render the full fault-analysis dashboard.

    Accepts the following optional query parameters:

    - ``month`` (int, 1–12) — defaults to the current month.
    - ``week`` (int, 1–5) — week within the month; defaults to 1.
    - ``plc`` (str) — if provided, also renders a fault heatmap for that PLC.

    The ``reference_date`` is computed as the Monday that starts the
    selected month/week.  All charts use snapshot data for that date,
    with a live-query fallback if no snapshot exists yet.

    Charts rendered:

    - Faults per hour (bar)
    - Top risers (horizontal bar, % increase)
    - Faults per PLC (pie)
    - MTBF per PLC (horizontal bar)
    - Top 10 climbing faults over 52 weeks (line)
    - Repeat offenders (horizontal bar)
    - Per-PLC heatmap (only when ``plc`` is provided)
    """
    selected_plc = request.args.get("plc", "").strip() or None
    plc_names    = _fault_count_service.get_all_plc_names()

    now = dt.date.today()
    selected_month = request.args.get("month", type=int, default=now.month)
    selected_week  = request.args.get("week",  type=int, default=1)
    selected_year  = now.year  # always current year for now

    selected_date = _first_monday_of_month_week(selected_year, selected_month, selected_week)

    months = [(m, dt.date(selected_year, m, 1).strftime("%B")) for m in range(1, 13)]

    chart_html      = _diagram_service.grouped_bar_chart_html(reference_date=selected_date)
    chart_html2     = _diagram_service.grouped_bar_chart_2_html(reference_date=selected_date)
    pie_html        = _diagram_service.pie_chart_html(reference_date=selected_date)
    heatmap         = _diagram_service.heatmap_html(selected_plc) if selected_plc else ""
    mtbf_html       = _diagram_service.mtbf_html(reference_date=selected_date)
    long_html       = _diagram_service.long_term_trend_html()
    repeat_offender = _diagram_service.repeat_offenders_html(reference_date=selected_date)

    return render_template(
        "diagrams.html",
        title="Diagrams",
        chart_html=chart_html,
        chart_2_html=chart_html2,
        pie_html=pie_html,
        heatmap_html=heatmap,
        plc_names=plc_names,
        selected_plc=selected_plc,
        mtbf_html=mtbf_html,
        long_html=long_html,
        repeat_offender=repeat_offender,
        months=months,
        selected_month=selected_month,
        selected_week=selected_week,
        selected_date=selected_date.isoformat(),
    )


@bp.route("/diagrams-pdf")
def diagrams_pdf():
    """
    Export all dashboard charts as a multi-page landscape PDF.

    Accepts the same ``month`` and ``week`` query parameters as
    :func:`diagrams`.  Delegates rendering to
    :class:`~presentations.services.diagram_pdf_service.DiagramPdfService`,
    which converts each Plotly figure to a PNG via Kaleido and assembles
    them into a ReportLab PDF (2 charts per page).

    Returns
    -------
    flask.Response
        A ``application/pdf`` attachment named
        ``diagrams_<reference_date>.pdf``.
    """
    now = dt.date.today()
    selected_month = request.args.get("month", type=int, default=now.month)
    selected_week = request.args.get("week", type=int, default=1)
    selected_date = _first_monday_of_month_week(now.year, selected_month, selected_week)

    buf = _diagram_pdf_service.generate_pdf(reference_date=selected_date)
    return send_file(
        buf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"diagrams_{selected_date.isoformat()}.pdf",
    )


@bp.route("/pdf-table_tree_export-tree", methods=["POST"])
def table_tree_export():
    """
    Export the current Interlock Table Tree result as a PDF.

    Reads the same filter form fields as :func:`table_tree` (POST body).
    On validation failure, redirects back to ``table_tree`` with a flash
    error.  On success, calls :class:`~presentations.services.pdf_generator.PdfGenerator`
    to build the PDF in memory and streams it as an attachment.

    Returns
    -------
    flask.Response
        A ``application/pdf`` attachment named ``table_tree_export.pdf``,
        or a redirect on validation error.
    """
    parsed = _parse_table_tree_filters_or_redirect(redirect_endpoint="plc.table_tree")
    if not isinstance(parsed, tuple):
        return parsed  # redirect response

    analyze_kwargs, _params = parsed
    items = service_interlock.analyze_interlock(**analyze_kwargs)
    buf = PdfGenerator().generate_interlock(items)

    return send_file(
        buf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name="table_tree_export.pdf",
    )


@bp.route("/table-tree", methods=["GET", "POST"])
def table_tree():
    """
    Render the Interlock Table Tree page with optional filtering.

    **POST (form submit):**
    Validates the filter inputs.  On success, redirects to the GET version
    of this route with all filters encoded as query parameters
    (POST → Redirect → GET pattern to prevent duplicate submissions on
    browser refresh).  On validation error, flashes a message and redirects
    back with whatever valid params were already collected.

    **GET:**
    Reads filter values from the query string.  If no filters are provided,
    renders the page with an empty result set so the user sees the form
    without a loading delay.  When filters are present, calls
    :meth:`InterlockService.analyze_interlock` and passes the result tree
    to ``table_tree.html``.

    Query / form parameters
    -----------------------
    target_bsid : int, optional
        Filter results to a specific interlock BSID.
    top_n : int, optional
        Limit output to the top N interlocks by fault count.
    filter_timestamp_start : str, optional
        ISO 8601 datetime lower bound, e.g. ``"2024-05-01T00:00"``.
    filter_timestamp_end : str, optional
        ISO 8601 datetime upper bound.
    filter_condition_message : str, optional
        Substring filter on the condition message text.
    filter_plc : str, optional
        Filter results to a specific PLC name.
    """
    if request.method == "POST":
        parsed = _parse_table_tree_filters_or_redirect(redirect_endpoint="plc.table_tree")
        if not isinstance(parsed, tuple):
            return parsed  # redirect response

        _analyze_kwargs, params = parsed
        return redirect(url_for("plc.table_tree", **params))

    # GET: read parameters
    target_bsid = request.args.get("target_bsid", type=int)
    top_n = request.args.get("top_n", type=int)
    filter_timestamp_start_raw = request.args.get("filter_timestamp_start")
    filter_timestamp_end_raw = request.args.get("filter_timestamp_end")
    filter_condition_message = request.args.get("filter_condition_message")
    filter_plc = request.args.get("filter_plc")

    try:
        filter_timestamp_start = _parse_iso_datetime(filter_timestamp_start_raw, "filter_timestamp_start")
        filter_timestamp_end = _parse_iso_datetime(filter_timestamp_end_raw, "filter_timestamp_end")
    except ValueError as e:
        flash(str(e), "error")
        filter_timestamp_start = None
        filter_timestamp_end = None

    if (
            target_bsid is None
            and top_n is None
            and filter_timestamp_start is None
            and filter_timestamp_end is None
            and filter_condition_message is None
            and filter_plc is None
    ):
        items = []
    else:
        items = service_interlock.analyze_interlock(
            target_bsid=target_bsid,
            top_n=top_n,
            filter_timestamp_start=filter_timestamp_start,
            filter_timestamp_end=filter_timestamp_end,
            filter_condition_message=filter_condition_message,
            filter_plc=filter_plc,
        )

    flashed = get_flashed_messages(with_categories=True)
    messages = [m for cat, m in flashed if cat in ("error", "success")]

    return render_template(
        "table_tree.html",
        title="Interlock Tree",
        items=items,
        messages=messages,
        target_bsid=target_bsid,
        top_n=top_n,
        filter_timestamp_start=filter_timestamp_start_raw,
        filter_timestamp_end=filter_timestamp_end_raw,
        filter_condition_message=filter_condition_message,
        filter_plc=filter_plc,
    )
