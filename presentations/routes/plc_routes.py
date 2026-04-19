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
service_interlock = InterlockService()
_diagram_service = DiagramService()
_diagram_pdf_service = DiagramPdfService()
_fault_count_service = FaultCountService()


def _parse_iso_datetime(value: str | None, field_name: str) -> dt.datetime | None:

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
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        flash(error_message, "error")
        return None


def _read_table_tree_form_params() -> dict:
    """Read and normalize table-tree form fields from request.form."""
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
    Parse table-tree inputs from request.form.
    Returns (analyze_kwargs, params_for_redirect) on success.
    Returns a Flask redirect response on validation error.
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


@bp.route("/")
def home():
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
    return render_template("table.html", title="Table", data=None)


@bp.route("/about")
def about():
    return render_template("about.html", title="About")


@bp.route("/contact")
def contact():
    return render_template("contact.html", title="Contact")


def _first_monday_of_month_week(year: int, month: int, week: int) -> dt.date:
    """Return the Monday of the *week*-th week in the given month.

    Week 1 contains the first Monday that falls in (or starts) the month.
    If the requested week overshoots the month, the last valid Monday is
    returned instead.
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
    selected_plc = request.args.get("plc", "").strip() or None
    plc_names    = _fault_count_service.get_all_plc_names()

    # --- month / week selection ---
    now = dt.date.today()
    selected_month = request.args.get("month", type=int, default=now.month)
    selected_week  = request.args.get("week",  type=int, default=1)
    selected_year  = now.year  # always current year for now

    selected_date = _first_monday_of_month_week(selected_year, selected_month, selected_week)

    # Build month options list
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
