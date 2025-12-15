from typing import List
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

from business.services.analyzer import InterlockAnalyzer
from business.services.diagram_service_view import DiagramService

from presentations.services.pdf_generator import PdfGenerator

bp = Blueprint("plc", __name__, url_prefix="/plc")
service_interlock = InterlockAnalyzer()


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
    return render_template("home.html", title="Home")


@bp.route("/table")
def table():
    return render_template("table.html", title="Table", data=None)


@bp.route("/about")
def about():
    return render_template("about.html", title="About")


@bp.route("/contact")
def contact():
    return render_template("contact.html", title="Contact")


@bp.route("/diagrams")
def diagrams():
    chart_html = DiagramService.grouped_bar_chart_html()
    chart_html2 = DiagramService.grouped_bar_chart_2_html()
    pie_html = DiagramService.pie_chart_html()
    return render_template(
        "diagrams.html",
        title="Diagrams",
        chart_html=chart_html,
        chart_2_html=chart_html2,
        pie_html=pie_html,
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


@bp.route("/form", methods=["GET", "POST"])
def form():
    if request.method == "POST":
        # Read fields
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        age = request.form.get("age", "").strip()
        topic = request.form.get("topic", "")
        message = request.form.get("message", "").strip()

        # Basic validation (expand as you like)
        errors = []
        if not name:
            errors.append("Name is required.")
        if email and "@" not in email:
            errors.append("Email looks invalid.")
        if age:
            try:
                age_val = int(age)
                if age_val < 0 or age_val > 120:
                    errors.append("Age must be between 0 and 120.")
            except ValueError:
                errors.append("Age must be a number.")
        # if errors, flash them and redirect back (PRG)
        if errors:
            for e in errors:
                flash(e, "error")
            # preserve minimal form data by flashing a dict (or use session)
            flash(
                f"Form submitted successfully: Name - {name}, Email - {email}, Age - {age}, Topic - {topic}, Message - {message}",
                "success")
            return redirect(url_for("form"))

        # No errors → process (store/send/etc). Here we just flash the result.
        flash("Form submitted successfully.", "success")
        flash(
            f"Form submitted successfully: Name - {name}, Email - {email}, Age - {age}, Topic - {topic}, Message - {message}",
            "form-data")
        return redirect(url_for("plc.form"))

    # GET: render the form and pick up flashed data/messages
    flashed = get_flashed_messages(with_categories=True)
    # separate messages and form-data
    messages = [m for cat, m in flashed if cat in ("error", "success")]
    form_data_items = [m for cat, m in flashed if cat == "form-data"]
    form_data = form_data_items[0] if form_data_items else {}
    return render_template("form.html", title="Form", messages=messages, form_data=form_data)