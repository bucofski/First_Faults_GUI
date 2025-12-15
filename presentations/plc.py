from typing import List

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


# Add a route to render diagrams page
@bp.route("/diagrams")
def diagrams():
    chart_html = DiagramService.grouped_bar_chart_html()
    chart_html2 = DiagramService.grouped_bar_chart_2_html()
    pie_html = DiagramService.pie_chart_html()
    return render_template("diagrams.html", title="Diagrams", chart_html=chart_html, chart_2_html=chart_html2,
                           pie_html=pie_html)
@bp.route("/pdf-table_tree_export-tree", methods=["POST"])
def table_tree_export():
    # Read same fields as table_tree
    target_bsid_str = request.form.get("target_bsid", "").strip()
    top_n_str = request.form.get("top_n", "").strip()
    filter_date = request.form.get("filter_date", "").strip() or None
    filter_timestamp_start = request.form.get("filter_timestamp_start", "").strip() or None
    filter_timestamp_end = request.form.get("filter_timestamp_end", "").strip() or None
    filter_condition_message = request.form.get("filter_condition_message", "").strip() or None
    filter_plc = request.form.get("filter_plc", "").strip() or None

    target_bsid = None
    top_n = None

    if target_bsid_str:
        try:
            target_bsid = int(target_bsid_str)
        except ValueError:
            flash("Target BSID must be a valid integer.", "error")
            return redirect(url_for("plc.table_tree"))

    if top_n_str:
        try:
            top_n = int(top_n_str)
        except ValueError:
            flash("Top N must be a valid integer.", "error")
            return redirect(url_for("plc.table_tree"))

    # Fetch the same data you show in the UI
    items = service_interlock.analyze_interlock(
        target_bsid=target_bsid,
        top_n=top_n,
        filter_date=filter_date,
        filter_timestamp_start=filter_timestamp_start,
        filter_timestamp_end=filter_timestamp_end,
        filter_condition_message=filter_condition_message,
        filter_plc=filter_plc,
    )
    buf=PdfGenerator().generate_interlock(items)

    return send_file(
        buf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name="table_tree_export.pdf",
    )

# ... existing code ...
@bp.route("/table-tree", methods=["GET", "POST"])
def table_tree():
    if request.method == "POST":
        target_bsid_str = request.form.get("target_bsid", "").strip()
        top_n_str = request.form.get("top_n", "").strip()
        filter_date = request.form.get("filter_date", "").strip() or None
        filter_timestamp_start = request.form.get("filter_timestamp_start", "").strip() or None
        filter_timestamp_end = request.form.get("filter_timestamp_end", "").strip() or None
        filter_condition_message = request.form.get("filter_condition_message", "").strip() or None
        filter_plc = request.form.get("filter_plc", "").strip() or None

        params = {}
        if target_bsid_str:
            try:
                params["target_bsid"] = int(target_bsid_str)
            except ValueError:
               flash("Target BSID must be a valid integer.", "error")
              #  return redirect(url_for("plc.table_tree"))

        if top_n_str:
            try:
                params["top_n"] = int(top_n_str)
            except ValueError:
                flash("Top must be a valid integer.", "error")

        if filter_date:
            params["filter_date"] = filter_date
        if filter_timestamp_start:
            params["filter_timestamp_start"] = filter_timestamp_start
        if filter_timestamp_end:
            params["filter_timestamp_end"] = filter_timestamp_end
        if filter_condition_message:
            params["filter_condition_message"] = filter_condition_message
        if filter_plc:
            params["filter_plc"] = filter_plc

        return redirect(url_for("plc.table_tree", **params))

    # GET: read parameters
    target_bsid = request.args.get("target_bsid", type=int)
    top_n = request.args.get("top_n", type=int)
    filter_date = request.args.get("filter_date")
    filter_timestamp_start = request.args.get("filter_timestamp_start")
    filter_timestamp_end = request.args.get("filter_timestamp_end")
    filter_condition_message = request.args.get("filter_condition_message")
    filter_plc = request.args.get("filter_plc")

    if target_bsid is None and top_n is None and filter_date is None and filter_timestamp_start is None and filter_timestamp_end is None and filter_condition_message is None and filter_plc is None:
        items= []
    else:
        items = service_interlock.analyze_interlock(
            target_bsid=target_bsid,
            top_n=top_n,
            filter_timestamp_start=filter_timestamp_start,
            filter_timestamp_end=filter_timestamp_end,
            filter_condition_message=filter_condition_message,
            filter_plc=filter_plc

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
        filter_timestamp_start=filter_timestamp_start,
        filter_timestamp_end=filter_timestamp_end,
        filter_condition_message=filter_condition_message,
        filter_plc=filter_plc
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
