from typing import List

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    url_for, get_flashed_messages,
)

from business.services.analyzer import InterlockAnalyzer
from business.services.diagram_service_view import DiagramService
from data.model.models import InterlockNode

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


@bp.route("/table-tree")
def table_tree():
    items:List[InterlockNode]=service_interlock.analyze_interlock(target_bsid=11222)
    return render_template("table_tree.html", title="Table", items=items)


# New form route
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
