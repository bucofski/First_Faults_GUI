
from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for, get_flashed_messages,
)

from business.services.diagram_service_view import DiagramService
from business.services.plc_data_service import PLCDataService

service= PLCDataService()
bp = Blueprint("plc", __name__, url_prefix="/plc")


@bp.route("/")
def home():
    return render_template("home.html", title="Home")

@bp.route("/table")
def table():
    return render_template("table.html", title="Table", data=service.get_plc_data())

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
    pie_html = DiagramService.PieChart_html()
    return render_template("diagrams.html", title="Diagrams", chart_html=chart_html, chart_2_html=chart_html2, pie_html=pie_html)

@bp.route("/table-tree")
def table_tree():
    items = [
        {
            "id": 1,
            "name": "Parent A",
            "status": "OK",
            "children": [
                {"id": 11, "name": "Child A1", "status": "OK"},
                {"id": 13, "name": "Child A2", "status": "Warn"},
                {"id": 14, "name": "Child A2", "status": "Warn"},
                {"id": 15, "name": "Child A2", "status": "Warn"},
                {"id": 16, "name": "Child A2", "status": "Warn"},
            ],
        },
        {
            "id": 2,
            "name": "Parent B",
            "status": "Fail",
            "children": [
                {"id": 21, "name": "Child B1", "status": "OK"},
                {"id": 22, "name": "Child B2", "status": "Warn"},
                {"id": 23, "name": "Child B3", "status": "Fail"},
                {"id": 24, "name": "Child B4", "status": "OK"},
                {"id": 25, "name": "Child B5", "status": "Warn"},
                {"id": 26, "name": "Child B6", "status": "Fail"},
                {"id": 27, "name": "Child B7", "status": "OK"},
                {"id": 28, "name": "Child B8", "status": "Warn"},
                {"id": 29, "name": "Child B9", "status": "Fail"},
                {"id": 30, "name": "Child B10", "status": "OK"},
                {"id": 31, "name": "Child B11", "status": "Warn"},
                {"id": 32, "name": "Child B12", "status": "Fail"},
                {"id": 33, "name": "Child B13", "status": "OK"},
                {"id": 34, "name": "Child B14", "status": "Warn"},
                {"id": 35, "name": "Child B15", "status": "Fail"},
                {"id": 36, "name": "Child B16", "status": "OK"},
                {"id": 37, "name": "Child B17", "status": "Warn"},
                {"id": 38, "name": "Child B18", "status": "Fail"},
                {"id": 39, "name": "Child B19", "status": "OK"},
                {"id": 40, "name": "Child B20", "status": "Warn"},
            ],
        },
        {
            "id": 3,
            "name": "Parent C",
            "status": "OK",
            "children": [],
        },
    ]
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
            flash({"name": name, "email": email, "age": age, "topic": topic, "message": message}, "form-data")
            return redirect(url_for("form"))

        # No errors → process (store/send/etc). Here we just flash the result.
        flash("Form submitted successfully.", "success")
        flash({"name": name, "email": email, "age": age, "topic": topic, "message": message}, "form-data")
        return redirect(url_for("plc.form"))

    # GET: render the form and pick up flashed data/messages
    flashed = get_flashed_messages(with_categories=True)
    # separate messages and form-data
    messages = [m for cat, m in flashed if cat in ("error", "success")]
    form_data_items = [m for cat, m in flashed if cat == "form-data"]
    form_data = form_data_items[0] if form_data_items else {}
    return render_template("form.html", title="Form", messages=messages, form_data=form_data)

