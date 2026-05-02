from flask import Blueprint, render_template

coffee_dashboard_bp = Blueprint("coffee_dashboard", __name__)


@coffee_dashboard_bp.route("/")
def index():
    return render_template("coffee/dashboard.html")
