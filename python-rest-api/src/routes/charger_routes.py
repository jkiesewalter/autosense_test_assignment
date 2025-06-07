from flask import Blueprint, request, jsonify
from controllers.charger_controller import ChargerController

# Create a blueprint for charger routes
charger_routes = Blueprint("charger_routes", __name__)

# Initialize the ChargerController
charger_controller = ChargerController()

@charger_routes.route("/", methods=["GET"])
def get_chargers():
    """
    GET /chargers
    Returns all chargers or filters based on query parameters.
    """
    filters = request.args.to_dict()
    result = charger_controller.get_chargers(filters)
    return jsonify(result)

@charger_routes.route("/<charger_id>/usage-analytics", methods=["GET"])
def get_usage_analytics(charger_id):
    """
    GET /chargers/<charger_id>/usage-analytics
    Returns usage analytics for a specific charger.
    """
    filters = request.args.to_dict()
    result = charger_controller.get_usage_analytics(charger_id, filters)
    return jsonify(result)