from flask import Blueprint, request, jsonify
from controllers.user_controller import UserController

# Create a blueprint for user routes
user_routes = Blueprint("user_routes", __name__)

# Initialize the UserController
user_controller = UserController()

@user_routes.route("/", methods=["GET"])
def get_users():
    """
    GET /users
    Returns all users or filters based on query parameters.
    """
    filters = request.args.to_dict()
    result = user_controller.get_users(filters)
    return jsonify(result)