from flask import Blueprint, request, jsonify
from controllers.transaction_controller import TransactionController

# Create a blueprint for transaction routes
transaction_routes = Blueprint("transaction_routes", __name__)

# Initialize the TransactionController
transaction_controller = TransactionController()

@transaction_routes.route("/extended", methods=["GET"])
def get_transactions_extended():
    """
    GET /transactions/extended
    Returns all transactions in extended format with optional filters.
    """
    filters = request.args.to_dict()
    result = transaction_controller.get_transactions_extended(filters)
    return jsonify(result)