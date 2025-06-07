from flask import Flask, request, jsonify
from controllers.user_controller import UserController
from controllers.charger_controller import ChargerController
from controllers.transaction_controller import TransactionController
from routes import register_routes

def create_app():
    app = Flask(__name__)

    # Initialize controllers
    user_controller = UserController()
    charger_controller = ChargerController()
    transaction_controller = TransactionController()

    # Middleware can be added here

    register_routes(app)

    @app.route('/users', methods=['GET'])
    def get_users():
        filters = request.args.to_dict()
        result = user_controller.get_users(filters)
        return jsonify(result)

    @app.route('/chargers', methods=['GET'])
    def get_chargers():
        filters = request.args.to_dict()
        result = charger_controller.get_chargers(filters)
        return jsonify(result)

    @app.route('/chargers/<charger_id>/usage-analytics', methods=['GET'])
    def get_usage_analytics(charger_id):
        filters = request.args.to_dict()
        result = charger_controller.get_usage_analytics(charger_id, filters)
        return jsonify(result)

    @app.route('/transactions-extended', methods=['GET'])
    def get_transactions_extended():
        filters = request.args.to_dict()
        result = transaction_controller.get_transactions_extended(filters)
        return jsonify(result)

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)