from .user_routes import user_routes
from .charger_routes import charger_routes
from .transaction_routes import transaction_routes

def register_routes(app):
    app.register_blueprint(user_routes, url_prefix="/users")
    app.register_blueprint(charger_routes, url_prefix="/chargers")
    app.register_blueprint(transaction_routes, url_prefix="/transactions")