def register_routes(app):
    from .user_routes import user_routes
    app.register_blueprint(user_routes)