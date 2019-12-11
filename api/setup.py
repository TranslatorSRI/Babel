'''
Set up Flask server
'''

from flask import Flask, Blueprint, request
from flask_restful import Api
from flasgger import Swagger, LazyString, LazyJSONEncoder

app = Flask(__name__, static_folder='../pack', template_folder='../templates')

api_blueprint = Blueprint('api', __name__, url_prefix='/api')
api = Api(api_blueprint)
app.register_blueprint(api_blueprint)

template = {
    "openapi": "3.0.1",
    "info": {
        "title": "ROBOKOP Builder",
        "description": "An API connecting questions with biomedical knowledge services",
        "contact": {
            "responsibleOrganization": "CoVar Applied Technologies",
            "responsibleDeveloper": "patrick@covar.com",
            "email": "patrick@covar.com",
            "url": "www.covar.com",
        },
        "termsOfService": "<url>",
        "version": "0.0.1"
    },
    "schemes": [
        "http",
        "https"
    ],
    'swaggerUiPrefix': LazyString (lambda : request.environ.get('HTTP_X_SWAGGER_PREFIX', ''))
}
app.json_encoder = LazyJSONEncoder

app.config['SWAGGER'] = {
    'title': 'ROBOKOP Builder API',
    'uiversion': 3
}
swagger_config = {
    "headers": [
    ],
    "specs": [
        {
            "endpoint": 'apispec_1',
            "route": '/builder/spec',
            "rule_filter": lambda rule: True,  # all in
            "model_filter": lambda tag: True,  # all in
        }
    ],
    "swagger_ui": True,
    "specs_route": "/apidocs/",
    "openapi": "3.0.1",
    'swagger_ui_bundle_js': 'https://rawcdn.githack.com/swagger-api/swagger-ui/v3.23.1/dist/swagger-ui-bundle.js',
    'swagger_ui_standalone_preset_js': 'https://rawcdn.githack.com/swagger-api/swagger-ui/v3.23.1/dist/swagger-ui-standalone-preset.js',
    'swagger_ui_css': 'https://rawcdn.githack.com/swagger-api/swagger-ui/v3.23.1/dist/swagger-ui.css',
    'swagger_ui_js': 'https://rawcdn.githack.com/swagger-api/swagger-ui/v3.23.1/dist/swagger-ui.js'
}
swagger = Swagger(app, template=template, config=swagger_config)