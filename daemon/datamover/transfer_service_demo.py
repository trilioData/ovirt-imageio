# """
# Entry point for Flask application.
# All the flask and workloadmgr related configuration are setup here.
# """
import threading
from flask import Flask, jsonify
from flask_cors import CORS

# Create object of flask application
app = Flask(__name__)
CORS(app, support_credentials=True)

with app.app_context():
    from transfer_api import tvm_blueprint

    app.register_blueprint(tvm_blueprint)

    def global_exception_handler(ex_obj):
        return jsonify(error=str(ex_obj)), ex_obj.code
    app.register_error_handler(Exception, global_exception_handler)


if __name__ == '__main__':
   app.run(host="0.0.0.0")

#
# from ovirt_utils import create_n_download_snapshot
# create_n_download_snapshot("","","","","")