from flask import Blueprint
from flask_cors import CORS
# local imports
from controllers.UsersController import *

# init blueprint
users_bp = Blueprint("users_bp", __name__)
CORS(users_bp)

# Routes
users_bp.route('/user/<userId>', methods=['GET']) (getUserById)
users_bp.route('/getUserbyEsperId', methods=['POST']) (getUserByEsperId)
users_bp.route('/users', methods=['GET']) (getUsers)

users_bp.route('/user', methods=['POST']) (createUser)
users_bp.route('/postUserOnboard', methods=['POST']) (postUserOnboard)
users_bp.route('/getUserByEmail', methods=['POST']) (getUserByEmail)
users_bp.route('/getUserOnboardingById', methods=['POST']) (getUserOnboardingById)
users_bp.route('/sentActivationEmail', methods=['POST']) (sendUserActivationEmail)
users_bp.route('/updatePasswordInactive', methods=['POST']) (updatePasswordInactive)

users_bp.route("/user/login", methods=["POST"]) (loginUser)
users_bp.route("/user/logout", methods=["POST"]) (logoutUser)

users_bp.route('/user/<userId>/password', methods=['PUT']) (changeUserPassword)
users_bp.route('/user/<userId>', methods=['PUT']) (updateUser)
users_bp.route('/user/<userId>', methods=['DELETE']) (deleteUser)


users_bp.route('/user/forgot-password', methods=['POST']) (sendForgotPasswordEmail)
users_bp.route('/user/reset-password', methods=['POST']) (resetUserPassword)


