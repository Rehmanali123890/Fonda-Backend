from flask import Blueprint

# local imports
from controllers.PlatformController import *

# init blueprint
platform_bp = Blueprint("platform_bp", __name__)


# platforms
platform_bp.route("/platform/<merchantId>/connect_platform", methods=['POST'])(createPlatform)
platform_bp.route("/platform/<merchantId>/get_platforms", methods=['GET'])(getPlatforms)
platform_bp.route("/platform/<merchantId>/update_stream_status", methods=['POST'])(updateStreamStatus)
platform_bp.route("/platform/<merchantId>/connect_platform_ubereats", methods=['POST'])(connectUberEats)

platform_bp.route("/merchant/<merchantId>/platform/<platformId>/synctype", methods=['PUT'])(UpdatePlatformSyncType)



platform_bp.route("/merchant/<merchantId>/platform/<platformId>/disconnect_platform", methods=['DELETE'])(DeletePlatform)

platform_bp.route("/merchant/<merchantId>/platform/<platformId>", methods=['GET']) (getPlatformById)

platform_bp.route("/platform/<merchantId>/connect_platform_clover", methods=['POST'])(connectClover)
platform_bp.route("/platform/<merchantId>/connect_platform_square", methods=['POST'])(connectSquare)
platform_bp.route("/platform/<merchantId>/connect_platform_square/<location>", methods=['POST'])(selectSquareLocation)
platform_bp.route("/platform/connect_google", methods=['POST'])(connectGoogle)
platform_bp.route("/platform/link_location", methods=['POST'])(linkLocation)
platform_bp.route("/merchant/<merchantId>/location", methods=['GET'])(getgoogleLocationByMerchantId)
platform_bp.route("/platform/unlink_location", methods=['POST'])(unlinkLocation)
platform_bp.route("/platform/get_reviews", methods=['POST'])(googleReviews)
platform_bp.route("/platform/reply_review", methods=['POST'])(replyGoogleReview)
platform_bp.route("/platform/sync_google_profile", methods=['POST'])(syncGoogleMyBusinessProfile)
platform_bp.route("/platform/sync_business_hour", methods=['PATCH'])(syncGoogleMyBusinessHours)
platform_bp.route("/platform/get_menu", methods=["POST"])(getMenu)
platform_bp.route("/platform/get_all_media", methods=["POST"])(getAllMedia)
platform_bp.route("/platform/menu_hour", methods=['PATCH'])(syncMenuHours)
# platform_bp.route("/platform/<merchantId>/create_google_location", methods=['GET'])(createGoogleLocation)
platform_bp.route("/platform/update_order_placer", methods=['PATCH'])(updatePlaceAction)
platform_bp.route("/platform/get_order_placer", methods=['POST'])(getPlacesAction)
platform_bp.route("/platform/upload-media", methods=["POST"])(uploadmedia)
platform_bp.route("/platform/upload-menu", methods=["POST"])(uploadMenuToGMB)
platform_bp.route("/platform/delete-media", methods=["POST"])(deletemedia)
platform_bp.route("/platform/sync_google_auth", methods=['GET'])(insert_refresh_tokens)
# platform_bp.route("/platform/gmbcronjob", methods=["POST"])(weekly_gmb_cronjob)
# platform_bp.route("/twilliocall", methods=["GET"])(twilio_call)
# platform_bp.route("/voice", methods=["GET"])(voice)
# platform_bp.route("/gather", methods=["GET"])(gather)
# platform_bp.route("/platform/update_order_placer", methods=['PATCH'])(updatePlaceAction)