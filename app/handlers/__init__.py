from .auth import register as register_auth
from .forms import register as register_forms
from .requests import register as register_requests
from .photos import register as register_photos
from .admin import register as register_admin


def register_all_handlers(dp, bot):
    register_auth(dp, bot)
    register_forms(dp, bot)
    register_requests(dp, bot)
    register_photos(dp, bot)
    register_admin(dp, bot)
