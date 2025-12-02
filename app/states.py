from aiogram.dispatcher.filters.state import State, StatesGroup

class RegForm(StatesGroup):
    first_name = State()
    last_name = State()
    age = State()
    contact = State()

class RequestForm(StatesGroup):
    client_name = State()
    client_company = State()
    client_contact = State()
    site_company = State()
    business_type = State()
    color_palette = State()
    site_contacts = State()
    short_desc = State()
    work_hours = State()
    # structure убран из анкеты - будет по умолчанию
    images = State()
    services = State()
    portfolio = State()
    testimonials = State()
    faq = State()
    seo_description = State()
    hero_title = State()
    hero_subtitle = State()

class AdminLogin(StatesGroup):
    password = State()

class EditField(StatesGroup):
    waiting_value = State()

class PhotoUpload(StatesGroup):
    """Пошаговая загрузка фото по категориям"""
    choosing_category = State()  # Выбор категории фото
    uploading = State()  # Загрузка фото в выбранную категорию
