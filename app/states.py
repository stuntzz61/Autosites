from aiogram.dispatcher.filters.state import State, StatesGroup


class RegForm(StatesGroup):
    """Форма регистрации пользователя"""
    first_name = State()
    last_name = State()
    age = State()
    contact = State()


class RequestForm(StatesGroup):
    """Форма создания заявки на сайт"""
    # Данные клиента
    client_name = State()
    client_company = State()
    client_contact = State()

    # Данные для сайта
    site_company = State()
    business_type = State()
    color_palette = State()

    # Контакты (разделены)
    phone = State()
    email = State()
    address = State()

    # Описание и режим работы
    short_desc = State()
    work_hours = State()

    # Дополнительные данные
    services = State()
    portfolio = State()
    testimonials = State()
    faq = State()
    seo_description = State()
    hero_title = State()
    hero_subtitle = State()


class AdminLogin(StatesGroup):
    """Авторизация администратора"""
    password = State()


class EditField(StatesGroup):
    """Редактирование отдельного поля"""
    waiting_value = State()


class PhotoUpload(StatesGroup):
    """Пошаговая загрузка фото по категориям"""
    choosing_category = State()
    uploading = State()
