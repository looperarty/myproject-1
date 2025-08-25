import logging
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters, CallbackQueryHandler, Application 
import datetime 

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign 
from facebook_business.adobjects.adset import AdSet 
from facebook_business.exceptions import FacebookRequestError

import io 
import matplotlib.pyplot as plt 

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import json 
import os 

plt.style.use('seaborn-v0_8-darkgrid') 

# --- КЛЮЧИ И НАСТРОЙКИ ---
TELEGRAM_TOKEN = '7879718352:AAFdjjlblqNROm4mq8GLB9pnRdUPIaq8lHw' 
ADMIN_TELEGRAM_ID = 5625120142  # ВАШ_ТЕЛЕГРАМ_ID_СЮДА (числом, без кавычек)

FB_ACCESS_TOKEN = 'EAAKhhJ2VeSsBPSLZBWCVZC3trqpLMW8RyU59XrbEn4y1R2KsbtdT1s9ncBTXZBSUgAhG3nA1Po5WVvnvayZCSq89FokeaaGP2Q0rm0Dc4uoWx5LBVJOGg8oeHedW9OhCwaUkJfk9c472gYCqVSKB2ZAB1Irlee3mAieZBuH7MPlsTklllrL5zbCzRT4LK0rWOGiLAmzwzFZAPlAF6m9awZA7ZCMPO0DV93WIxYdm6xKyGAjz1HXOxDTJE4rgPAwS0ifMZD' 
FB_APP_ID = '740540904995115'
FB_APP_SECRET = '7d665dc84c588fba122066991ea76f2b'
AD_ACCOUNT_ID = 'act_1573639266674008' 
# -----------------------------------------

# --- Настройки для бота ---
SETTINGS_FILE = 'bot_settings.json' 

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, 'r') as f:
            try:
                settings = json.load(f)
                settings.setdefault('auto_reports_enabled', False)
                settings.setdefault('report_time', '09:00')
                settings.setdefault('notifications_enabled', False)
                settings.setdefault('cost_per_conversation_threshold', 2.0)
                settings.setdefault('last_cost_per_conversation_alert', None)
                settings.setdefault('login_code', '0105') 
                current_authorized_users = settings.get('authorized_users', [])
                if ADMIN_TELEGRAM_ID not in current_authorized_users:
                    current_authorized_users.append(ADMIN_TELEGRAM_ID)
                settings['authorized_users'] = list(set(current_authorized_users))
                settings.setdefault('daily_orders', {}) 
                return settings
            except json.JSONDecodeError:
                logging.error(f"Ошибка чтения файла настроек {SETTINGS_FILE}. Используем значения по умолчанию.")
                return {
                    'auto_reports_enabled': False, 
                    'report_time': '09:00', 
                    'notifications_enabled': False, 
                    'cost_per_conversation_threshold': 2.0,
                    'last_cost_per_conversation_alert': None,
                    'login_code': '0105', 
                    'authorized_users': [ADMIN_TELEGRAM_ID],
                    'daily_orders': {} 
                }
    return {
        'auto_reports_enabled': False, 
        'report_time': '09:00', 
        'notifications_enabled': False, 
        'cost_per_conversation_threshold': 2.0,
        'last_cost_per_conversation_alert': None,
        'login_code': '0105', 
        'authorized_users': [ADMIN_TELEGRAM_ID],
        'daily_orders': {} 
    } 

def save_settings(settings):
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(settings, f, indent=4)

bot_settings = load_settings()

scheduler = AsyncIOScheduler()
# --- КОНЕЦ НАСТРОЕК ---


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- ВСЕ ФУНКЦИИ ОПРЕДЕЛЯЮТСЯ ЗДЕСЬ, ДО БЛОКА if __name__ == '__main__': ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает команду /start.
    Проверяет, является ли пользователь авторизованным, и отображает основное меню.
    """
    user_id = update.effective_user.id
    if user_id not in bot_settings['authorized_users']:
        await context.bot.send_message(chat_id=user_id, text="Добро пожаловать! У вас нет доступа к боту. Пожалуйста, введите `/auth <ваш_код>` для авторизации.")
        logging.warning(f"Попытка доступа неавторизованного пользователя: {user_id}")
        return

    keyboard = [
        [KeyboardButton("📊 Получить отчет")], 
        [KeyboardButton("💸 Потрачено")], 
        [KeyboardButton("📦 Кол. заказов")], # <-- НОВАЯ КНОПКА
        [KeyboardButton("⚙️ Настройки")], 
        [KeyboardButton("🔔 Уведомления")], 
        [KeyboardButton("📊 Отчет по продажам")], # <-- НОВАЯ КНОПКА
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await context.bot.send_message(
        chat_id=update.effective_chat.id, 
        text="Авторизация пройдена. Выберите действие:", 
        reply_markup=reply_markup
    )
    logging.info(f"Пользователь {user_id} запустил бот.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает текстовые сообщения от пользователя.
    Перенаправляет на функцию выбора периода, настроек или помощи.
    """
    user_id = update.effective_user.id
    if user_id not in bot_settings['authorized_users']: 
        await context.bot.send_message(chat_id=user_id, text="Извините, у вас нет доступа к этому боту. Введите `/auth <ваш_код>` для авторизации.")
        return
    
    text = update.message.text
    
    # --- ОБРАБОТКА ОЖИДАЕМОГО ВВОДА ДЛЯ НАСТРОЕК ---
    if context.user_data.get('awaiting_input_for'):
        input_type = context.user_data.pop('awaiting_input_for')
        
        if input_type == 'cost_per_conversation_threshold':
            try:
                value = float(text)
                bot_settings['cost_per_conversation_threshold'] = value
                save_settings(bot_settings)
                await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Порог цены за переписку установлен на **${value:.2f}**.")
                logging.info(f"Пользователь {user_id} установил 'cost_per_conversation_threshold' на '{value}'.")
                await show_notification_settings_menu(update, context) 
            except ValueError:
                await context.bot.send_message(chat_id=update.effective_chat.id, text="Пожалуйста, введите корректное числовое значение.")
                logging.warning(f"Пользователь {user_id} ввел некорректное значение для порога: '{text}'.")
                context.user_data['awaiting_input_for'] = 'cost_per_conversation_threshold' # Оставляем флаг, чтобы он мог повторить ввод
        # --- УДАЛЕНО: ЛОГИКА ВВОДА ДАТЫ И КОЛИЧЕСТВА ЗАКАЗОВ В handle_message (теперь +1 по кнопке) ---
        # (orders_date_input и orders_count_input обработка удалена)
        else: # Неизвестный тип ожидаемого ввода
            await context.bot.send_message(chat_id=update.effective_chat.id, text="Неизвестный тип ожидаемого ввода. Пожалуйста, используйте кнопки.")
            logging.warning(f"Пользователь {user_id} ввел текст, когда ожидался ввод для неизвестного типа: '{input_type}'.")
        return 

    # --- СУЩЕСТВУЮЩИЕ БЛОКИ ---
    if text == "📊 Получить отчет":
        context.user_data['action_type'] = 'get_full_report' 
        await ask_for_period(update, context) 
    elif text == "💸 Потрачено": 
        context.user_data['action_type'] = 'get_spend_summary' 
        await ask_for_period(update, context) 
    elif text == "📦 Кол. заказов": # <-- НОВЫЙ БЛОК ДЛЯ КНОПКИ "КОЛ-ВО ЗАКАЗОВ"
        await show_orders_management_menu(update, context) # Вызываем новое меню для заказов
    elif text == "⚙️ Настройки": 
        await show_settings_menu(update, context) 
    elif text == "🔔 Уведомления": 
        await show_notification_settings_menu(update, context) 
    elif text == "📊 Отчет по продажам": # <-- НОВЫЙ БЛОК ДЛЯ КНОПКИ "ОТЧЕТ ПО ПРОДАЖАМ"
        context.user_data['action_type'] = 'get_sales_report' # Устанавливаем флаг для отчета по продажам
        await ask_for_period(update, context) # Просим выбрать период
    else: 
        await context.bot.send_message(
            chat_id=update.effective_chat.id, 
            text="Неизвестная команда. Пожалуйста, используйте кнопки меню."
        )

async def ask_for_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in bot_settings['authorized_users']: 
        await context.bot.send_message(chat_id=user_id, text="Извините, у вас нет доступа к этому боту. Введите `/auth <ваш_код>` для авторизации.")
        return

    keyboard = [
        [InlineKeyboardButton("За сегодня", callback_data='period_today')],
        [InlineKeyboardButton("За вчера", callback_data='period_yesterday')],
        [InlineKeyboardButton("За 7 дней", callback_data='period_last_7d')],
        [InlineKeyboardButton("За 30 дней", callback_data='period_last_30d')],
        [InlineKeyboardButton("Текущий месяц", callback_data='period_this_month')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text('За какой период показать статистику?', reply_markup=reply_markup)
    logging.info(f"Пользователь {user_id} запросил выбор периода.")

    context.user_data['last_message_id'] = update.message.message_id
    context.user_data['chat_id'] = update.effective_chat.id

async def show_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in bot_settings['authorized_users']: 
        await context.bot.send_message(chat_id=user_id, text="Извините, у вас нет доступа к этому боту. Введите `/auth <ваш_код>` для авторизации.")
        return

    auto_reports_enabled = bot_settings.get('auto_reports_enabled', False)
    status_text = "Вкл" if auto_reports_enabled else "Выкл"

    keyboard = [
        [InlineKeyboardButton(f"Автоматические отчеты: {status_text}", callback_data='setting_toggle_auto_reports')],
        [InlineKeyboardButton("↩️ Назад", callback_data='setting_back_to_main_menu')] 
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.send_message(
        chat_id=update.effective_chat.id, 
        text="Выберите настройку:", 
        reply_markup=reply_markup
    )
    logging.info(f"Пользователь {user_id} открыл меню настроек.")

async def show_notification_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню настроек уведомлений."""
    user_id = update.effective_user.id
    if user_id not in bot_settings['authorized_users']: 
        await context.bot.send_message(chat_id=user_id, text="Извините, у вас нет доступа к этому боту. Введите `/auth <ваш_код>` для авторизации.")
        return

    notifications_enabled = bot_settings.get('notifications_enabled', False)
    notifications_status_text = "Вкл" if notifications_enabled else "Выкл"
    
    cost_per_conversation_threshold = bot_settings.get('cost_per_conversation_threshold', 2.0) 

    keyboard = [
        [InlineKeyboardButton(f"Статус уведомлений: {notifications_status_text}", callback_data='notification_setting_toggle_status')],
        [InlineKeyboardButton(f"Порог цены за переписку: ${cost_per_conversation_threshold:.2f}", callback_data='notification_setting_set_cost_per_conversation_threshold')],
        [InlineKeyboardButton("↩️ Назад в Настройки", callback_data='notification_setting_back_to_main_settings')], 
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.send_message(
        chat_id=update.effective_chat.id, 
        text="Настройки уведомлений (чтобы не пропустить ухудшения показателей):", 
        reply_markup=reply_markup
    )
    logging.info(f"Пользователь {user_id} открыл меню настроек уведомлений.")

async def show_orders_management_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает меню управления количеством заказов."""
    user_id = update.effective_user.id
    if user_id not in bot_settings['authorized_users']: 
        await context.bot.send_message(chat_id=user_id, text="Извините, у вас нет доступа к этому боту. Введите `/auth <ваш_код>` для авторизации.")
        return

    today_date = datetime.date.today().isoformat()
    orders_today = bot_settings['daily_orders'].get(today_date, 0)

    keyboard = [
        [InlineKeyboardButton("➕ Добавить заказ (+1)", callback_data='orders_action_increment_today')], # <-- ИЗМЕНЕНО: текст кнопки
        [InlineKeyboardButton(f"📈 Заказов сегодня: {orders_today}", callback_data='orders_action_show_today')],
        [InlineKeyboardButton("↩️ Назад", callback_data='orders_action_back_to_main_menu')] 
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await context.bot.send_message(
        chat_id=update.effective_chat.id, 
        text="Управление заказами:", 
        reply_markup=reply_markup
    )
    logging.info(f"Пользователь {user_id} открыл меню заказов.")


async def check_for_alerts(bot_instance, chat_id: int, ad_account_id: str, fb_access_token: str, fb_app_id: str, fb_app_secret: str):
    """
    Проверяет показатели рекламного аккаунта на соответствие заданным порогам
    и отправляет уведомления в Telegram.
    """
    logging.info(f"Запуск проверки алертов для аккаунта `{ad_account_id}`...")
    
    current_settings = load_settings()
    if chat_id not in current_settings['authorized_users'] or not current_settings.get('notifications_enabled', False):
        logging.info(f"Уведомления отключены или пользователь {chat_id} не авторизован. Проверка алертов пропущена.")
        return 

    threshold = current_settings.get('cost_per_conversation_threshold', 2.0)
    last_alert_time_str = current_settings.get('last_cost_per_conversation_alert')

    COOLDOWN_PERIOD_SECONDS = 3600 # 1 час

    try:
        FacebookAdsApi.init(app_id=fb_app_id, app_secret=fb_app_secret, access_token=fb_access_token)
        account = AdAccount(ad_account_id)

        insights_data = list(account.get_insights(
            params={'date_preset': 'today', 'fields': 'actions,cost_per_action_type'}
        ))
        
        current_cost_per_conversation = 0.0
        
        if insights_data and insights_data[0]:
            insights = insights_data[0]
            
            for cost_action in insights.get('cost_per_action_type', []):
                if cost_action.get('action_type') == 'onsite_conversion.total_messaging_connection':
                    current_cost_per_conversation = float(cost_action.get('value', '0'))
                    break

        logging.info(f"Аккаунт `{ad_account_id}`: Текущая цена за переписку: ${current_cost_per_conversation:.2f}, Порог: ${threshold:.2f}")

        if current_cost_per_conversation > threshold and current_cost_per_conversation > 0:
            send_alert = False
            if last_alert_time_str:
                last_alert_datetime = datetime.datetime.fromisoformat(last_alert_time_str)
                time_since_last_alert = (datetime.datetime.now() - last_alert_datetime).total_seconds()
                if time_since_last_alert > COOLDOWN_PERIOD_SECONDS:
                    send_alert = True
                else:
                    logging.info(f"Аккаунт `{ad_account_id}`: Алертинг по цене за переписку на кулдауне ({time_since_last_alert:.0f}s из {COOLDOWN_PERIOD_SECONDS}s).")
            else:
                send_alert = True 

            if send_alert:
                alert_message = (
                    f"⚠️ **ВНИМАНИЕ! Показатель ухудшился!**\n\n"
                    f"**Аккаунт:** `{ad_account_id}`\n"
                    f"**Метрика:** Цена за переписку\n"
                    f"  - **Текущее значение:** **${current_cost_per_conversation:.2f}**\n"
                    f"  - **Заданный порог:** **${threshold:.2f}**\n\n"
                    f"Проверьте ваши рекламные кампании!"
                )
                await bot_instance.send_message(chat_id=chat_id, text=alert_message, parse_mode='Markdown')
                logging.warning(f"ОТПРАВЛЕН АЛЕРТ: Цена за переписку превысила порог для аккаунта `{ad_account_id}`.")
                
                bot_settings['last_cost_per_conversation_alert'] = datetime.datetime.now().isoformat()
                save_settings(bot_settings)
        else:
            if last_alert_time_str:
                bot_settings['last_cost_per_conversation_alert'] = None
                save_settings(bot_settings)

    except Exception as e:
        logging.error(f"Ошибка в функции проверки алертов для аккаунта `{ad_account_id}`: {e}", exc_info=True)
        await bot_instance.send_message(chat_id=chat_id, text=f"**Ошибка при проверке алертов для аккаунта `{ad_account_id}`:**\n{e}")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer() 

    data = query.data 
    user_id = query.from_user.id
    chat_id = query.message.chat_id

    if user_id not in bot_settings['authorized_users']: 
        await query.answer("У вас нет доступа к боту. Используйте `/auth <ваш_код>` для авторизации.")
        await context.bot.send_message(chat_id=user_id, text="Извините, у вас нет доступа к этому боту. Введите `/auth <ваш_код>` для авторизации.")
        return

    # 1. Если пользователь выбрал период
    if data.startswith('period_'):
        period_type = data.replace('period_', '') 
        context.user_data['selected_period'] = period_type 
        
        if context.user_data.get('action_type') == 'get_spend_summary':
            await query.edit_message_text(text=f"Минутку, запрашиваю общие траты по аккаунту за '{period_type.replace('_', ' ').title()}' у Facebook...")
            logging.info(f"Пользователь {user_id} запросил общие траты по аккаунту за {period_type}.")
            try:
                FacebookAdsApi.init(app_id=FB_APP_ID, app_secret=FB_APP_SECRET, access_token=FB_ACCESS_TOKEN)
                account = AdAccount(AD_ACCOUNT_ID) 
                
                insights_data = list(account.get_insights(
                    params={'date_preset': period_type, 'fields': 'spend'}
                ))
                
                total_spend = "0"
                if insights_data and insights_data[0]:
                    total_spend = insights_data[0].get('spend', '0')
                
                report_text = f"💸 **Общие траты по аккаунту ({period_type.replace('_', ' ').title()}):**\n\n" \
                              f"  Всего потрачено: **${float(total_spend):.2f}**"
                
                await query.edit_message_text(text=report_text, parse_mode='Markdown')
                logging.info(f"Общие траты по аккаунту отправлены пользователю {user_id} за период {period_type}.")

            except FacebookRequestError as e:
                error_message = f"Ошибка Facebook API: Код: {e.api_error_code()} - Сообщение: {e.api_error_message()}"
                logging.error(f"Ошибка Facebook API для пользователя {user_id}: {error_message}", exc_info=True)
                await query.edit_message_text(text=error_message)
            except Exception as e:
                full_error = f"Что-то пошло не так: {e}"
                logging.error(f"Неизвестная ошибка: {full_error}", exc_info=True)
                await query.edit_message_text(text=f"Что-то пошло не так: {e}")
            
            context.user_data.pop('action_type', None) 
            return 

        # --- ОБРАБОТКА ЗАПРОСА "ОТЧЕТ ПО ПРОДАЖАМ" ---
        elif context.user_data.get('action_type') == 'get_sales_report':
            report_parts = await get_sales_report(period_type, AD_ACCOUNT_ID, FB_ACCESS_TOKEN, FB_APP_ID, FB_APP_SECRET)
            if report_parts:
                for part in report_parts:
                    await context.bot.send_message(chat_id=chat_id, text=part, parse_mode='Markdown')
                logging.info(f"Отчет по продажам отправлен пользователю {user_id} за период {period_type}.")
            else:
                await context.bot.send_message(chat_id=chat_id, text="Не удалось сгенерировать отчет по продажам. Возможно, нет данных.", parse_mode='Markdown')
            
            context.user_data.pop('action_type', None) 
            return 
        
        keyboard = [
            [InlineKeyboardButton("По кампаниям (подробно)", callback_data='level_campaigns')],
            [InlineKeyboardButton("По группам объявлений (подробно)", callback_data='level_adsets')], 
            [InlineKeyboardButton("По группам (кратко переписки)", callback_data='level_brief_adsets_messages')], 
            [InlineKeyboardButton("📊 Сравнить Вчера vs Сегодня", callback_data='level_compare_daily')], 
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text=f"Период: {period_type.replace('_', ' ').title()}. Теперь выберите уровень отчета:", 
            reply_markup=reply_markup
        )
        logging.info(f"Пользователь {user_id} выбрал период: {period_type}. Запрос уровня детализации.")

    elif data.startswith('level_'):
        level_type = data.replace('level_', '') 
        selected_period = context.user_data.get('selected_period')

        if level_type != 'compare_daily' and not selected_period:
            await context.bot.send_message(chat_id=chat_id, text="Ошибка: Период не выбран. Пожалуйста, начните заново, нажав '📊 Получить отчет'.")
            logging.error(f"Пользователь {user_id} попытался выбрать уровень без выбранного периода.")
            return

        await context.bot.send_message(chat_id=chat_id, text=f"Минутку, запрашиваю статистику по '{level_type.replace('_', ' ').title()}' у Facebook...")
        logging.info(f"Пользователь {user_id} запросил отчет по {level_type} за {selected_period if level_type != 'compare_daily' else 'сегодня/вчера'}.")

        try:
            FacebookAdsApi.init(app_id=FB_APP_ID, app_secret=FB_APP_SECRET, access_token=FB_ACCESS_TOKEN)
            account = AdAccount(AD_ACCOUNT_ID) 
            
            response_parts = [] 

            if level_type == 'campaigns':
                response_parts = await get_campaign_report(account, selected_period)
            elif level_type == 'adsets': 
                response_parts = await get_adset_report(account, selected_period)
            elif level_type == 'brief_adsets_messages': 
                response_parts = await get_brief_adset_report(account, selected_period)
            elif level_type == 'compare_daily': 
                response_parts = await get_daily_comparison_report(account, chat_id, context.bot) 
                
            else:
                response_parts = ["Неизвестный уровень отчета."]
                logging.warning(f"Неизвестный уровень отчета: {level_type} для пользователя {user_id}.")

            if response_parts:
                for part in response_parts:
                    if isinstance(part, str): 
                        await context.bot.send_message(chat_id=chat_id, text=part, parse_mode='Markdown')
                logging.info(f"Отчет успешно отправлен пользователю {user_id} за период {selected_period if level_type != 'compare_daily' else 'сегодня/вчера'}.")
            else:
                await context.bot.send_message(chat_id=chat_id, text="Не удалось сгенерировать отчет. Возможно, нет данных.", parse_mode='Markdown')


        except FacebookRequestError as e:
            error_message = f"Ошибка Facebook API: Код: {e.api_error_code()} - Сообщение: {e.api_error_message()}"
            logging.error(f"Ошибка Facebook API для пользователя {user_id}: {error_message}", exc_info=True)
            await context.bot.send_message(chat_id=chat_id, text=error_message) 
        except Exception as e:
            full_error = f"Что-то пошло не так: {e}"
            logging.error(f"Неизвестная ошибка: {full_error}", exc_info=True)
            await context.bot.send_message(chat_id=chat_id, text=f"Что-то пошло не так: {e}") 

    # 3. Если пользователь нажал кнопку настройки (общие настройки бота)
    elif data.startswith('setting_'):
        setting_type = data.replace('setting_', '')
        user_id = query.from_user.id
        chat_id = query.message.chat_id

        if setting_type == 'toggle_auto_reports':
            current_status = bot_settings.get('auto_reports_enabled', False) 
            new_status = not current_status
            bot_settings['auto_reports_enabled'] = new_status 
            save_settings(bot_settings) 

            if new_status:
                scheduler.add_job(
                    send_daily_auto_report,
                    CronTrigger(hour=bot_settings['report_time'].split(':')[0], minute=bot_settings['report_time'].split(':')[1]),
                    args=[context.bot, ADMIN_TELEGRAM_ID, FB_ACCESS_TOKEN, FB_APP_ID, FB_APP_SECRET, AD_ACCOUNT_ID], 
                    id='daily_auto_report',
                    replace_existing=True 
                )
                status_text = "Включены"
                logging.info(f"Автоматическая рассылка отчетов активирована. Время: {bot_settings['report_time']}")
            else:
                if scheduler.get_job('daily_auto_report'):
                    scheduler.remove_job('daily_auto_report')
                    logging.info("Автоматическая рассылка отчетов деактивирована.")
                status_text = "Выключены"
            
            await query.edit_message_text(f"Автоматические отчеты теперь: **{status_text}**.", parse_mode='Markdown')
            logging.info(f"Пользователь {user_id} переключил автоотчеты в статус: {status_text}.")
            await show_settings_menu(update, context) 
        
        elif setting_type == 'back_to_main_menu': 
            await context.bot.delete_message(chat_id=chat_id, message_id=query.message.message_id)
            keyboard = [[KeyboardButton("📊 Получить отчет")], 
                        [KeyboardButton("💸 Потрачено")], 
                        [KeyboardButton("📦 Кол. заказов")], 
                        [KeyboardButton("⚙️ Настройки")], 
                        [KeyboardButton("🔔 Уведомления")], 
                        [KeyboardButton("📊 Отчет по продажам")], 
                        ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await context.bot.send_message(chat_id=chat_id, text="Добро пожаловать в главное меню!", reply_markup=reply_markup)
            logging.info(f"Пользователь {user_id} вернулся в главное меню.")

        else:
            await query.edit_message_text("Неизвестная настройка.")
            logging.warning(f"Неизвестная настройка: {setting_type} для пользователя {user_id}.")

    # 4. Если пользователь нажал кнопку настройки уведомлений
    elif data.startswith('notification_setting_'):
        setting_type = data.replace('notification_setting_', '')
        user_id = query.from_user.id
        chat_id = query.message.chat_id

        if setting_type == 'toggle_status':
            current_status = bot_settings.get('notifications_enabled', False)
            new_status = not current_status
            bot_settings['notifications_enabled'] = new_status
            save_settings(bot_settings) 

            if new_status:
                scheduler.add_job(
                    check_for_alerts,
                    'interval', minutes=30, 
                    args=[context.bot, ADMIN_TELEGRAM_ID, AD_ACCOUNT_ID, FB_ACCESS_TOKEN, FB_APP_ID, FB_APP_SECRET],
                    id='alert_check_job',
                    replace_existing=True
                )
                status_text = "Включены"
                logging.info("Уведомления активированы. Задача проверки алертов добавлена.")
            else:
                if scheduler.get_job('alert_check_job'):
                    scheduler.remove_job('alert_check_job')
                    logging.info("Уведомления деактивированы. Задача проверки алертов удалена.")
                status_text = "Выключены"
            
            await query.edit_message_text(f"Уведомления теперь: **{status_text}**.", parse_mode='Markdown')
            logging.info(f"Пользователь {user_id} переключил уведомления в статус: {status_text}.")
            await show_notification_settings_menu(update, context) 

        elif setting_type == 'set_cost_per_conversation_threshold':
            await query.edit_message_text("Чтобы установить порог цены за переписку, отправьте мне число (например, '2.50').")
            context.user_data['awaiting_input_for'] = 'cost_per_conversation_threshold' 
            logging.info(f"Пользователь {user_id} начал настройку порога цены за переписку.")
            
        elif setting_type == 'back_to_main_settings': 
            await context.bot.delete_message(chat_id=chat_id, message_id=query.message.message_id)
            await show_settings_menu(update, context) 
            logging.info(f"Пользователь {user_id} вернулся в главное меню настроек из меню уведомлений.")

        else:
            await query.edit_message_text("Неизвестная настройка уведомлений.")
            logging.warning(f"Неизвестная настройка уведомлений: {setting_type} для пользователя {user_id}.")

    # 5. Если пользователь нажал кнопку управления заказами
    elif data.startswith('orders_action_'):
        action_type = data.replace('orders_action_', '')
        user_id = query.from_user.id
        chat_id = query.message.chat_id

        if action_type == 'increment_today': # Кнопка "➕ Добавить заказ (+1)"
            today_date = datetime.date.today().isoformat()
            bot_settings.setdefault('daily_orders', {})
            current_orders = bot_settings['daily_orders'].get(today_date, 0)
            bot_settings['daily_orders'][today_date] = current_orders + 1
            save_settings(bot_settings)
            
            # Обновляем сообщение с новым количеством заказов
            await query.edit_message_text(f"📦 Заказов на сегодня ({today_date}): **{bot_settings['daily_orders'][today_date]}**.")
            logging.info(f"Пользователь {user_id} добавил 1 заказ. Всего на {today_date}: {bot_settings['daily_orders'][today_date]}.")
            await show_orders_management_menu(update, context) # Вернуться в меню заказов
        elif action_type == 'show_today':
            today_date = datetime.date.today().isoformat()
            orders_today = bot_settings['daily_orders'].get(today_date, 0)
            await query.edit_message_text(f"📈 Заказов на сегодня ({today_date}): **{orders_today}**.")
            logging.info(f"Пользователь {user_id} запросил заказы на сегодня.")
            await show_orders_management_menu(update, context) 
        elif action_type == 'back_to_main_menu': 
            await context.bot.delete_message(chat_id=chat_id, message_id=query.message.message_id)
            keyboard = [[KeyboardButton("📊 Получить отчет")], 
                        [KeyboardButton("💸 Потрачено")], 
                        [KeyboardButton("📦 Кол. заказов")], 
                        [KeyboardButton("⚙️ Настройки")], 
                        [KeyboardButton("🔔 Уведомления")], 
                        [KeyboardButton("📊 Отчет по продажам")]] 
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await context.bot.send_message(chat_id=chat_id, text="Добро пожаловать в главное меню!", reply_markup=reply_markup)
            logging.info(f"Пользователь {user_id} вернулся в главное меню из меню заказов.")
        else:
            await query.edit_message_text("Неизвестное действие с заказами.")
            logging.warning(f"Неизвестное действие с заказами: {action_type} для пользователя {user_id}.")


# --- НОВАЯ ФУНКЦИЯ ДЛЯ АВТОРИЗАЦИИ ---
async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает команду /auth для авторизации пользователей по коду.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if user_id in bot_settings['authorized_users']:
        await context.bot.send_message(chat_id=chat_id, text="Вы уже авторизованы!")
        return

    if not context.args:
        await context.bot.send_message(chat_id=chat_id, text="Пожалуйста, укажите код авторизации. Пример: `/auth 0105`")
        return

    entered_code = context.args[0] 

    if entered_code == bot_settings['login_code']:
        bot_settings['authorized_users'].append(user_id)
        save_settings(bot_settings) 
        
        await context.bot.send_message(chat_id=chat_id, text="🎉 Авторизация пройдена! Теперь у вас есть доступ к боту.")
        logging.info(f"Пользователь {user_id} успешно авторизовался.")
        await start(update, context) 
    else:
        await context.bot.send_message(chat_id=chat_id, text="Неверный код авторизации.")
        logging.warning(f"Пользователь {user_id} ввел неверный код авторизации: '{entered_code}'.")

# --- КОНЕЦ НОВОЙ ФУНКЦИИ ДЛЯ АВТОРИЗАЦИИ ---


# Вспомогательная функция для разбивки текста на части (чтобы не превышать лимит Telegram)
def split_message(text: str, max_length: int = 4000) -> list[str]:
    """Разбивает длинный текст на части, не превышающие max_length."""
    parts = []
    current_part = ""
    lines = text.split('\n')
    
    for line in lines:
        if len(current_part) + len(line) + 1 > max_length: 
            parts.append(current_part.strip())
            current_part = ""
        current_part += line + '\n'
    
    if current_part: 
        parts.append(current_part.strip())
        
    return parts

async def send_daily_auto_report(bot_instance, chat_id: int, ad_account_id: str, fb_access_token: str, fb_app_id: str, fb_app_secret: str): 
    """
    Отправляет ежедневный автоматический отчет админу по одному аккаунту.
    Эта функция будет запускаться планировщиком.
    """
    logging.info(f"Запуск автоматической рассылки отчета для пользователя {chat_id} по аккаунту `{ad_account_id}`...")
    
    try:
        FacebookAdsApi.init(app_id=fb_app_id, app_secret=fb_app_secret, access_token=fb_access_token)
        account = AdAccount(ad_account_id) 
        
        report_parts = await get_campaign_report(account, 'today') 

        if report_parts:
            await bot_instance.send_message(chat_id=chat_id, text=f"📊 **Ежедневный автоматический отчет по аккаунту `{ad_account_id}`:**", parse_mode='Markdown')
            for part in report_parts:
                await bot_instance.send_message(chat_id=chat_id, text=part, parse_mode='Markdown')
            logging.info(f"Ежедневный автоматический отчет успешно отправлен пользователю {chat_id} для аккаунта `{ad_account_id}`.")
        else:
            await bot_instance.send_message(chat_id=chat_id, text=f"Не удалось сгенерировать ежедневный автоматический отчет для аккаунта `{ad_account_id}`. Возможно, нет данных.", parse_mode='Markdown')

    except Exception as e:
        logging.error(f"Ошибка при автоматической рассылке отчета пользователю {chat_id} для аккаунта `{ad_account_id}`: {e}", exc_info=True)
        await bot_instance.send_message(chat_id=chat_id, text=f"**Ошибка при отправке автоматического отчета:**\n{e}")

async def get_campaign_report(account: AdAccount, period: str) -> list[str]:
    """Формирует подробный отчет по кампаниям и возвращает список строк (частей сообщения)."""
    active_campaigns = account.get_campaigns(
        fields=[Campaign.Field.name, Campaign.Field.id], 
        params={'effective_status': ['ACTIVE']} 
    )

    if not active_campaigns:
        return [f"Активных кампаний за период '{period.replace('_', ' ').title()}' не найдено."]

    all_campaign_details = [] 
    
    for campaign in active_campaigns:
        campaign_id = campaign['id']
        campaign_name = campaign['name']

        insights_params = {
            'date_preset': period,
            'fields': 'spend,clicks,cpc,ctr,impressions,reach,conversions,frequency,actions,cost_per_action_type'
        }
        campaign_insights = list(campaign.get_insights(params=insights_params))
        insights = campaign_insights[0] if campaign_insights else {} 

        spend = insights.get('spend', '0')
        clicks = insights.get('clicks', '0')
        cpc = insights.get('cpc', '0')
        ctr = insights.get('ctr', '0')
        impressions = insights.get('impressions', '0')
        reach = insights.get('reach', '0')
        conversions = insights.get('conversions', '0') 
        frequency = insights.get('frequency', '0')

        messenger_sends = '0' 
        leads_generated = '0' 
        cost_per_messenger_send = '0'
        cost_per_lead = '0'

        actions_list = insights.get('actions', []) 
        for action in actions_list:
            action_type = action.get('action_type')
            value = str(action.get('value', '0')) 

            if action_type == 'onsite_conversion.total_messaging_connection':
                messenger_sends = value
            elif action_type == 'onsite_conversion.lead': 
                leads_generated = value
            
        cost_per_action_list = insights.get('cost_per_action_type', [])
        for cost_action in cost_per_action_list:
            action_type = cost_action.get('action_type')
            value = str(cost_action.get('value', '0')) 
            if action_type == 'onsite_conversion.total_messaging_connection':
                cost_per_messenger_send = value
            elif action_type == 'onsite_conversion.lead':
                cost_per_lead = value

        campaign_detail_text = (
            f"🌟 Кампания *{campaign_name}*:\n"
            f"  - 💸 Потрачено: **${float(spend):.2f}**\n" 
            f"  - 🖱 Клики: **{clicks}**\n"
            f"  - 💰 Цена за клик (CPC): **${float(cpc):.2f}**\n"
            f"  - 🎯 CTR: **${float(ctr):.2f}%**\n"
            f"  - 👀 Показы: **{impressions}**\n"
            f"  - 👤 Охват: **{reach}**\n"
            f"  - ✨ Конверсии (общие): **{conversions}**\n"
            f"  - 💬 Начато переписок: **{messenger_sends}**\n" 
            f"  - 💰 Цена за переписку: **${float(cost_per_messenger_send):.2f}**\n" 
            f"  - 📝 Лиды (с форм): **${float(cost_per_lead):.2f}**\n" 
            f"  - 🔁 Частота: **${float(frequency):.2f}**\n\n"
        )
        all_campaign_details.append(campaign_detail_text)
    
    header = f"📊 **Отчет по активным кампаниям ({period.replace('_', ' ').title()}):**\n\n"
    full_report_text = header + "".join(all_campaign_details)
    return split_message(full_report_text)


async def get_adset_report(account: AdAccount, period: str) -> list[str]:
    """Формирует подробный отчет по группам объявлений (Adsets) и возвращает список строк."""
    
    active_campaigns = account.get_campaigns(
        fields=[Campaign.Field.name, Campaign.Field.id],
        params={'effective_status': ['ACTIVE']}
    )

    if not active_campaigns:
        return ["Активных кампаний не найдено для получения групп объявлений."]

    all_adset_details = [] 
    
    found_adsets_in_period = False 
    for campaign in active_campaigns:
        campaign_name = campaign['name']
        
        adsets = campaign.get_ad_sets( 
            fields=[AdSet.Field.name, AdSet.Field.id],
            params={'effective_status': ['ACTIVE']}
        )
        
        for adset in adsets:
            adset_name = adset['name']
            
            insights_params = {
                'date_preset': period,
                'fields': 'spend,clicks,cpc,ctr,impressions,reach,conversions,frequency,actions,cost_per_action_type'
            }
            adset_insights = list(adset.get_insights(params=insights_params))
            insights = adset_insights[0] if adset_insights else {}

            if float(insights.get('spend', '0')) > 0 or float(insights.get('clicks', '0')) > 0:
                found_adsets_in_period = True
                
                spend = insights.get('spend', '0')
                clicks = insights.get('clicks', '0')
                cpc = insights.get('cpc', '0')
                ctr = insights.get('ctr', '0')
                impressions = insights.get('impressions', '0')
                reach = insights.get('reach', '0')
                conversions = insights.get('conversions', '0') 
                frequency = insights.get('frequency', '0')

                messenger_sends = '0' 
                leads_generated = '0' 
                cost_per_messenger_send = '0'
                cost_per_lead = '0'

                actions_list = insights.get('actions', []) 
                for action in actions_list:
                    action_type = action.get('action_type')
                    value = str(action.get('value', '0'))
                    if action_type == 'onsite_conversion.total_messaging_connection':
                        messenger_sends = value
                    elif action_type == 'onsite_conversion.lead': 
                        leads_generated = value

                cost_per_action_list = insights.get('cost_per_action_type', [])
                for cost_action in cost_per_action_list:
                    action_type = cost_action.get('action_type')
                    value = str(cost_action.get('value', '0'))
                    if action_type == 'onsite_conversion.total_messaging_connection':
                        cost_per_messenger_send = value
                    elif action_type == 'onsite_conversion.lead':
                        cost_per_lead = value

                adset_detail_text = (
                    f"🔥 Группа объявлений *{adset_name}* (Кампания: {campaign_name}):\n"
                    f"  - 💸 Потрачено: **${float(spend):.2f}**\n" 
                    f"  - 🖱 Клики: **{clicks}**\n"
                    f"  - 💰 Цена за клик (CPC): **${float(cpc):.2f}**\n"
                    f"  - 🎯 CTR: **${float(ctr):.2f}%**\n"
                    f"  - 👀 Показы: **{impressions}**\n"
                    f"  - 👤 Охват: **{reach}**\n"
                    f"  - ✨ Конверсии (общие): **{conversions}**\n"
                    f"  - 💬 Начато переписок: **{messenger_sends}**\n" 
                    f"  - 💰 Цена за переписку: **${float(cost_per_messenger_send):.2f}**\n" 
                    f"  - 📝 Лиды (с форм): **${float(cost_per_lead):.2f}**\n" 
                    f"  - 🔁 Частота: **${float(frequency):.2f}**\n\n"
                )
                all_adset_details.append(adset_detail_text)
    
    if not found_adsets_in_period:
        return [f"Активных групп объявлений с данными за период '{period.replace('_', ' ').title()}' не найдено."]

    header = f"📊 **Отчет по активным группам объявлений ({period.replace('_', ' ').title()}):**\n\n"
    full_report_text = header + "".join(all_adset_details)
    return split_message(full_report_text)


async def get_brief_adset_report(account: AdAccount, period: str) -> list[str]:
    """
    Формирует краткий отчет по группам объявлений (Adsets) с фокусом только на переписках и их стоимости.
    Возвращает список строк (частей сообщения).
    """
    active_campaigns = account.get_campaigns(
        fields=[Campaign.Field.name, Campaign.Field.id],
        params={'effective_status': ['ACTIVE']}
    )

    if not active_campaigns:
        return ["Активных кампаний не найдено для получения групп объявлений (для краткого отчета)."]

    all_brief_adset_details = [] 
    
    found_adsets_with_messages = False 
    for campaign in active_campaigns:
        campaign_name = campaign['name']
        
        adsets = campaign.get_ad_sets( 
            fields=[AdSet.Field.name, AdSet.Field.id],
            params={'effective_status': ['ACTIVE']}
        )
        
        for adset in adsets:
            adset_name = adset['name']
            
            # Запрашиваем только actions и cost_per_action_type для краткого отчета
            insights_params = {
                'date_preset': period,
                'fields': 'actions,cost_per_action_type' 
            }
            adset_insights = list(adset.get_insights(params=insights_params))
            insights = adset_insights[0] if adset_insights else {}

            messenger_sends = '0' 
            cost_per_messenger_send = '0'
            
            actions_list = insights.get('actions', []) 
            for action in actions_list:
                action_type = action.get('action_type')
                value = str(action.get('value', '0'))
                if action_type == 'onsite_conversion.total_messaging_connection':
                    messenger_sends = value
                # Если у тебя есть другие action_type для переписок, добавь их сюда и суммируй

            cost_per_action_list = insights.get('cost_per_action_type', [])
            for cost_action in cost_per_action_list:
                action_type = cost_action.get('action_type')
                value = str(cost_action.get('value', '0'))
                if action_type == 'onsite_conversion.total_messaging_connection':
                    cost_per_messenger_send = value
                # Если у тебя есть другие action_type для переписок, добавь их сюда и усредняй или выбирай основную

            # Добавляем adset в отчет только если есть сообщения
            if float(messenger_sends) > 0: 
                found_adsets_with_messages = True
                
                brief_adset_detail_text = (
                    f"💬 Группа *{adset_name}* (Кампания: {campaign_name}):\n"
                    f"  - Начато переписок: **{messenger_sends}**\n" 
                    f"  - Цена за переписку: **${float(cost_per_messenger_send):.2f}**\n\n" 
                )
                all_brief_adset_details.append(brief_adset_detail_text)
    
    if not found_adsets_with_messages:
        return [f"Активных групп объявлений с переписками за период '{period.replace('_', ' ').title()}' не найдено."]

    header = f"📊 **Краткий отчет по группам объявлений (Переписки, {period.replace('_', ' ').title()}):**\n\n"
    full_report_text = header + "".join(all_brief_adset_details)
    return split_message(full_report_text)


async def get_daily_comparison_report(account: AdAccount, chat_id: int, bot) -> list[str]:
    """
    Формирует краткий отчет-сравнение "Вчера vs Сегодня" для ключевых метрик (потрачено, переписки, цена).
    Также генерирует и отправляет диаграмму.
    Возвращает список строк (частей сообщения), исключая изображения, которые отправляются напрямую.
    """
    report_lines = []
    
    # 1. Запрос данных за сегодня и вчера
    insights_today = {}
    insights_yesterday = {}

    try:
        # Получаем данные за сегодня (уровень аккаунта, не кампаний)
        today_insights_data = list(account.get_insights(
            params={'date_preset': 'today', 'fields': 'spend,actions,cost_per_action_type'}
        ))
        if today_insights_data:
            insights_today = today_insights_data[0]

        # Получаем данные за вчера (уровень аккаунта)
        yesterday_insights_data = list(account.get_insights(
            params={'date_preset': 'yesterday', 'fields': 'spend,actions,cost_per_action_type'}
        ))
        if yesterday_insights_data:
            insights_yesterday = yesterday_insights_data[0]

    except Exception as e:
        logging.error(f"Ошибка при получении данных для сравнения: {e}", exc_info=True)
        return ["Ошибка при получении данных для сравнения. Пожалуйста, попробуйте позже."]

    # 2. Извлечение метрик (для сегодня и вчера)
    # Сегодня
    spend_today = float(insights_today.get('spend', '0'))
    messenger_sends_today = '0'
    cost_per_messenger_send_today = '0'

    for action in insights_today.get('actions', []):
        if action.get('action_type') == 'onsite_conversion.total_messaging_connection':
            messenger_sends_today = str(action.get('value', '0'))
            break 
    for cost_action in insights_today.get('cost_per_action_type', []):
        if cost_action.get('action_type') == 'onsite_conversion.total_messaging_connection':
            cost_per_messenger_send_today = str(cost_action.get('value', '0'))
            break
    
    # Вчера
    spend_yesterday = float(insights_yesterday.get('spend', '0'))
    messenger_sends_yesterday = '0'
    cost_per_messenger_send_yesterday = '0'

    for action in insights_yesterday.get('actions', []):
        if action.get('action_type') == 'onsite_conversion.total_messaging_connection':
            messenger_sends_yesterday = str(action.get('value', '0'))
            break
    for cost_action in insights_yesterday.get('cost_per_action_type', []):
        if cost_action.get('action_type') == 'onsite_conversion.total_messaging_connection':
            cost_per_messenger_send_yesterday = str(cost_action.get('value', '0'))
            break

    # 3. Расчет разницы и формирование текста отчета
    report_lines.append("📊 **Сравнение показателей: Сегодня vs Вчера (по аккаунту)**\n\n")

    # Потрачено
    # Используем 1e-9 (очень маленькое число) вместо 0 для деления, чтобы избежать ошибок, но при этом получить корректный процент
    spend_diff_percent = ((spend_today - spend_yesterday) / (spend_yesterday or 1e-9) * 100) if spend_today > 0 or spend_yesterday > 0 else 0
    report_lines.append(f"💸 Потрачено:\n  - Сегодня: **${spend_today:.2f}**\n  - Вчера: **${spend_yesterday:.2f}**\n  - Изменение: {spend_diff_percent:.2f}% {'⬆️' if spend_diff_percent > 0 else ('⬇️' if spend_diff_percent < 0 else '↔️')}\n\n")

    # Количество переписок
    sends_today_val = float(messenger_sends_today)
    sends_yesterday_val = float(messenger_sends_yesterday)
    sends_diff_percent = ((sends_today_val - sends_yesterday_val) / (sends_yesterday_val or 1e-9) * 100) if sends_today_val > 0 or sends_yesterday_val > 0 else 0
    report_lines.append(f"💬 Начато переписок:\n  - Сегодня: **{int(sends_today_val)}**\n  - Вчера: **{int(sends_yesterday_val)}**\n  - Изменение: {sends_diff_percent:.2f}% {'⬆️' if sends_diff_percent > 0 else ('⬇️' if sends_diff_percent < 0 else '↔️')}\n\n")

    # Цена за переписку (CPC на переписки)
    cpm_today_val = float(cost_per_messenger_send_today)
    cpm_yesterday_val = float(cost_per_messenger_send_yesterday)
    cpm_diff_percent = ((cpm_today_val - cpm_yesterday_val) / (cpm_yesterday_val or 1e-9) * 100) if cpm_today_val > 0 or cpm_yesterday_val > 0 else 0
    # Для цены стрелки инвертированы (меньшая цена - лучше)
    report_lines.append(f"💰 Цена за переписку:\n  - Сегодня: **${cpm_today_val:.2f}**\n  - Вчера: **${cpm_yesterday_val:.2f}**\n  - Изменение: {cpm_diff_percent:.2f}% {'⬇️' if cpm_diff_percent > 0 else ('⬆️' if cpm_diff_percent < 0 else '↔️')}\n\n") 

    # 4. Генерация диаграммы
    try:
        if spend_today + spend_yesterday > 0 or sends_today_val + sends_yesterday_val > 0:
            labels = ['Вчера', 'Сегодня']
            bar_width = 0.5 # Толщина полосы

            # --- График трат ---
            fig1, ax1 = plt.subplots(figsize=(6, 3)) # Уменьшил высоту для тонких полос
            spends = [spend_yesterday, spend_today]
            
            # Горизонтальная гистограмма
            y_pos_spends = range(len(labels))
            ax1.barh(y_pos_spends, spends, height=bar_width, color=['lightcoral', 'skyblue'])
            ax1.set_yticks(y_pos_spends)
            ax1.set_yticklabels(labels)
            ax1.set_title('Траты')
            ax1.set_xlabel('Сумма ($)')
            ax1.invert_yaxis() # Сверху "Вчера", снизу "Сегодня"
            
            # Добавляем значения на полосы
            for i, v in enumerate(spends):
                ax1.text(v + (max(spends)*0.05 if max(spends) > 0 else 1), i, f'${v:.2f}', va='center', ha='left', fontsize=9)
            
            plt.tight_layout()
            
            buf1 = io.BytesIO()
            plt.savefig(buf1, format='png', bbox_inches='tight')
            buf1.seek(0)
            plt.close(fig1) 

            await bot.send_photo(chat_id=chat_id, photo=buf1, caption="Динамика трат по аккаунту:")
            
            # --- График количества переписок ---
            fig2, ax2 = plt.subplots(figsize=(6, 3)) # Уменьшил высоту
            sends_counts = [sends_yesterday_val, sends_today_val]
            
            # Горизонтальная гистограмма
            y_pos_sends = range(len(labels))
            ax2.barh(y_pos_sends, sends_counts, height=bar_width, color=['lightcoral', 'skyblue'])
            ax2.set_yticks(y_pos_sends)
            ax2.set_yticklabels(labels)
            ax2.set_title('Количество переписок')
            ax2.set_xlabel('Количество')
            ax2.invert_yaxis() # Сверху "Вчера", снизу "Сегодня"
            
            # Добавляем значения на полосы
            for i, v in enumerate(sends_counts):
                ax2.text(v + (max(sends_counts)*0.05 if max(sends_counts) > 0 else 1), i, f'{int(v)}', va='center', ha='left', fontsize=9)
            
            plt.tight_layout()

            buf2 = io.BytesIO()
            plt.savefig(buf2, format='png', bbox_inches='tight')
            buf2.seek(0)
            plt.close(fig2)

            await bot.send_photo(chat_id=chat_id, photo=buf2, caption="Динамика переписок по аккаунту:")

        else:
            report_lines.append("Нет достаточных данных для построения диаграмм за последние два дня.")

    except Exception as e:
        logging.error(f"Ошибка при генерации диаграммы: {e}", exc_info=True)
        report_lines.append("Возникла ошибка при попытке построения диаграмм.")

    return split_message("".join(report_lines))


# Новая вспомогательная функция для безопасного запуска планировщика
async def start_scheduler_safely(context: ContextTypes.DEFAULT_TYPE):
    """
    Запускает APScheduler и добавляет задачи, если они включены,
    после того как Telegram-бот полностью инициализируется.
    """
    logging.info("Функция start_scheduler_safely запущена.")

    if not scheduler.running:
        scheduler.start()
        logging.info("Планировщик APScheduler успешно запущен через job_queue.")

    # Re-add the daily report job if auto-reports are enabled in settings
    if bot_settings['auto_reports_enabled']:
        if 'daily_auto_report' not in [job.id for job in scheduler.get_jobs()]: 
            scheduler.add_job(
                send_daily_auto_report,
                CronTrigger(hour=bot_settings['report_time'].split(':')[0], minute=bot_settings['report_time'].split(':')[1]),
                args=[context.bot, ADMIN_TELEGRAM_ID, FB_ACCESS_TOKEN, FB_APP_ID, FB_APP_SECRET, AD_ACCOUNT_ID], 
                id='daily_auto_report',
                replace_existing=True 
            )
            logging.info(f"Задача автоматической рассылки отчетов добавлена/обновлена через job_queue. Время: {bot_settings['report_time']}")
    else:
        if scheduler.get_job('daily_auto_report'): 
            scheduler.remove_job('daily_auto_report')
            logging.info("Автоматическая рассылка отчетов деактивирована при старте.")


# --- НОВАЯ ФУНКЦИЯ ДЛЯ АВТОРИЗАЦИИ ---
async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает команду /auth для авторизации пользователей по коду.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if user_id in bot_settings['authorized_users']:
        await context.bot.send_message(chat_id=chat_id, text="Вы уже авторизованы!")
        return

    if not context.args:
        await context.bot.send_message(chat_id=chat_id, text="Пожалуйста, укажите код авторизации. Пример: `/auth 0105`")
        return

    entered_code = context.args[0] 

    if entered_code == bot_settings['login_code']:
        bot_settings['authorized_users'].append(user_id)
        save_settings(bot_settings) 
        
        await context.bot.send_message(chat_id=chat_id, text="🎉 Авторизация пройдена! Теперь у вас есть доступ к боту.")
        logging.info(f"Пользователь {user_id} успешно авторизовался.")
        await start(update, context) 
    else:
        await context.bot.send_message(chat_id=chat_id, text="Неверный код авторизации.")
        logging.warning(f"Пользователь {user_id} ввел неверный код авторизации: '{entered_code}'.")

# --- КОНЕЦ НОВОЙ ФУНКЦИИ ДЛЯ АВТОРИЗАЦИИ ---


# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ СОХРАНЕНИЯ ЗАКАЗОВ ---
def save_orders_for_date(date_str: str, count: int):
    """
    Сохраняет количество заказов для определенной даты в bot_settings.
    """
    bot_settings['daily_orders'][date_str] = count
    save_settings(bot_settings)
    logging.info(f"Заказы за {date_str} сохранены: {count}")
# --- КОНЕЦ ВСПОМОГАТЕЛЬНОЙ ФУНКЦИИ ---


# Вспомогательная функция для разбивки текста на части (чтобы не превышать лимит Telegram)
def split_message(text: str, max_length: int = 4000) -> list[str]:
    """Разбивает длинный текст на части, не превышающие max_length."""
    parts = []
    current_part = ""
    lines = text.split('\n')
    
    for line in lines:
        if len(current_part) + len(line) + 1 > max_length: 
            parts.append(current_part.strip())
            current_part = ""
        current_part += line + '\n'
    
    if current_part: 
        parts.append(current_part.strip())
        
    return parts

async def send_daily_auto_report(bot_instance, chat_id: int, ad_account_id: str, fb_access_token: str, fb_app_id: str, fb_app_secret: str): 
    """
    Отправляет ежедневный автоматический отчет админу по одному аккаунту.
    Эта функция будет запускаться планировщиком.
    """
    logging.info(f"Запуск автоматической рассылки отчета для пользователя {chat_id} по аккаунту `{ad_account_id}`...")
    
    try:
        FacebookAdsApi.init(app_id=fb_app_id, app_secret=fb_app_secret, access_token=fb_access_token)
        account = AdAccount(ad_account_id) 
        
        report_parts = await get_campaign_report(account, 'today') 

        if report_parts:
            await bot_instance.send_message(chat_id=chat_id, text=f"📊 **Ежедневный автоматический отчет по аккаунту `{ad_account_id}`:**", parse_mode='Markdown')
            for part in report_parts:
                await bot_instance.send_message(chat_id=chat_id, text=part, parse_mode='Markdown')
            logging.info(f"Ежедневный автоматический отчет успешно отправлен пользователю {chat_id} для аккаунта `{ad_account_id}`.")
        else:
            await bot_instance.send_message(chat_id=chat_id, text=f"Не удалось сгенерировать ежедневный автоматический отчет для аккаунта `{ad_account_id}`. Возможно, нет данных.", parse_mode='Markdown')

    except Exception as e:
        logging.error(f"Ошибка при автоматической рассылке отчета пользователю {chat_id} для аккаунта `{ad_account_id}`: {e}", exc_info=True)
        await bot_instance.send_message(chat_id=chat_id, text=f"**Ошибка при отправке автоматического отчета:**\n{e}")

async def get_campaign_report(account: AdAccount, period: str) -> list[str]:
    """Формирует подробный отчет по кампаниям и возвращает список строк (частей сообщения)."""
    active_campaigns = account.get_campaigns(
        fields=[Campaign.Field.name, Campaign.Field.id], 
        params={'effective_status': ['ACTIVE']} 
    )

    if not active_campaigns:
        return [f"Активных кампаний за период '{period.replace('_', ' ').title()}' не найдено."]

    all_campaign_details = [] 
    
    for campaign in active_campaigns:
        campaign_id = campaign['id']
        campaign_name = campaign['name']

        insights_params = {
            'date_preset': period,
            'fields': 'spend,clicks,cpc,ctr,impressions,reach,conversions,frequency,actions,cost_per_action_type'
        }
        campaign_insights = list(campaign.get_insights(params=insights_params))
        insights = campaign_insights[0] if campaign_insights else {} 

        spend = insights.get('spend', '0')
        clicks = insights.get('clicks', '0')
        cpc = insights.get('cpc', '0')
        ctr = insights.get('ctr', '0')
        impressions = insights.get('impressions', '0')
        reach = insights.get('reach', '0')
        conversions = insights.get('conversions', '0') 
        frequency = insights.get('frequency', '0')

        messenger_sends = '0' 
        leads_generated = '0' 
        cost_per_messenger_send = '0'
        cost_per_lead = '0'

        actions_list = insights.get('actions', []) 
        for action in actions_list:
            action_type = action.get('action_type')
            value = str(action.get('value', '0')) 

            if action_type == 'onsite_conversion.total_messaging_connection':
                messenger_sends = value
            elif action_type == 'onsite_conversion.lead': 
                leads_generated = value
            
        cost_per_action_list = insights.get('cost_per_action_type', [])
        for cost_action in cost_per_action_list:
            action_type = cost_action.get('action_type')
            value = str(cost_action.get('value', '0')) 
            if action_type == 'onsite_conversion.total_messaging_connection':
                cost_per_messenger_send = value
            elif action_type == 'onsite_conversion.lead':
                cost_per_lead = value

        campaign_detail_text = (
            f"🌟 Кампания *{campaign_name}*:\n"
            f"  - 💸 Потрачено: **${float(spend):.2f}**\n" 
            f"  - 🖱 Клики: **{clicks}**\n"
            f"  - 💰 Цена за клик (CPC): **${float(cpc):.2f}**\n"
            f"  - 🎯 CTR: **${float(ctr):.2f}%**\n"
            f"  - 👀 Показы: **{impressions}**\n"
            f"  - 👤 Охват: **{reach}**\n"
            f"  - ✨ Конверсии (общие): **{conversions}**\n"
            f"  - 💬 Начато переписок: **{messenger_sends}**\n" 
            f"  - 💰 Цена за переписку: **${float(cost_per_messenger_send):.2f}**\n" 
            f"  - 📝 Лиды (с форм): **${float(cost_per_lead):.2f}**\n" 
            f"  - 🔁 Частота: **${float(frequency):.2f}**\n\n"
        )
        all_campaign_details.append(campaign_detail_text)
    
    header = f"📊 **Отчет по активным кампаниям ({period.replace('_', ' ').title()}):**\n\n"
    full_report_text = header + "".join(all_campaign_details)
    return split_message(full_report_text)


async def get_adset_report(account: AdAccount, period: str) -> list[str]:
    """Формирует подробный отчет по группам объявлений (Adsets) и возвращает список строк."""
    
    active_campaigns = account.get_campaigns(
        fields=[Campaign.Field.name, Campaign.Field.id],
        params={'effective_status': ['ACTIVE']}
    )

    if not active_campaigns:
        return ["Активных кампаний не найдено для получения групп объявлений."]

    all_adset_details = [] 
    
    found_adsets_in_period = False 
    for campaign in active_campaigns:
        campaign_name = campaign['name']
        
        adsets = campaign.get_ad_sets( 
            fields=[AdSet.Field.name, AdSet.Field.id],
            params={'effective_status': ['ACTIVE']}
        )
        
        for adset in adsets:
            adset_name = adset['name']
            
            insights_params = {
                'date_preset': period,
                'fields': 'spend,clicks,cpc,ctr,impressions,reach,conversions,frequency,actions,cost_per_action_type'
            }
            adset_insights = list(adset.get_insights(params=insights_params))
            insights = adset_insights[0] if adset_insights else {}

            if float(insights.get('spend', '0')) > 0 or float(insights.get('clicks', '0')) > 0:
                found_adsets_in_period = True
                
                spend = insights.get('spend', '0')
                clicks = insights.get('clicks', '0')
                cpc = insights.get('cpc', '0')
                ctr = insights.get('ctr', '0')
                impressions = insights.get('impressions', '0')
                reach = insights.get('reach', '0')
                conversions = insights.get('conversions', '0') 
                frequency = insights.get('frequency', '0')

                messenger_sends = '0' 
                leads_generated = '0' 
                cost_per_messenger_send = '0'
                cost_per_lead = '0'

                actions_list = insights.get('actions', []) 
                for action in actions_list:
                    action_type = action.get('action_type')
                    value = str(action.get('value', '0'))
                    if action_type == 'onsite_conversion.total_messaging_connection':
                        messenger_sends = value
                    elif action_type == 'onsite_conversion.lead': 
                        leads_generated = value

                cost_per_action_list = insights.get('cost_per_action_type', [])
                for cost_action in cost_per_action_list:
                    action_type = cost_action.get('action_type')
                    value = str(cost_action.get('value', '0'))
                    if action_type == 'onsite_conversion.total_messaging_connection':
                        cost_per_messenger_send = value
                    elif action_type == 'onsite_conversion.lead':
                        cost_per_lead = value

                adset_detail_text = (
                    f"🔥 Группа объявлений *{adset_name}* (Кампания: {campaign_name}):\n"
                    f"  - 💸 Потрачено: **${float(spend):.2f}**\n" 
                    f"  - 🖱 Клики: **{clicks}**\n"
                    f"  - 💰 Цена за клик (CPC): **${float(cpc):.2f}**\n"
                    f"  - 🎯 CTR: **${float(ctr):.2f}%**\n"
                    f"  - 👀 Показы: **{impressions}**\n"
                    f"  - 👤 Охват: **{reach}**\n"
                    f"  - ✨ Конверсии (общие): **{conversions}**\n"
                    f"  - 💬 Начато переписок: **{messenger_sends}**\n" 
                    f"  - 💰 Цена за переписку: **${float(cost_per_messenger_send):.2f}**\n" 
                    f"  - 📝 Лиды (с форм): **${float(cost_per_lead):.2f}**\n" 
                    f"  - 🔁 Частота: **${float(frequency):.2f}**\n\n"
                )
                all_adset_details.append(adset_detail_text)
    
    if not found_adsets_in_period:
        return [f"Активных групп объявлений с данными за период '{period.replace('_', ' ').title()}' не найдено."]

    header = f"📊 **Отчет по активным группам объявлений ({period.replace('_', ' ').title()}):**\n\n"
    full_report_text = header + "".join(all_adset_details)
    return split_message(full_report_text)


async def get_brief_adset_report(account: AdAccount, period: str) -> list[str]:
    """
    Формирует краткий отчет по группам объявлений (Adsets) с фокусом только на переписках и их стоимости.
    Возвращает список строк (частей сообщения).
    """
    active_campaigns = account.get_campaigns(
        fields=[Campaign.Field.name, Campaign.Field.id],
        params={'effective_status': ['ACTIVE']}
    )

    if not active_campaigns:
        return ["Активных кампаний не найдено для получения групп объявлений (для краткого отчета)."]

    all_brief_adset_details = [] 
    
    found_adsets_with_messages = False 
    for campaign in active_campaigns:
        campaign_name = campaign['name']
        
        adsets = campaign.get_ad_sets( 
            fields=[AdSet.Field.name, AdSet.Field.id],
            params={'effective_status': ['ACTIVE']}
        )
        
        for adset in adsets:
            adset_name = adset['name']
            
            # Запрашиваем только actions и cost_per_action_type для краткого отчета
            insights_params = {
                'date_preset': period,
                'fields': 'actions,cost_per_action_type' 
            }
            adset_insights = list(adset.get_insights(params=insights_params))
            insights = adset_insights[0] if adset_insights else {}

            messenger_sends = '0' 
            cost_per_messenger_send = '0'
            
            actions_list = insights.get('actions', []) 
            for action in actions_list:
                action_type = action.get('action_type')
                value = str(action.get('value', '0'))
                if action_type == 'onsite_conversion.total_messaging_connection':
                    messenger_sends = value
                # Если у тебя есть другие action_type для переписок, добавь их сюда и суммируй

            cost_per_action_list = insights.get('cost_per_action_type', [])
            for cost_action in cost_per_action_list:
                action_type = cost_action.get('action_type')
                value = str(cost_action.get('value', '0'))
                if action_type == 'onsite_conversion.total_messaging_connection':
                    cost_per_messenger_send = value
                # Если у тебя есть другие action_type для переписок, добавь их сюда и усредняй или выбирай основную

            # Добавляем adset в отчет только если есть сообщения
            if float(messenger_sends) > 0: 
                found_adsets_with_messages = True
                
                brief_adset_detail_text = (
                    f"💬 Группа *{adset_name}* (Кампания: {campaign_name}):\n"
                    f"  - Начато переписок: **{messenger_sends}**\n" 
                    f"  - Цена за переписку: **${float(cost_per_messenger_send):.2f}**\n\n" 
                )
                all_brief_adset_details.append(brief_adset_detail_text)
    
    if not found_adsets_with_messages:
        return [f"Активных групп объявлений с переписками за период '{period.replace('_', ' ').title()}' не найдено."]

    header = f"📊 **Краткий отчет по группам объявлений (Переписки, {period.replace('_', ' ').title()}):**\n\n"
    full_report_text = header + "".join(all_brief_adset_details)
    return split_message(full_report_text)


async def get_daily_comparison_report(account: AdAccount, chat_id: int, bot) -> list[str]:
    """
    Формирует краткий отчет-сравнение "Вчера vs Сегодня" для ключевых метрик (потрачено, переписки, цена).
    Также генерирует и отправляет диаграмму.
    Возвращает список строк (частей сообщения), исключая изображения, которые отправляются напрямую.
    """
    report_lines = []
    
    # 1. Запрос данных за сегодня и вчера
    insights_today = {}
    insights_yesterday = {}

    try:
        # Получаем данные за сегодня (уровень аккаунта, не кампаний)
        today_insights_data = list(account.get_insights(
            params={'date_preset': 'today', 'fields': 'spend,actions,cost_per_action_type'}
        ))
        if today_insights_data:
            insights_today = today_insights_data[0]

        # Получаем данные за вчера (уровень аккаунта)
        yesterday_insights_data = list(account.get_insights(
            params={'date_preset': 'yesterday', 'fields': 'spend,actions,cost_per_action_type'}
        ))
        if yesterday_insights_data:
            insights_yesterday = yesterday_insights_data[0]

    except Exception as e:
        logging.error(f"Ошибка при получении данных для сравнения: {e}", exc_info=True)
        return ["Ошибка при получении данных для сравнения. Пожалуйста, попробуйте позже."]

    # 2. Извлечение метрик (для сегодня и вчера)
    # Сегодня
    spend_today = float(insights_today.get('spend', '0'))
    messenger_sends_today = '0'
    cost_per_messenger_send_today = '0'

    for action in insights_today.get('actions', []):
        if action.get('action_type') == 'onsite_conversion.total_messaging_connection':
            messenger_sends_today = str(action.get('value', '0'))
            break 
    for cost_action in insights_today.get('cost_per_action_type', []):
        if cost_action.get('action_type') == 'onsite_conversion.total_messaging_connection':
            cost_per_messenger_send_today = str(cost_action.get('value', '0'))
            break
    
    # Вчера
    spend_yesterday = float(insights_yesterday.get('spend', '0'))
    messenger_sends_yesterday = '0'
    cost_per_messenger_send_yesterday = '0'

    for action in insights_yesterday.get('actions', []):
        if action.get('action_type') == 'onsite_conversion.total_messaging_connection':
            messenger_sends_yesterday = str(action.get('value', '0'))
            break
    for cost_action in insights_yesterday.get('cost_per_action_type', []):
        if cost_action.get('action_type') == 'onsite_conversion.total_messaging_connection':
            cost_per_messenger_send_yesterday = str(cost_action.get('value', '0'))
            break

    # 3. Расчет разницы и формирование текста отчета
    report_lines.append("📊 **Сравнение показателей: Сегодня vs Вчера (по аккаунту)**\n\n")

    # Потрачено
    # Используем 1e-9 (очень маленькое число) вместо 0 для деления, чтобы избежать ошибок, но при этом получить корректный процент
    spend_diff_percent = ((spend_today - spend_yesterday) / (spend_yesterday or 1e-9) * 100) if spend_today > 0 or spend_yesterday > 0 else 0
    report_lines.append(f"💸 Потрачено:\n  - Сегодня: **${spend_today:.2f}**\n  - Вчера: **${spend_yesterday:.2f}**\n  - Изменение: {spend_diff_percent:.2f}% {'⬆️' if spend_diff_percent > 0 else ('⬇️' if spend_diff_percent < 0 else '↔️')}\n\n")

    # Количество переписок
    sends_today_val = float(messenger_sends_today)
    sends_yesterday_val = float(messenger_sends_yesterday)
    sends_diff_percent = ((sends_today_val - sends_yesterday_val) / (sends_yesterday_val or 1e-9) * 100) if sends_today_val > 0 or sends_yesterday_val > 0 else 0
    report_lines.append(f"💬 Начато переписок:\n  - Сегодня: **{int(sends_today_val)}**\n  - Вчера: **{int(sends_yesterday_val)}**\n  - Изменение: {sends_diff_percent:.2f}% {'⬆️' if sends_diff_percent > 0 else ('⬇️' if sends_diff_percent < 0 else '↔️')}\n\n")

    # Цена за переписку (CPC на переписки)
    cpm_today_val = float(cost_per_messenger_send_today)
    cpm_yesterday_val = float(cost_per_messenger_send_yesterday)
    cpm_diff_percent = ((cpm_today_val - cpm_yesterday_val) / (cpm_yesterday_val or 1e-9) * 100) if cpm_today_val > 0 or cpm_yesterday_val > 0 else 0
    # Для цены стрелки инвертированы (меньшая цена - лучше)
    report_lines.append(f"💰 Цена за переписку:\n  - Сегодня: **${cpm_today_val:.2f}**\n  - Вчера: **${cpm_yesterday_val:.2f}**\n  - Изменение: {cpm_diff_percent:.2f}% {'⬇️' if cpm_diff_percent > 0 else ('⬆️' if cpm_diff_percent < 0 else '↔️')}\n\n") 

    # 4. Генерация диаграммы
    try:
        if spend_today + spend_yesterday > 0 or sends_today_val + sends_yesterday_val > 0:
            labels = ['Вчера', 'Сегодня']
            bar_width = 0.5 # Толщина полосы

            # --- График трат ---
            fig1, ax1 = plt.subplots(figsize=(6, 3)) # Уменьшил высоту для тонких полос
            spends = [spend_yesterday, spend_today]
            
            # Горизонтальная гистограмма
            y_pos_spends = range(len(labels))
            ax1.barh(y_pos_spends, spends, height=bar_width, color=['lightcoral', 'skyblue'])
            ax1.set_yticks(y_pos_spends)
            ax1.set_yticklabels(labels)
            ax1.set_title('Траты')
            ax1.set_xlabel('Сумма ($)')
            ax1.invert_yaxis() # Сверху "Вчера", снизу "Сегодня"
            
            # Добавляем значения на полосы
            for i, v in enumerate(spends):
                ax1.text(v + (max(spends)*0.05 if max(spends) > 0 else 1), i, f'${v:.2f}', va='center', ha='left', fontsize=9)
            
            plt.tight_layout()
            
            buf1 = io.BytesIO()
            plt.savefig(buf1, format='png', bbox_inches='tight')
            buf1.seek(0)
            plt.close(fig1) 

            await bot.send_photo(chat_id=chat_id, photo=buf1, caption="Динамика трат по аккаунту:")
            
            # --- График количества переписок ---
            fig2, ax2 = plt.subplots(figsize=(6, 3)) # Уменьшил высоту
            sends_counts = [sends_yesterday_val, sends_today_val]
            
            # Горизонтальная гистограмма
            y_pos_sends = range(len(labels))
            ax2.barh(y_pos_sends, sends_counts, height=bar_width, color=['lightcoral', 'skyblue'])
            ax2.set_yticks(y_pos_sends)
            ax2.set_yticklabels(labels)
            ax2.set_title('Количество переписок')
            ax2.set_xlabel('Количество')
            ax2.invert_yaxis() # Сверху "Вчера", снизу "Сегодня"
            
            # Добавляем значения на полосы
            for i, v in enumerate(sends_counts):
                ax2.text(v + (max(sends_counts)*0.05 if max(sends_counts) > 0 else 1), i, f'{int(v)}', va='center', ha='left', fontsize=9)
            
            plt.tight_layout()

            buf2 = io.BytesIO()
            plt.savefig(buf2, format='png', bbox_inches='tight')
            buf2.seek(0)
            plt.close(fig2)

            await bot.send_photo(chat_id=chat_id, photo=buf2, caption="Динамика переписок по аккаунту:")

        else:
            report_lines.append("Нет достаточных данных для построения диаграмм за последние два дня.")

    except Exception as e:
        logging.error(f"Ошибка при генерации диаграммы: {e}", exc_info=True)
        report_lines.append("Возникла ошибка при попытке построения диаграмм.")

    return split_message("".join(report_lines))


# Новая вспомогательная функция для безопасного запуска планировщика
async def start_scheduler_safely(context: ContextTypes.DEFAULT_TYPE):
    """
    Запускает APScheduler и добавляет задачи, если они включены,
    после того как Telegram-бот полностью инициализируется.
    """
    logging.info("Функция start_scheduler_safely запущена.")

    if not scheduler.running:
        scheduler.start()
        logging.info("Планировщик APScheduler успешно запущен через job_queue.")

    # Re-add the daily report job if auto-reports are enabled in settings
    if bot_settings['auto_reports_enabled']:
        if 'daily_auto_report' not in [job.id for job in scheduler.get_jobs()]: 
            scheduler.add_job(
                send_daily_auto_report,
                CronTrigger(hour=bot_settings['report_time'].split(':')[0], minute=bot_settings['report_time'].split(':')[1]),
                args=[context.bot, ADMIN_TELEGRAM_ID, FB_ACCESS_TOKEN, FB_APP_ID, FB_APP_SECRET, AD_ACCOUNT_ID], 
                id='daily_auto_report',
                replace_existing=True 
            )
            logging.info(f"Задача автоматической рассылки отчетов добавлена/обновлена через job_queue. Время: {bot_settings['report_time']}")
    else:
        if scheduler.get_job('daily_auto_report'): 
            scheduler.remove_job('daily_auto_report')
            logging.info("Автоматическая рассылка отчетов деактивирована при старте.")


# --- НОВАЯ ФУНКЦИЯ ДЛЯ АВТОРИЗАЦИИ ---
async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатывает команду /auth для авторизации пользователей по коду.
    """
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    
    if user_id in bot_settings['authorized_users']:
        await context.bot.send_message(chat_id=chat_id, text="Вы уже авторизованы!")
        return

    if not context.args:
        await context.bot.send_message(chat_id=chat_id, text="Пожалуйста, укажите код авторизации. Пример: `/auth 0105`")
        return

    entered_code = context.args[0] 

    if entered_code == bot_settings['login_code']:
        bot_settings['authorized_users'].append(user_id)
        save_settings(bot_settings) 
        
        await context.bot.send_message(chat_id=chat_id, text="🎉 Авторизация пройдена! Теперь у вас есть доступ к боту.")
        logging.info(f"Пользователь {user_id} успешно авторизовался.")
        await start(update, context) 
    else:
        await context.bot.send_message(chat_id=chat_id, text="Неверный код авторизации.")
        logging.warning(f"Пользователь {user_id} ввел неверный код авторизации: '{entered_code}'.")

# --- КОНЕЦ НОВОЙ ФУНКЦИИ ДЛЯ АВТОРИЗАЦИИ ---


# --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ СОХРАНЕНИЯ ЗАКАЗОВ ---
def save_orders_for_date(date_str: str, count: int):
    """
    Сохраняет количество заказов для определенной даты в bot_settings.
    """
    bot_settings['daily_orders'][date_str] = count
    save_settings(bot_settings)
    logging.info(f"Заказы за {date_str} сохранены: {count}")
# --- КОНЕЦ ВСПОМОГАТЕЛЬНОЙ ФУНКЦИИ ---


# --- НОВАЯ ФУНКЦИЯ ДЛЯ ОТЧЕТА ПО ПРОДАЖАМ ---
async def get_sales_report(period: str, ad_account_id: str, fb_access_token: str, fb_app_id: str, fb_app_secret: str) -> list[str]:
    """
    Формирует отчет по продажам (Потрачено, Кол-во заказов, Стоимость заказа) за выбранный период.
    Возвращает список из 3 отдельных сообщений.
    """
    report_parts = []
    
    # 1. Получаем потраченную сумму из Facebook Ads API
    total_spend = 0.0
    try:
        FacebookAdsApi.init(app_id=fb_app_id, app_secret=fb_app_secret, access_token=fb_access_token)
        account = AdAccount(ad_account_id)
        
        insights_data = list(account.get_insights(
            params={'date_preset': period, 'fields': 'spend'}
        ))
        if insights_data and insights_data[0]:
            total_spend = float(insights_data[0].get('spend', '0'))
    except Exception as e:
        logging.error(f"Ошибка при получении трат для отчета по продажам: {e}", exc_info=True)
        report_parts.append(f"❌ Ошибка при получении потраченных средств за {period.replace('_', ' ').title()}.")
        total_spend = None 

    # 2. Суммируем количество заказов из bot_settings['daily_orders'] за выбранный период
    total_orders = 0
    orders_data = bot_settings.get('daily_orders', {})

    # Определяем диапазон дат для суммирования заказов
    start_date = None
    end_date = datetime.date.today()

    if period == 'today':
        start_date = datetime.date.today()
        end_date = datetime.date.today()
    elif period == 'yesterday':
        start_date = datetime.date.today() - datetime.timedelta(days=1)
        end_date = start_date
    elif period == 'last_7d':
        start_date = datetime.date.today() - datetime.timedelta(days=6)
    elif period == 'last_30d':
        start_date = datetime.date.today() - datetime.timedelta(days=29)
    elif period == 'this_month':
        start_date = datetime.date(end_date.year, end_date.month, 1)

    if start_date:
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.isoformat()
            total_orders += orders_data.get(date_str, 0)
            current_date += datetime.timedelta(days=1)
    
    # 3. Формируем 3 отдельных сообщения
    # Сообщение 1: Потрачено
    report_parts.append(
        f"📊 **Отчет по продажам ({period.replace('_', ' ').title()}):**\n\n"
        f"💸 **Сумма потраченных средств:**\n"
        f"  - **${total_spend:.2f}**" if total_spend is not None else "  - Не удалось получить данные."
    )

    # Сообщение 2: Количество заказов
    report_parts.append(
        f"📦 **Общее количество заказов:**\n"
        f"  - **{total_orders}**"
    )

    # Сообщение 3: Стоимость заказа
    cost_per_order = 0.0
    if total_spend is not None and total_orders > 0:
        cost_per_order = total_spend / total_orders
        report_parts.append(
            f"💰 **Средняя стоимость заказа:**\n"
            f"  - **${cost_per_order:.2f}**"
        )
    else:
        report_parts.append(
            f"💰 **Средняя стоимость заказа:**\n"
            f"  - Невозможно рассчитать (нет трат или заказов)."
        )
    
    return report_parts

# --- КОНЕЦ НОВОЙ ФУНКЦИИ ---


# --- Запуск бота ---
if __name__ == '__main__':
    print("Запускаем Telegram бота...")
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    application.bot_data['admin_chat_id'] = ADMIN_TELEGRAM_ID
    application.bot_data['fb_access_token'] = FB_ACCESS_TOKEN
    application.bot_data['fb_app_id'] = FB_APP_ID
    application.bot_data['fb_app_secret'] = FB_APP_SECRET
    application.bot_data['ad_account_id'] = AD_ACCOUNT_ID

    application.job_queue.run_once(start_scheduler_safely, 0) 

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('auth', auth_command)) 
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    application.add_handler(CallbackQueryHandler(button_callback))
    
    print("Бот успешно запущен и ожидает событий...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)