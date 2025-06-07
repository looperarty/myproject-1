import logging
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
# ИСПРАВЛЕНИЕ: Добавлен CallbackQueryHandler в импорты
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
ADMIN_TELEGRAM_ID = 5625120142  

FB_ACCESS_TOKEN = 'EAAJ8ZBYdYhHIBOZCtL2GHEqqqfwaGKcu0nTsV8Ch0lYzZBdhlqZB80ggz3kbgYHJDW0tpik45sfnzfjaSACeOFitccutYsnjIc1fPQTa7Q1nTyaZAcBLzXnp2BrEmrnPkNUkRo7SYoEQWQuczLU1pqKVRgYCEAWeJcTy05CsBWjM2DBUJ5ZBl21JFOUcT4r9VpN2xJyN2vssZARo6YjNKoPHxIDNZCTlF9ZB2ZAsud5mt9HKdD0N8348oZD' 
FB_APP_ID = '700361112847474'
FB_APP_SECRET = 'c947026dfd934f50d832a65eae000ba1'
AD_ACCOUNT_ID = 'act_1573639266674008' 
# -----------------------------------------

# --- Настройки для автоматических отчетов ---
SETTINGS_FILE = 'bot_settings.json' 

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                logging.error(f"Ошибка чтения файла настроек {SETTINGS_FILE}. Используем значения по умолчанию.")
                return {'auto_reports_enabled': False, 'report_time': '09:00'}
    return {'auto_reports_enabled': False, 'report_time': '09:00'} 

def save_settings(settings):
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(settings, f, indent=4)

bot_settings = load_settings()

scheduler = AsyncIOScheduler()

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_TELEGRAM_ID:
        await context.bot.send_message(chat_id=user_id, text="Извините, у вас нет доступа к этому боту.")
        logging.warning(f"Попытка доступа неавторизованного пользователя: {user_id}")
        return

    keyboard = [
        [KeyboardButton("📊 Получить отчет")], 
        [KeyboardButton("💸 Потрачено")], 
        [KeyboardButton("⚙️ Настройки")], 
        [KeyboardButton("❓ Помощь")]
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await context.bot.send_message(
        chat_id=update.effective_chat.id, 
        text="Авторизация пройдена. Выберите действие:", 
        reply_markup=reply_markup
    )
    logging.info(f"Админ {user_id} запустил бот.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_TELEGRAM_ID: 
        await context.bot.send_message(chat_id=user_id, text="Извините, у вас нет доступа к этому боту.")
        return
    
    text = update.message.text
    
    if text == "📊 Получить отчет":
        context.user_data['action_type'] = 'get_full_report' 
        await ask_for_period(update, context) 
    elif text == "💸 Потрачено": 
        context.user_data['action_type'] = 'get_spend_summary' 
        await ask_for_period(update, context) 
    elif text == "⚙️ Настройки": 
        await show_settings_menu(update, context) 
    elif text == "❓ Помощь":
        await context.bot.send_message(
            chat_id=update.effective_chat.id, 
            text="Этот бот поможет вам следить за показателями ваших рекламных кампаний в Facebook (Meta)."
        )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, 
            text="Неизвестная команда. Пожалуйста, используйте кнопки меню."
        )

async def ask_for_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("За сегодня", callback_data='period_today')],
        [InlineKeyboardButton("За вчера", callback_data='period_yesterday')],
        [InlineKeyboardButton("За 7 дней", callback_data='period_last_7d')],
        [InlineKeyboardButton("За 30 дней", callback_data='period_last_30d')],
        [InlineKeyboardButton("Текущий месяц", callback_data='period_this_month')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text('За какой период показать статистику?', reply_markup=reply_markup)
    logging.info(f"Админ {update.effective_user.id} запросил выбор периода.")

    context.user_data['last_message_id'] = update.message.message_id
    context.user_data['chat_id'] = update.effective_chat.id

async def show_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
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
    logging.info(f"Админ {user_id} открыл меню настроек.")


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer() 

    data = query.data 
    user_id = query.from_user.id
    chat_id = query.message.chat_id

    if data.startswith('period_'):
        period_type = data.replace('period_', '') 
        context.user_data['selected_period'] = period_type 
        
        if context.user_data.get('action_type') == 'get_spend_summary':
            await query.edit_message_text(text=f"Минутку, запрашиваю общие траты за '{period_type.replace('_', ' ').title()}' у Facebook...")
            logging.info(f"Админ {user_id} запросил общие траты за {period_type}.")
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
                logging.info(f"Общие траты отправлены админу {user_id} за период {period_type}.")

            except FacebookRequestError as e:
                error_message = f"Ошибка Facebook API: Код: {e.api_error_code()} - Сообщение: {e.api_error_message()}"
                logging.error(f"Ошибка Facebook API для админа {user_id}: {error_message}", exc_info=True)
                await query.edit_message_text(text=error_message)
            except Exception as e:
                full_error = f"Что-то пошло не так: {e}"
                logging.error(f"Неизвестная ошибка: {full_error}", exc_info=True)
                await query.edit_message_text(text=f"Что-то пошло не так: {e}")
            
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
        logging.info(f"Админ {user_id} выбрал период: {period_type}. Запрос уровня детализации.")

    elif data.startswith('level_'):
        level_type = data.replace('level_', '') 
        selected_period = context.user_data.get('selected_period')

        if level_type != 'compare_daily' and not selected_period:
            await context.bot.send_message(chat_id=chat_id, text="Ошибка: Период не выбран. Пожалуйста, начните заново, нажав '📊 Получить отчет'.")
            logging.error(f"Админ {user_id} попытался выбрать уровень без выбранного периода.")
            return

        await context.bot.send_message(chat_id=chat_id, text=f"Минутку, запрашиваю статистику по '{level_type.replace('_', ' ').title()}' у Facebook...")
        logging.info(f"Админ {user_id} запросил отчет по {level_type} за {selected_period if level_type != 'compare_daily' else 'сегодня/вчера'}.")

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
                logging.warning(f"Неизвестный уровень отчета: {level_type} для админа {user_id}.")

            if response_parts:
                for part in response_parts:
                    if isinstance(part, str): 
                        await context.bot.send_message(chat_id=chat_id, text=part, parse_mode='Markdown')
                logging.info(f"Отчет успешно отправлен админу {user_id} за период {selected_period if level_type != 'compare_daily' else 'сегодня/вчера'}.")
            else:
                await context.bot.send_message(chat_id=chat_id, text="Не удалось сгенерировать отчет. Возможно, нет данных.", parse_mode='Markdown')


        except FacebookRequestError as e:
            error_message = f"Ошибка Facebook API: Код: {e.api_error_code()} - Сообщение: {e.api_error_message()}"
            logging.error(f"Ошибка Facebook API для админа {user_id}: {error_message}", exc_info=True)
            await context.bot.send_message(chat_id=chat_id, text=error_message) 
        except Exception as e:
            full_error = f"Что-то пошло не так: {e}"
            logging.error(f"Неизвестная ошибка: {full_error}", exc_info=True)
            await context.bot.send_message(chat_id=chat_id, text=f"Что-то пошло не так: {e}") 

    # 3. Если пользователь нажал кнопку настройки
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
            logging.info(f"Админ {user_id} переключил автоотчеты в статус: {status_text}.")
            await show_settings_menu(update, context) 
        
        elif setting_type == 'back_to_main_menu': 
            await context.bot.delete_message(chat_id=chat_id, message_id=query.message.message_id)
            keyboard = [[KeyboardButton("📊 Получить отчет")], 
                        [KeyboardButton("💸 Потрачено")], 
                        [KeyboardButton("⚙️ Настройки")], 
                        [KeyboardButton("❓ Помощь")]]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await context.bot.send_message(chat_id=chat_id, text="Добро пожаловать в главное меню!", reply_markup=reply_markup)
            logging.info(f"Админ {user_id} вернулся в главное меню.")

        else:
            await query.edit_message_text("Неизвестная настройка.")
            logging.warning(f"Неизвестная настройка: {setting_type} для админа {user_id}.")

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
    Отправляет ежедневный автоматический отчет админу.
    Эта функция будет запускаться планировщиком.
    """
    logging.info(f"Запуск автоматической рассылки отчета для админа {chat_id}...")
    try:
        FacebookAdsApi.init(app_id=fb_app_id, app_secret=fb_app_secret, access_token=fb_access_token)
        account = AdAccount(ad_account_id)
        
        report_parts = await get_campaign_report(account, 'today') 

        if report_parts:
            await bot_instance.send_message(chat_id=chat_id, text="📊 **Ежедневный автоматический отчет:**", parse_mode='Markdown')
            for part in report_parts:
                await bot_instance.send_message(chat_id=chat_id, text=part, parse_mode='Markdown')
            logging.info(f"Ежедневный автоматический отчет успешно отправлен админу {chat_id}.")
        else:
            await bot_instance.send_message(chat_id=chat_id, text="Не удалось сгенерировать ежедневный автоматический отчет. Возможно, нет данных.", parse_mode='Markdown')

    except Exception as e:
        logging.error(f"Ошибка при автоматической рассылке отчета админу {chat_id}: {e}", exc_info=True)
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
                    f"  - 📝 Лиды (с форм): **{leads_generated}**\n" 
                    f"  - 💰 Цена за лид: **${float(cost_per_lead):.2f}**\n" 
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


# --- Запуск бота ---
if __name__ == '__main__':
    print("Запускаем Telegram бота...")
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    
    # Передаем FB API ключи и ID в bot_data, чтобы они были доступны для send_daily_auto_report
    application.bot_data['admin_chat_id'] = ADMIN_TELEGRAM_ID
    application.bot_data['fb_access_token'] = FB_ACCESS_TOKEN
    application.bot_data['fb_app_id'] = FB_APP_ID
    application.bot_data['fb_app_secret'] = FB_APP_SECRET
    application.bot_data['ad_account_id'] = AD_ACCOUNT_ID

    # НОВОЕ: Используем job_queue для безопасного запуска планировщика
    application.job_queue.run_once(start_scheduler_safely, 0) 

    # Регистрируем обработчики для команд, текстовых сообщений и инлайн-кнопок
    application.add_handler(CommandHandler('start', start))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
    application.add_handler(CallbackQueryHandler(button_callback)) # <-- ВОТ ОН, CallbackQueryHandler

    print("Бот успешно запущен и ожидает событий...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)