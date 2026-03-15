# -*- coding: utf-8 -*-
import logging
import html
import requests
import random
import asyncio
import re
from pathlib import Path
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
# --- Button style helpers for Telegram update ---
def get_button_style(text: str) -> str:
    """Return 'primary' for yes/confirm, 'destructive' for no/cancel, else None."""
    text_lower = text.lower()
    if any(x in text_lower for x in ["да", "подтверд", "yes", "ok", "confirm"]):
        return "primary"
    if any(x in text_lower for x in ["нет", "отмена", "отклон", "cancel", "no", "decline"]):
        return "destructive"
    return None
from telegram.error import BadRequest, Forbidden, RetryAfter, TimedOut, NetworkError, Conflict
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, PreCheckoutQueryHandler, filters, ContextTypes, Defaults, ExtBot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.date import DateTrigger
import sys
import os

# Fix encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


# Добавляем текущую директорию в путь для поиска модулей
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db, DB_PATH, BAMBOO_ROD, TEMP_ROD_RANGES

# --- TelegramBotAPI for invoice link creation ---
import httpx
from typing import Any, Optional, Dict, List

class TelegramBotAPI:
    def __init__(self, bot_token: str) -> None:
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"

    async def create_invoice_link(self, **kwargs: Any) -> Optional[str]:
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"[INVOICE] CALL create_invoice_link with kwargs: {kwargs}")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/createInvoiceLink",
                    json=kwargs,
                    timeout=10
                )
            logger.info(f"[INVOICE] Telegram API status: {response.status_code}")
            logger.info(f"[INVOICE] Telegram API response: {response.text}")
            if response.status_code == 200:
                try:
                    result = response.json()
                except Exception as e:
                    logger.error(f"[INVOICE] Failed to parse JSON: {e}, text: {response.text}")
                    return None
                if result.get("ok"):
                    logger.info(f"[INVOICE] Got invoice_url: {result.get('result')}")
                    return result.get("result")
                else:
                    logger.error(f"[INVOICE] Telegram API error: {result.get('description')}, full response: {response.text}")
                    return None
            else:
                logger.error(f"[INVOICE] HTTP error: {response.status_code}, text: {response.text}")
                return None
        except Exception as e:
            logger.error(f"[INVOICE] Exception in create_invoice_link: {e}")
            return None
from game_logic import game
from config import BOT_TOKEN, COIN_NAME, STAR_NAME, GUARANTEED_CATCH_COST, get_current_season, RULES_TEXT, RULES_LINK, INFO_LINK
import notifications
from fish_stickers import FISH_INFO, FISH_STICKERS
from trash_stickers import TRASH_STICKERS
from treasures_stickers import TREASURES_STICKERS
from treasures import DIAMOND_BUY_PRICE, DIAMOND_SELL_PRICE
from weather import weather_system

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

COIN_EMOJI_TAG = '<tg-emoji emoji-id="5379600444098093058">🪙</tg-emoji>'
BAG_EMOJI_TAG = '<tg-emoji emoji-id="5375296873982604963">💰</tg-emoji>'
RULER_EMOJI_TAG = '<tg-emoji emoji-id="5323632458975945310">📏</tg-emoji>'
WORM_EMOJI_TAG = '<tg-emoji emoji-id="5233206123036682153">🪱</tg-emoji>'
FISHING_EMOJI_TAG = '<tg-emoji emoji-id="5343609421316521960">🎣</tg-emoji>'
SCALE_EMOJI_TAG = '<tg-emoji emoji-id="5323632458975945310">⚖️</tg-emoji>'
WAIT_EMOJI_TAG = '<tg-emoji emoji-id="5413704112220949842">⏳</tg-emoji>'
BELUGA_EMOJI_TAG = '<tg-emoji emoji-id="5222292529533167322">🐟</tg-emoji>'
WHITE_SHARK_EMOJI_TAG = '<tg-emoji emoji-id="5361632650278744629">🦈</tg-emoji>'
XP_EMOJI_TAG = '<tg-emoji emoji-id="5472164874886846699">✨</tg-emoji>'
FISH_EMOJI_TAGS = [
    '<tg-emoji emoji-id="5397842858126353661">🐟</tg-emoji>',
    '<tg-emoji emoji-id="5382210409824525356">🐟</tg-emoji>',
]
STAR_EMOJI_TAG = '<tg-emoji emoji-id="5463289097336405244">⭐</tg-emoji>'
LOCATION_EMOJI_TAG = '<tg-emoji emoji-id="5821128296217185461">📍</tg-emoji>'
PARTY_EMOJI_TAG = '<tg-emoji emoji-id="5436040291507247633">🎉</tg-emoji>'
DIAMOND_EMOJI_TAG = '<tg-emoji emoji-id="5366124516055487969">💎</tg-emoji>'
TG_EMOJI_TAG_RE = re.compile(r'<tg-emoji\s+emoji-id="[^"]+">(.*?)</tg-emoji>')

# Booster catalog used by the feeders/echosounder shop.
FEEDER_ITEMS = [
    {
        "code": "feeder_3",
        "name": "Кормашка базовая",
        "bonus": 3,
        "duration_minutes": 60,
        "price_coins": 3000,
        "price_stars": 0,
    },
    {
        "code": "feeder_7",
        "name": "Кормушка усиленная",
        "bonus": 5,
        "duration_minutes": 60,
        "price_coins": 5000,
        "price_stars": 0,
    },
    {
        "code": "feeder_10",
        "name": "Кормушка звёздная",
        "bonus": 7,
        "duration_minutes": 60,
        "price_coins": 0,
        "price_stars": 10,
    },
]

ECHOSOUNDER_CODE = "echosounder"
ECHOSOUNDER_COST_STARS = 20
ECHOSOUNDER_DURATION_HOURS = 24

DYNAMITE_COOLDOWN_HOURS = 8
DYNAMITE_BATCH_ROLLS = 12
DYNAMITE_SKIP_COST_STARS = 15
DYNAMITE_GUARD_CHANCE = 0.001
DYNAMITE_GUARD_BAN_HOURS = 24
DYNAMITE_GUARD_FINE_STARS = 20
DYNAMITE_STICKER_FILE_ID = "CAACAgEAAxkBAAEcHQlptoOhA4B-LV0g-vv7Orrwg4UZfgACXgIAAg60IEQze4zUaM3_bzoE"

def _replace_plain_emoji_segment(text: str) -> str:
    if not text:
        return text
    return (
        text
        .replace("🪙", COIN_EMOJI_TAG)
        .replace("💰", BAG_EMOJI_TAG)
        .replace("📏", RULER_EMOJI_TAG)
        .replace("🪱", WORM_EMOJI_TAG)
        .replace("🎣", FISHING_EMOJI_TAG)
        .replace("⚖️", SCALE_EMOJI_TAG)
        .replace("⏳", WAIT_EMOJI_TAG)
        .replace("⏰", WAIT_EMOJI_TAG)
        .replace("✨", XP_EMOJI_TAG)
        .replace("⭐", STAR_EMOJI_TAG)
        .replace("📍", LOCATION_EMOJI_TAG)
        .replace("🎉", PARTY_EMOJI_TAG)
        .replace("💎", DIAMOND_EMOJI_TAG)
    )


def replace_coin_emoji(text: str) -> str:
    if not text:
        return text
    if '<tg-emoji' not in text:
        return _replace_plain_emoji_segment(text)

    # Preserve existing <tg-emoji> tags and only replace plain emoji outside them.
    result_parts = []
    last_pos = 0
    for match in TG_EMOJI_TAG_RE.finditer(text):
        result_parts.append(_replace_plain_emoji_segment(text[last_pos:match.start()]))
        result_parts.append(match.group(0))
        last_pos = match.end()
    result_parts.append(_replace_plain_emoji_segment(text[last_pos:]))
    return ''.join(result_parts)

def strip_tg_emoji_tags(text: str) -> str:
    if not text or '<tg-emoji' not in text:
        return text
    return TG_EMOJI_TAG_RE.sub(r'\1', text)


class EmojiBot(ExtBot):
    API_CALL_TIMEOUT = float(os.getenv('TG_API_CALL_TIMEOUT', '20'))
    API_CALL_RETRIES = int(os.getenv('TG_API_CALL_RETRIES', '1'))
    RETRY_BACKOFF_SEC = float(os.getenv('TG_API_RETRY_BACKOFF', '1.5'))

    async def _call_with_timeout(self, method_name: str, coro_factory):
        last_exc = None
        for attempt in range(self.API_CALL_RETRIES + 1):
            try:
                return await asyncio.wait_for(coro_factory(), timeout=self.API_CALL_TIMEOUT)
            except RetryAfter as exc:
                last_exc = exc
                wait = float(getattr(exc, 'retry_after', 1) or 1)
                logger.warning("EmojiBot.%s flood limit, waiting %.2fs (attempt %s/%s)", method_name, wait, attempt + 1, self.API_CALL_RETRIES + 1)
                await asyncio.sleep(wait + 1)
            except (BadRequest, Forbidden) as exc:
                # Ошибки Telegram API (например, Chat not found, Forbidden) не лечатся retry'ем
                logger.warning("EmojiBot.%s non-retryable error: %s", method_name, exc)
                raise
            except (TimedOut, NetworkError, asyncio.TimeoutError) as exc:
                last_exc = exc
                if attempt < self.API_CALL_RETRIES:
                    backoff = self.RETRY_BACKOFF_SEC * (attempt + 1)
                    logger.warning("EmojiBot.%s timeout/network error (%s), retry in %.2fs (attempt %s/%s)", method_name, type(exc).__name__, backoff, attempt + 1, self.API_CALL_RETRIES + 1)
                    await asyncio.sleep(backoff)
                    continue
                logger.error("EmojiBot.%s failed after retries due to timeout/network error: %s", method_name, exc)
                raise
            except Exception as exc:
                # Не скрываем неизвестные ошибки логики Telegram API
                logger.error("EmojiBot.%s unexpected error: %s", method_name, exc)
                raise

        if last_exc is not None:
            raise last_exc

    @staticmethod
    def _should_retry_without_custom_emoji(exc: Exception, text: str) -> bool:
        if not text or '<tg-emoji' not in text:
            return False
        error_text = str(exc).lower()
        return any(fragment in error_text for fragment in (
            'document_invalid',
            'can\'t parse entities',
            'cant parse entities',
            'unsupported start tag',
            'custom emoji',
            'entity',
        ))

    async def _send_with_custom_emoji_fallback(self, method_name: str, sender, *args, **kwargs):
        converted_kwargs = dict(kwargs)
        original_text = converted_kwargs.get('text')
        if isinstance(original_text, str):
            converted_kwargs['text'] = replace_coin_emoji(original_text)

        try:
            return await self._call_with_timeout(method_name, lambda: sender(*args, **converted_kwargs))
        except BadRequest as exc:
            converted_text = converted_kwargs.get('text')
            if not isinstance(converted_text, str) or not self._should_retry_without_custom_emoji(exc, converted_text):
                raise

            fallback_kwargs = dict(converted_kwargs)
            fallback_kwargs['text'] = strip_tg_emoji_tags(converted_text)
            logger.warning(
                "EmojiBot.%s rejected custom emoji markup, retrying with plain Unicode: %s",
                method_name,
                exc,
            )
            return await self._call_with_timeout(method_name, lambda: sender(*args, **fallback_kwargs))

    async def send_message(self, *args, **kwargs):
        return await self._send_with_custom_emoji_fallback(
            "send_message",
            super(EmojiBot, self).send_message,
            *args,
            **kwargs,
        )

    async def edit_message_text(self, *args, **kwargs):
        return await self._send_with_custom_emoji_fallback(
            "edit_message_text",
            super(EmojiBot, self).edit_message_text,
            *args,
            **kwargs,
        )

    async def send_document(self, *args, **kwargs):
        return await self._call_with_timeout("send_document", lambda: super(EmojiBot, self).send_document(*args, **kwargs))

    async def send_invoice(self, *args, **kwargs):
        return await self._call_with_timeout("send_invoice", lambda: super(EmojiBot, self).send_invoice(*args, **kwargs))

    async def get_chat(self, *args, **kwargs):
        return await self._call_with_timeout("get_chat", lambda: super(EmojiBot, self).get_chat(*args, **kwargs))

def format_level_progress(level_info):
    if not level_info:
        return ""

    level = level_info.get('level', 0)
    next_level_xp = level_info.get('next_level_xp')
    if next_level_xp is None:
        bar = "[" + ("=" * 10) + "]"
        return f"Уровень {level}: {bar} MAX"

    progress = level_info.get('progress', 0.0)
    filled = int(progress * 10)
    filled = max(0, min(10, filled))
    bar = "[" + ("=" * filled) + ("-" * (10 - filled)) + "]"
    xp_into = level_info.get('xp_into_level', 0)
    xp_needed = level_info.get('xp_needed', 0)
    return f"Уровень {level}: {bar} {xp_into}/{xp_needed}"

def calculate_sale_summary(items):
    total_xp = 0
    total_weight_bonus = 0
    total_rarity_bonus = 0
    total_base = 0
    total_weight = 0.0
    for item in items:
        details = db.calculate_item_xp_details(item)
        total_xp += details['xp_total']
        total_weight_bonus += details['weight_bonus']
        total_rarity_bonus += details.get('rarity_bonus', 0)
        total_base += details['xp_base']
        total_weight += float(item.get('weight') or 0)
    return total_xp, total_base, total_rarity_bonus, total_weight_bonus, total_weight

def format_fish_name(name: str) -> str:
    if name == "Белуга":
        return f"{BELUGA_EMOJI_TAG} {name}"
    if name == "Белая акула":
        return f"{WHITE_SHARK_EMOJI_TAG} {name}"
    return f"{random.choice(FISH_EMOJI_TAGS)} {name}"

class FishBot:
    async def ref_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /ref: показать статистику и обработать вывод звёзд"""
        user_id = update.effective_user.id
        # Получаем разрешённые чаты для пользователя
        allowed_chats = db.get_ref_access_chats(user_id)
        if not allowed_chats:
            await update.message.reply_text("Нет разрешённых чатов для просмотра дохода.")
            return
        # Собираем статистику по каждому чату
        lines = []
        for ref_chat_id in allowed_chats:
            chat_title = db.get_chat_title(ref_chat_id) or f"Чат {ref_chat_id}"
            stars_total = db.get_chat_stars_total(ref_chat_id)
            matured_stars_total = db.get_chat_stars_total(ref_chat_id, min_age_days=21)
            refunds_total = db.get_chat_refunds_total(ref_chat_id)
            percent_sum = int((matured_stars_total * 0.85) / 2)
            available_stars = db.get_available_stars_for_withdraw(user_id, ref_chat_id)
            withdrawn_stars = db.get_withdrawn_stars(user_id, ref_chat_id)
            lines.append(
                f"{chat_title}\nВсего звёзд: {stars_total}\nЗвёзд старше 21 дня: {matured_stars_total}\nРефаунды: {refunds_total}\nВаш процент: {percent_sum}\nДоступно к выводу: {available_stars}\nУже выведено: {withdrawn_stars}"
            )
        keyboard = [[InlineKeyboardButton("💸 Вывод", callback_data=f"withdraw_stars_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text("\n\n".join(lines), reply_markup=reply_markup)

    async def handle_withdraw_stars_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатия на кнопку вывода звёзд"""
        query = update.callback_query
        await query.answer()
        context.user_data['waiting_withdraw_stars'] = True
        await query.message.reply_text("Введите количество звёзд для вывода:")

    async def handle_withdraw_stars_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ввода количества звёзд для вывода"""
        if not context.user_data.get('waiting_withdraw_stars'):
            return
        user_id = update.effective_user.id
        try:
            amount = int(update.message.text.strip())
        except Exception:
            await update.message.reply_text("Ошибка: введите число.")
            return

        allowed_chats = db.get_ref_access_chats(user_id)
        available_stars = sum(db.get_available_stars_for_withdraw(user_id, chat_id) for chat_id in allowed_chats)
        if amount < 1000:
            await update.message.reply_text("Ошибка: минимальный вывод 1000 звёзд.")
            return
        if amount > available_stars:
            await update.message.reply_text("Ошибка: недостаточно звёзд для вывода.")
            return

        admin_id = 793216884
        await self.application.bot.send_message(
            chat_id=admin_id,
            text=(
                f"Пользователь {user_id} запросил вывод {amount} звёзд.\n"
                f"Доступно: {available_stars}.\n"
                f"Одобрить?"
            ),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Одобрено", callback_data=f"approve_withdraw_{user_id}_{amount}")]
            ])
        )
        await update.message.reply_text("Запрос отправлен на одобрение админу.")
        context.user_data.pop('waiting_withdraw_stars', None)

    async def handle_approve_withdraw_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка одобрения вывода звёзд админом"""
        query = update.callback_query
        admin_id = 793216884
        if update.effective_user.id != admin_id:
            await query.answer("Нет доступа", show_alert=True)
            return
        parts = query.data.split('_')
        if len(parts) != 4:
            await query.answer("Ошибка данных", show_alert=True)
            return
        _, _, user_id, amount = parts
        user_id = int(user_id)
        amount = int(amount)

        allowed_chats = db.get_ref_access_chats(user_id)
        if not allowed_chats:
            await query.answer("Нет доступных чатов для вывода", show_alert=True)
            return

        remaining = amount
        for chat_id in allowed_chats:
            if remaining <= 0:
                break
            chat_available = db.get_available_stars_for_withdraw(user_id, chat_id)
            if chat_available <= 0:
                continue
            chunk = min(remaining, chat_available)
            if chunk > 0:
                db.mark_stars_withdrawn(user_id, chunk, chat_id=chat_id)
                remaining -= chunk

        if remaining > 0:
            await query.answer("Недостаточно доступных звёзд на момент одобрения", show_alert=True)
            return

        await query.answer("Одобрено!")
        await self.application.bot.send_message(
            chat_id=user_id,
            text=f"✅ Ваш вывод {amount} звёзд одобрен и обработан!"
        )

    async def new_ref_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /new_ref: добавить реферала с доступом к доходу чата по ссылке"""
        user_id = update.effective_user.id
        if not self._is_owner(user_id):
            await update.message.reply_text("Команда доступна только владельцу бота.")
            return

        await update.message.reply_text(
            "Введите ID пользователя, которому дать доступ, и ссылку на чат (через пробел):\n"
            "Примеры:\n"
            "123456789 -1001234567890\n"
            "123456789 @channel_or_group_username\n"
            "123456789 https://t.me/channel_or_group_username"
        )
        context.user_data['waiting_new_ref'] = True

    async def handle_new_ref_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ввода для /new_ref"""
        if not context.user_data.get('waiting_new_ref'):
            return
        text = update.message.text.strip()
        parts = text.split()
        if len(parts) != 2:
            await update.message.reply_text("Ошибка: введите ID и ссылку через пробел.")
            return
        ref_user_id_raw, chat_link = parts

        try:
            ref_user_id = int(ref_user_id_raw)
        except ValueError:
            await update.message.reply_text("Ошибка: ID пользователя должен быть числом.")
            return

        chat_id = None

        # 1) t.me/c/<id>/<msg_id> -> преобразуем в -100<id> (проверяем первым,
        #    иначе regex на числа выхватит голый id из URL без -100)
        m_c = re.search(r't\.me/c/(\d+)', chat_link, flags=re.IGNORECASE)
        if m_c:
            chat_id = int(f"-100{m_c.group(1)}")
        else:
            # 2) Прямой числовой chat_id (например: -1001234567890 или просто число)
            m = re.search(r'-?\d{9,}', chat_link)
            if m:
                chat_id = int(m.group(0))

        # 3) username / @username / t.me/username[/msg_id] -> resolve через get_chat
        if chat_id is None:
            username = None
            m_user = re.search(r't\.me/([A-Za-z0-9_]{5,})(?:/\d+)?/?$', chat_link, flags=re.IGNORECASE)
            if m_user:
                username = m_user.group(1)
            elif re.fullmatch(r'@?[A-Za-z0-9_]{5,}', chat_link):
                username = chat_link.lstrip('@')

            if username:
                try:
                    chat = await context.bot.get_chat(f"@{username}")
                    chat_id = chat.id
                except Exception as e:
                    logger.warning("/new_ref: failed to resolve @%s: %s", username, e)

        if chat_id is None:
            await update.message.reply_text(
                "Не удалось определить chat_id. Используйте -100... или @username (бот должен быть в этом чате)."
            )
            return
        try:
            db.add_ref_access(ref_user_id, chat_id)
            await update.message.reply_text(f"✅ Доступ для пользователя {ref_user_id} к чату {chat_id} сохранён.")
        except Exception as e:
            await update.message.reply_text(f"Ошибка при сохранении: {e}")
        context.user_data.pop('waiting_new_ref', None)

    async def check_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /check — топ пользователей по весу улова за период (только для владельца)."""
        user_id = update.effective_user.id
        if not self._is_owner(user_id):
            await update.message.reply_text("Команда доступна только владельцу бота.")
            return
        context.user_data['check_step'] = 'ids'
        await update.message.reply_text(
            "Введите ID пользователей через запятую:\n"
            "Пример: 123456789, 987654321"
        )

    async def handle_check_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пошаговый ввод для /check."""
        step = context.user_data.get('check_step')
        if not step:
            return
        text = update.message.text.strip()

        if step == 'ids':
            try:
                ids = [int(x.strip()) for x in text.split(',') if x.strip()]
            except ValueError:
                await update.message.reply_text("Ошибка: все ID должны быть числами. Попробуйте ещё раз.")
                return
            if not ids:
                await update.message.reply_text("Список ID пуст. Попробуйте ещё раз.")
                return
            context.user_data['check_ids'] = ids
            context.user_data['check_step'] = 'since'
            await update.message.reply_text(
                "Введите дату/время начала:\n"
                "Формат: ДД.ММ.ГГГГ ЧЧ:ММ  или  ГГГГ-ММ-ДД ЧЧ:ММ"
            )

        elif step == 'since':
            dt = self._parse_datetime_input(text)
            if dt is None:
                await update.message.reply_text("Не удалось распознать дату. Используйте формат ДД.ММ.ГГГГ ЧЧ:ММ")
                return
            context.user_data['check_since'] = dt
            context.user_data['check_step'] = 'until'
            await update.message.reply_text(
                "Введите дату/время конца:\n"
                "Формат: ДД.ММ.ГГГГ ЧЧ:ММ  или  ГГГГ-ММ-ДД ЧЧ:ММ"
            )

        elif step == 'until':
            dt = self._parse_datetime_input(text)
            if dt is None:
                await update.message.reply_text("Не удалось распознать дату. Используйте формат ДД.ММ.ГГГГ ЧЧ:ММ")
                return
            ids = context.user_data.pop('check_ids', [])
            since = context.user_data.pop('check_since', None)
            context.user_data.pop('check_step', None)

            rows = db.get_users_weight_leaderboard(user_ids=ids, since=since, until=dt)

            since_str = since.strftime('%d.%m.%Y %H:%M') if since else '?'
            until_str = dt.strftime('%d.%m.%Y %H:%M')
            lines = [f"📊 Топ по весу улова\n🕐 {since_str} — {until_str}\n"]
            if not rows:
                lines.append("Нет уловов за указанный период.")
            else:
                for i, r in enumerate(rows, 1):
                    medal = '🥇' if i == 1 else '🥈' if i == 2 else '🥉' if i == 3 else f'{i}.'
                    name = html.escape(str(r.get('username') or '').strip() or f"id{r['user_id']}")
                    lines.append(f"{medal} {name}: {r['total_weight']:.2f} кг ({r['total_fish']} шт.)")
            # Добавляем тех, кого нет в результатах — у них 0
            found_ids = {r['user_id'] for r in rows}
            for uid in ids:
                if uid not in found_ids:
                    lines.append(f"— id{uid}: 0.00 кг (0 шт.)")  # id как fallback если username неизвестен

            await update.message.reply_text('\n'.join(lines))

    async def new_tour_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Создание турнира: выбор типа и ввод параметров."""
        user_id = update.effective_user.id
        if not self._is_owner(user_id):
            await update.message.reply_text("Команда доступна только владельцу бота.")
            return

        context.user_data['new_tour'] = {
            'chat_id': update.effective_chat.id,
            'created_by': user_id,
            'step': 'type',
        }

        keyboard = [
            [InlineKeyboardButton(self.TOUR_TYPES['longest_fish'], callback_data='tour_type_longest_fish')],
            [InlineKeyboardButton(self.TOUR_TYPES['biggest_weight'], callback_data='tour_type_biggest_weight')],
            [InlineKeyboardButton(self.TOUR_TYPES['total_weight'], callback_data='tour_type_total_weight')],
            [InlineKeyboardButton(self.TOUR_TYPES['specific_fish'], callback_data='tour_type_specific_fish')],
        ]
        await update.message.reply_text("Выберите тип турнира:", reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_tour_type_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор типа турнира через inline-кнопки."""
        query = update.callback_query
        await query.answer()

        if not self._is_owner(update.effective_user.id):
            await query.answer("Нет доступа", show_alert=True)
            return

        draft = context.user_data.get('new_tour')
        if not draft:
            await query.edit_message_text("Сессия создания турнира не найдена. Запустите /new_tour заново.")
            return

        selected_type = query.data.replace('tour_type_', '').strip()
        if selected_type not in self.TOUR_TYPES:
            await query.answer("Неизвестный тип", show_alert=True)
            return

        draft['tournament_type'] = selected_type
        if selected_type == 'specific_fish':
            draft['step'] = 'target_fish'
            context.user_data['new_tour'] = draft
            await query.edit_message_text(
                f"Выбран тип: {self.TOUR_TYPES[selected_type]}\n\nВведите название рыбы (точно как в игре):"
            )
            return

        if selected_type == 'longest_fish':
            locations = db.get_locations()
            keyboard = [
                [InlineKeyboardButton(loc['name'], callback_data=f'tour_location_{loc["name"]}')]
                for loc in locations
            ]
            draft['step'] = 'location'
            context.user_data['new_tour'] = draft
            await query.edit_message_text(
                f"Выбран тип: {self.TOUR_TYPES[selected_type]}\n\nВыберите локацию:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        draft['step'] = 'title'
        context.user_data['new_tour'] = draft
        await query.edit_message_text(
            f"Выбран тип: {self.TOUR_TYPES[selected_type]}\n\nВведите название турнира:"
        )

    async def handle_tour_location_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор локации для турнира 'Самая длинная рыба'."""
        query = update.callback_query
        await query.answer()

        if not self._is_owner(update.effective_user.id):
            await query.answer("Нет доступа", show_alert=True)
            return

        draft = context.user_data.get('new_tour')
        if not draft or draft.get('step') != 'location':
            await query.edit_message_text("Сессия не найдена. Запустите /new_tour заново.")
            return

        location_name = query.data.replace('tour_location_', '', 1)
        draft['target_location'] = location_name
        draft['step'] = 'title'
        context.user_data['new_tour'] = draft
        await query.edit_message_text(
            f"📍 Локация: {location_name}\n\nВведите название турнира:"
        )

    async def handle_new_tour_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Пошаговый ввод параметров для нового турнира."""
        draft = context.user_data.get('new_tour')
        if not draft:
            return False

        if not self._is_owner(update.effective_user.id):
            context.user_data.pop('new_tour', None)
            return False

        message = update.effective_message
        if not message or not message.text:
            return True

        text = message.text.strip()
        step = draft.get('step')

        if step == 'target_fish':
            if len(text) < 2:
                await update.message.reply_text("Название рыбы слишком короткое. Введите снова:")
                return True
            draft['target_fish'] = text
            draft['step'] = 'title'
            context.user_data['new_tour'] = draft
            await update.message.reply_text("Введите название турнира:")
            return True

        if step == 'title':
            draft['title'] = text[:120]
            draft['step'] = 'starts_at'
            context.user_data['new_tour'] = draft
            await update.message.reply_text(
                "Введите дату/время начала\n"
                "Формат: ДД.ММ.ГГГГ ЧЧ:ММ\n"
                "или: YYYY-MM-DD HH:MM"
            )
            return True

        if step == 'starts_at':
            starts_at = self._parse_datetime_input(text)
            if not starts_at:
                await update.message.reply_text("Неверный формат даты. Пример: 05.03.2026 19:30")
                return True
            draft['starts_at'] = starts_at
            draft['step'] = 'ends_at'
            context.user_data['new_tour'] = draft
            await update.message.reply_text("Введите дату/время окончания в том же формате:")
            return True

        if step == 'ends_at':
            ends_at = self._parse_datetime_input(text)
            if not ends_at:
                await update.message.reply_text("Неверный формат даты. Пример: 06.03.2026 19:30")
                return True

            starts_at = draft.get('starts_at')
            if not starts_at or ends_at <= starts_at:
                await update.message.reply_text("Дата окончания должна быть позже даты начала.")
                return True

            tournament_id = db.create_tournament(
                chat_id=int(draft['chat_id']),
                created_by=int(draft['created_by']),
                title=draft.get('title') or 'Турнир',
                tournament_type=draft.get('tournament_type'),
                starts_at=starts_at,
                ends_at=ends_at,
                target_fish=draft.get('target_fish'),
                target_location=draft.get('target_location')
            )

            if tournament_id:
                created = db.get_tournament(tournament_id) or {}
                t_type = created.get('tournament_type') or draft.get('tournament_type')
                t_type_name = self.TOUR_TYPES.get(t_type, t_type)
                fish_line = ""
                fish_name = created.get('target_fish') or draft.get('target_fish')
                if fish_name:
                    fish_line = f"\n🎯 Рыба: {fish_name}"
                await update.message.reply_text(
                    f"✅ Турнир создан (ID: {tournament_id})\n"
                    f"🏆 {created.get('title') or draft.get('title')}\n"
                    f"📌 Тип: {t_type_name}{fish_line}\n"
                    f"🕒 {starts_at.strftime('%d.%m.%Y %H:%M')} — {ends_at.strftime('%d.%m.%Y %H:%M')}"
                )
            else:
                await update.message.reply_text("❌ Не удалось создать турнир.")

            context.user_data.pop('new_tour', None)
            return True

        return False

    async def tour_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать топ-10 игроков в активном турнире."""
        tour = db.get_active_tournament()
        if not tour:
            await update.message.reply_text("🏁 Сейчас нет активных турниров.")
            return

        medals = ['🥇', '🥈', '🥉']
        starts_str = tour['starts_at'].strftime('%d.%m.%Y %H:%M') if hasattr(tour['starts_at'], 'strftime') else str(tour['starts_at'])[:16]
        ends_str = tour['ends_at'].strftime('%d.%m.%Y %H:%M') if hasattr(tour['ends_at'], 'strftime') else str(tour['ends_at'])[:16]
        t_type = tour.get('tournament_type', 'total_weight')
        target_location = tour.get('target_location')

        lines = [
            f"🏆 <b>Турнир: {tour['title']}</b>",
            f"📅 {starts_str} — {ends_str}",
            "",
        ]

        if t_type == 'longest_fish' and target_location:
            rows = db.get_location_leaderboard_length(target_location, tour['starts_at'], tour['ends_at'])
            lines.insert(1, f"📍 Локация: {target_location}")
            if not rows:
                lines.append("Пока никто не поймал рыбу на этой локации.")
            else:
                for i, r in enumerate(rows, 1):
                    medal = medals[i - 1] if i <= 3 else f"{i}."
                    name = r.get('username') or str(r['user_id'])
                    fish = r.get('fish_name', '?')
                    length = round(float(r['best_length']), 1)
                    lines.append(f"{medal} {name} — {fish} — {length} см")
        else:
            rows = db.get_tour_leaderboard_weight(tour['starts_at'], tour['ends_at'])
            if not rows:
                lines.append("Пока никто не поймал рыбу.")
            else:
                for i, r in enumerate(rows, 1):
                    medal = medals[i - 1] if i <= 3 else f"{i}."
                    name = html.escape(r.get('username') or str(r['user_id']))
                    weight = round(float(r['total_weight']), 2)
                    lines.append(f"{medal} {name} — {weight} кг")

        await update.message.reply_text("\n".join(lines))

    async def _location_leaderboard_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE, location_name: str):
        """Топ-10 по самой длинной рыбе на локации в рамках активного турнира."""
        tour = db.get_active_tournament_for_location(location_name)
        if not tour:
            await update.message.reply_text(f"🏁 Нет активного турнира по длине рыбы для локации {location_name}.")
            return

        rows = db.get_location_leaderboard_length(location_name, tour['starts_at'], tour['ends_at'])
        medals = ['🥇', '🥈', '🥉']
        starts_str = tour['starts_at'].strftime('%d.%m.%Y %H:%M') if hasattr(tour['starts_at'], 'strftime') else str(tour['starts_at'])[:16]
        ends_str = tour['ends_at'].strftime('%d.%m.%Y %H:%M') if hasattr(tour['ends_at'], 'strftime') else str(tour['ends_at'])[:16]

        lines = [
            f"🕸️ <b>Топ локации: {location_name}</b>",
            f"📅 {starts_str} — {ends_str}",
            "",
        ]
        if not rows:
            lines.append("Пока никто не поймал рыбу на этой локации.")
        else:
            for i, r in enumerate(rows, 1):
                medal = medals[i - 1] if i <= 3 else f"{i}."
                name = html.escape(r.get('username') or str(r['user_id']))
                fish = html.escape(r.get('fish_name', '?'))
                length = round(float(r['best_length']), 1)
                lines.append(f"{medal} {name} — {fish} — {length} см")

        await update.message.reply_text("\n".join(lines), parse_mode='HTML')

    async def ozero_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._location_leaderboard_command(update, context, "Озеро")

    async def reka_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._location_leaderboard_command(update, context, "Река")

    async def more_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._location_leaderboard_command(update, context, "Море")

    async def prud_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self._location_leaderboard_command(update, context, "Городской пруд")

    async def mes_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить сообщение во все чаты (только для владельца)."""
        if not self._is_owner(update.effective_user.id):
            await update.message.reply_text("❌ Только для владельца.")
            return
        if not context.args:
            await update.message.reply_text("Использование: /mes <текст>")
            return
        text = " ".join(context.args)
        chat_ids = db.get_all_chat_ids()
        sent = 0
        failed = 0
        for cid in chat_ids:
            try:
                await context.bot.send_message(chat_id=cid, text=text)
                sent += 1
            except Exception:
                failed += 1
        await update.message.reply_text(f"✅ Отправлено: {sent}, ❌ Ошибки: {failed}")

    async def send_invoice_url_button(self, chat_id, invoice_url, text, user_id=None, invoice_id=None, timeout_sec=60, reply_to_message_id=None):
        """Отправить кнопку оплаты со ссылкой инвойса, с автоотключением."""
        logger.info(f"[INVOICE] Sending invoice button to chat_id={chat_id}, url={invoice_url}, user_id={user_id}, invoice_id={invoice_id}")
        if user_id is None:
            raise ValueError("user_id обязателен для send_invoice_url_button")
        if invoice_id is None:
            invoice_id = f"{user_id}_{int(datetime.now().timestamp())}"
        keyboard = [[InlineKeyboardButton(
            "💳 Оплатить Telegram Stars",
            url=invoice_url
        )]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        send_kwargs = {
            "chat_id": chat_id,
            "text": text,
            "reply_markup": reply_markup,
        }
        if reply_to_message_id is not None:
            send_kwargs["reply_to_message_id"] = reply_to_message_id

        msg = await self.application.bot.send_message(**send_kwargs)
        # Сохраняем активный инвойс для пользователя
        self.active_invoices[user_id] = {
            'invoice_url': invoice_url,
            'group_chat_id': chat_id,
            'group_message_id': msg.message_id,
            'invoice_id': invoice_id,
            'created_at': datetime.now(),
        }
        # Ставим таймаут на отключение кнопки
        await self.schedule_timeout(chat_id, msg.message_id, "⏰ Срок действия этого инвойса истек", timeout_seconds=timeout_sec)

    def _build_guaranteed_payload(self, user_id: int, chat_id: int) -> str:
        return f"guaranteed_{user_id}_{chat_id}_{int(datetime.now().timestamp())}"

    def _parse_guaranteed_payload(self, payload: str) -> Optional[Dict[str, Any]]:
        if not payload or not payload.startswith("guaranteed_"):
            return None

        body = payload[len("guaranteed_"):]
        parts = body.rsplit("_", 2)
        if len(parts) != 3:
            return None

        first_part, chat_part, ts_part = parts

        try:
            group_chat_id = int(chat_part)
            created_ts = int(ts_part)
        except (TypeError, ValueError):
            return None

        payload_user_id = None
        location = None
        try:
            payload_user_id = int(first_part)
        except (TypeError, ValueError):
            location = first_part

        return {
            "payload_user_id": payload_user_id,
            "group_chat_id": group_chat_id,
            "created_ts": created_ts,
            "location": location,
        }

    def _build_harpoon_skip_payload(self, user_id: int, chat_id: int) -> str:
        return f"harpoon_skip_{user_id}_{chat_id}_{int(datetime.now().timestamp())}"

    def _parse_harpoon_skip_payload(self, payload: str) -> Optional[Dict[str, int]]:
        if not payload or not payload.startswith("harpoon_skip_"):
            return None

        body = payload[len("harpoon_skip_"):]
        parts = body.rsplit("_", 2)
        if len(parts) != 3:
            return None

        user_part, chat_part, ts_part = parts
        try:
            return {
                "payload_user_id": int(user_part),
                "group_chat_id": int(chat_part),
                "created_ts": int(ts_part),
            }
        except (TypeError, ValueError):
            return None

    def _build_booster_payload(self, booster_code: str, user_id: int, chat_id: int) -> str:
        return f"booster_{booster_code}_{user_id}_{chat_id}_{int(datetime.now().timestamp())}"

    def _build_dynamite_skip_payload(self, user_id: int, chat_id: int) -> str:
        return f"dynamite_skip_cd_{user_id}_{chat_id}_{int(datetime.now().timestamp())}"

    def _parse_dynamite_skip_payload(self, payload: str) -> Optional[Dict[str, int]]:
        if not payload or not payload.startswith("dynamite_skip_cd_"):
            return None

        body = payload[len("dynamite_skip_cd_"):]
        parts = body.rsplit("_", 2)
        if len(parts) != 3:
            return None

        user_part, chat_part, ts_part = parts
        try:
            return {
                "payload_user_id": int(user_part),
                "group_chat_id": int(chat_part),
                "created_ts": int(ts_part),
            }
        except (TypeError, ValueError):
            return None

    def _build_dynamite_fine_payload(self, user_id: int, chat_id: int) -> str:
        return f"dynamite_fine_{user_id}_{chat_id}_{int(datetime.now().timestamp())}"

    def _parse_dynamite_fine_payload(self, payload: str) -> Optional[Dict[str, int]]:
        if not payload or not payload.startswith("dynamite_fine_"):
            return None

        body = payload[len("dynamite_fine_"):]
        parts = body.rsplit("_", 2)
        if len(parts) != 3:
            return None

        user_part, chat_part, ts_part = parts
        try:
            return {
                "payload_user_id": int(user_part),
                "group_chat_id": int(chat_part),
                "created_ts": int(ts_part),
            }
        except (TypeError, ValueError):
            return None

    def _parse_booster_payload(self, payload: str) -> Optional[Dict[str, Any]]:
        if not payload or not payload.startswith("booster_"):
            return None

        body = payload[len("booster_"):]
        parts = body.rsplit("_", 3)
        if len(parts) != 4:
            return None

        booster_code, user_part, chat_part, ts_part = parts
        try:
            return {
                "booster_code": booster_code,
                "payload_user_id": int(user_part),
                "group_chat_id": int(chat_part),
                "created_ts": int(ts_part),
            }
        except (TypeError, ValueError):
            return None

    def _get_feeder_by_code(self, feeder_code: str) -> Optional[Dict[str, Any]]:
        legacy_aliases = {
            "feeder_basic": "feeder_3",
            "feeder_pro": "feeder_7",
            "feeder_premium": "feeder_10",
            "feeder_5": "feeder_7",
        }
        feeder_code = legacy_aliases.get(feeder_code, feeder_code)
        for item in FEEDER_ITEMS:
            if item["code"] == feeder_code:
                return item
        return None

    def _format_seconds_compact(self, seconds: int) -> str:
        total = max(0, int(seconds))
        minutes, sec = divmod(total, 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}ч {minutes}м {sec}с"
        return f"{minutes}м {sec}с"

    async def _execute_harpoon_catch(self, user_id: int, group_chat_id: int, reply_to_message_id: Optional[int] = None) -> None:
        player = db.get_player(user_id, group_chat_id)
        if not player:
            await self._safe_send_message(
                chat_id=group_chat_id,
                text="❌ Профиль не найден. Используйте /start в этом чате.",
                reply_to_message_id=reply_to_message_id,
            )
            return

        location = player.get('current_location') or "Городской пруд"
        result = game.fish_with_harpoon(user_id, group_chat_id, location)

        db.mark_harpoon_used(user_id, group_chat_id)

        if not result.get("success"):
            await self._safe_send_message(
                chat_id=group_chat_id,
                text=result.get("message", "❌ Гарпун не сработал."),
                reply_to_message_id=reply_to_message_id,
            )
            return

        fish = result.get('fish') or {}
        weight = result.get('weight', 0)
        length = result.get('length', 0)
        fish_name = fish.get('name', 'Неизвестная рыба')
        fish_price = db.calculate_fish_price(fish, weight, length) if fish else 0

        fish_name_display = format_fish_name(fish_name)
        message = (
            f"🗡️ Гарпун сработал!\n\n"
            f"🐟 {fish_name_display}\n"
            f"📏 Размер: {length}см | Вес: {weight} кг\n"
            f"💰 Стоимость: {fish_price} 🪙\n"
            f"📍 Место: {result.get('location', location)}\n"
            f"⭐ Редкость: {fish.get('rarity', 'Обычная')}"
        )

        await self._safe_send_message(
            chat_id=group_chat_id,
            text=message,
            reply_to_message_id=reply_to_message_id,
        )

    async def _create_guaranteed_invoice_url(self, user_id: int, chat_id: int) -> Optional[str]:
        """Создать ссылку инвойса для гарантированного улова."""
        from config import BOT_TOKEN, STAR_NAME

        tg_api = TelegramBotAPI(BOT_TOKEN)
        return await tg_api.create_invoice_link(
            title="Гарантированный улов",
            description=f"Гарантированный улов — подтвердите оплату (1 {STAR_NAME})",
            payload=self._build_guaranteed_payload(user_id, chat_id),
            currency="XTR",
            prices=[{"label": "Вход", "amount": 1}],
        )

    async def _build_guaranteed_invoice_markup(self, user_id: int, chat_id: int) -> Optional[InlineKeyboardMarkup]:
        """Собрать inline-кнопку со ссылкой на оплату гарантированного улова."""
        try:
            invoice_url = await self._create_guaranteed_invoice_url(user_id, chat_id)
        except Exception as e:
            logger.error(f"[INVOICE] Failed to create guaranteed invoice link: {e}")
            return None

        if not invoice_url:
            return None

        return InlineKeyboardMarkup([
            [InlineKeyboardButton(f"⭐ Оплатить {GUARANTEED_CATCH_COST} Telegram Stars", url=invoice_url)]
        ])

    async def handle_pay_invoice_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        user_id = update.effective_user.id
        data = query.data.split(":")
        if len(data) != 3:
            await query.answer("Некорректная кнопка", show_alert=True)
            return
        _, owner_id, invoice_id = data
        if str(user_id) != owner_id:
            await query.answer("Эта кнопка только для вас!", show_alert=True)
            return
        # Проверяем, что инвойс ещё активен
        invoice_info = self.active_invoices.get(int(owner_id))
        if not invoice_info or invoice_info.get('invoice_id') != invoice_id:
            await query.answer("Инвойс уже неактивен", show_alert=True)
            return
        # Открываем ссылку на оплату (отправляем url в чат)
        invoice_url = invoice_info['invoice_url']
        await query.answer()
        await query.edit_message_reply_markup(reply_markup=None)
        await query.message.reply_text(f"Откройте ссылку для оплаты: {invoice_url}")
        # После оплаты (или сразу) можно убрать инвойс из активных
        del self.active_invoices[int(owner_id)]

    def __init__(self):
        self.scheduler = None  # Будет создан в main() с asyncio loop
        self.user_locations = {}  # Временное хранение локаций пользователей
        self.active_timeouts = {}  # Отслеживание активных таймеров
        self.active_invoices = {}  # Отслеживание активных инвойсов по пользователям
        self.application = None  # Будет установлено в main()
        self.OWNER_ID = 793216884
        # Множество уже оплаченных payload'ов — защита от двойной оплаты одного инвойса
        # Ограничено 5000 записями — при переполнении удаляем половину (старые записи)
        self.paid_payloads: set = set()
        self._paid_payloads_max: int = 5000
        # Время запуска бота — сообщения, отправленные ДО этого времени, игнорируются
        self.bot_start_time = datetime.utcnow()
        self.TOUR_TYPES = {
            'longest_fish': 'Самая длинная рыба',
            'biggest_weight': 'Самая большая рыба (вес)',
            'total_weight': 'Общий вес улова',
            'specific_fish': 'Улов определённой рыбы',
        }

    def _is_owner(self, user_id: int) -> bool:
        return int(user_id) == self.OWNER_ID

    def _parse_datetime_input(self, raw_text: str) -> Optional[datetime]:
        value = (raw_text or '').strip()
        if not value:
            return None
        for fmt in ('%d.%m.%Y %H:%M', '%Y-%m-%d %H:%M'):
            try:
                return datetime.strptime(value, fmt)
            except Exception:
                continue
        return None

    # --- Safe API wrappers to handle Flood control (RetryAfter) ---
    async def _safe_send_message(self, **kwargs):
        for attempt in range(3):
            try:
                return await self.application.bot.send_message(**kwargs)
            except (BadRequest, Forbidden) as e:
                logger.warning("_safe_send_message: non-retryable error (chat_id=%s): %s", kwargs.get('chat_id'), e)
                return None
            except RetryAfter as e:
                wait = getattr(e, 'retry_after', None) or getattr(e, 'timeout', 1)
                logger.warning("RetryAfter on send_message, waiting %s sec (attempt %s)", wait, attempt + 1)
                await asyncio.sleep(float(wait) + 1)
            except Exception as e:
                logger.warning("_safe_send_message: unexpected error (chat_id=%s, attempt %s): %s", kwargs.get('chat_id'), attempt + 1, e)
                if attempt >= 2:
                    return None
        logger.error("_safe_send_message: failed after retries args=%s", kwargs)
        return None

    async def _safe_send_document(self, **kwargs):
        for attempt in range(3):
            try:
                return await self.application.bot.send_document(**kwargs)
            except (BadRequest, Forbidden) as e:
                logger.warning("_safe_send_document: non-retryable error (chat_id=%s): %s", kwargs.get('chat_id'), e)
                return None
            except RetryAfter as e:
                wait = getattr(e, 'retry_after', None) or getattr(e, 'timeout', 1)
                logger.warning("RetryAfter on send_document, waiting %s sec (attempt %s)", wait, attempt + 1)
                await asyncio.sleep(float(wait) + 1)
            except Exception as e:
                logger.warning("_safe_send_document: unexpected error (attempt %s): %s", attempt + 1, e)
                if attempt >= 2:
                    return None
        logger.error("_safe_send_document: failed after retries args=%s", kwargs)
        return None

    async def _safe_send_sticker(self, **kwargs):
        for attempt in range(3):
            try:
                return await self.application.bot.send_sticker(**kwargs)
            except (BadRequest, Forbidden) as e:
                logger.warning("_safe_send_sticker: non-retryable error (chat_id=%s): %s", kwargs.get('chat_id'), e)
                return None
            except RetryAfter as e:
                wait = getattr(e, 'retry_after', None) or getattr(e, 'timeout', 1)
                logger.warning("RetryAfter on send_sticker, waiting %s sec (attempt %s)", wait, attempt + 1)
                await asyncio.sleep(float(wait) + 1)
            except Exception as e:
                logger.warning("_safe_send_sticker: unexpected error (chat_id=%s, attempt %s): %s", kwargs.get('chat_id'), attempt + 1, e)
                if attempt >= 2:
                    return None
        logger.error("_safe_send_sticker: failed after retries args=%s", kwargs)
        return None

    async def _safe_edit_message_text(self, **kwargs):
        for attempt in range(3):
            try:
                return await self.application.bot.edit_message_text(**kwargs)
            except (BadRequest, Forbidden) as e:
                logger.warning("_safe_edit_message_text: non-retryable error: %s", e)
                return None
            except RetryAfter as e:
                wait = getattr(e, 'retry_after', None) or getattr(e, 'timeout', 1)
                logger.warning("RetryAfter on edit_message_text, waiting %s sec (attempt %s)", wait, attempt + 1)
                await asyncio.sleep(float(wait) + 1)
            except Exception as e:
                logger.warning("_safe_edit_message_text: unexpected error (attempt %s): %s", attempt + 1, e)
                if attempt >= 2:
                    return None
        logger.error("_safe_edit_message_text: failed after retries args=%s", kwargs)
        return None

    async def _safe_send_invoice(self, **kwargs):
        for attempt in range(3):
            try:
                return await self.application.bot.send_invoice(**kwargs)
            except (BadRequest, Forbidden) as e:
                logger.warning("_safe_send_invoice: non-retryable error (chat_id=%s): %s", kwargs.get('chat_id'), e)
                return None
            except RetryAfter as e:
                wait = getattr(e, 'retry_after', None) or getattr(e, 'timeout', 1)
                logger.warning("RetryAfter on send_invoice, waiting %s sec (attempt %s)", wait, attempt + 1)
                await asyncio.sleep(float(wait) + 1)
            except Exception as e:
                logger.warning("_safe_send_invoice: unexpected error (attempt %s): %s", attempt + 1, e)
                if attempt >= 2:
                    return None
        logger.error("_safe_send_invoice: failed after retries args=%s", kwargs)
        return None

        
    async def cancel_previous_invoice(self, user_id: int):
        """Отменяет предыдущий активный инвойс пользователя"""
        if user_id in self.active_invoices:
            invoice_info = self.active_invoices[user_id]
            chat_id = invoice_info.get('group_chat_id') or invoice_info.get('chat_id')
            message_id = invoice_info.get('group_message_id') or invoice_info.get('message_id')
            
            try:
                # Обновляем предыдущий инвойс с неактивной кнопкой
                keyboard = [
                    [InlineKeyboardButton(
                        f"⏰ Срок действия истек", 
                        callback_data="invoice_cancelled"
                    )]
                ]
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                if chat_id is not None and message_id is not None:
                    await self.application.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text="⏰ Срок действия этого инвойса истек",
                        reply_markup=reply_markup
                    )
                
                # Удаляем таймаут для старого инвойса
                timeout_key = f"payment_{chat_id}_{message_id}"
                if timeout_key in self.active_timeouts:
                    del self.active_timeouts[timeout_key]
                
                # Удаляем старый инвойс из активных
                del self.active_invoices[user_id]
                
            except Exception as e:
                # Инвойсы нельзя редактировать после оплаты или если они уже изменены
                logger.error(f"Ошибка отмены предыдущего инвойса: {e}")
                # Просто удаляем инвойс из активных, чтобы не было конфликтов
                if user_id in self.active_invoices:
                    del self.active_invoices[user_id]
    
    async def schedule_timeout(self, chat_id: int, message_id: int, timeout_message: str, timeout_seconds: int = 30, timeout_callback=None):
        """Планирует таймаут для сообщения"""
        timeout_key = f"payment_{chat_id}_{message_id}"
        
        async def handle_timeout():
            try:
                # Проверяем, что таймер все еще активен
                if timeout_key in self.active_timeouts:
                    # Если есть callback, вызываем его
                    if timeout_callback:
                        await timeout_callback(chat_id, message_id)
                    else:
                        # Если нет callback, просто редактируем сообщение
                        try:
                            await self.application.bot.edit_message_text(
                                chat_id=chat_id,
                                message_id=message_id,
                                text=timeout_message,
                                reply_markup=None
                            )
                        except Exception as edit_error:
                            logger.error(f"Ошибка редактирования сообщения: {edit_error}")
                    
                    # Удаляем таймер из активных
                    if timeout_key in self.active_timeouts:
                        del self.active_timeouts[timeout_key]
            except Exception as e:
                logger.error(f"Error handling timeout: {e}")
        
        # Добавляем таймер в активные
        self.active_timeouts[timeout_key] = True
        
        # Планируем выполнение через указанное время
        run_time = datetime.now() + timedelta(seconds=timeout_seconds)
        try:
            self.scheduler.add_job(
                handle_timeout,
                trigger=DateTrigger(run_date=run_time),
                id=f"timeout_{chat_id}_{message_id}",
                replace_existing=True,
            )
        except Exception as e:
            logger.warning("schedule_timeout: failed to add job for %s: %s", timeout_key, e)
    
    async def heartbeat(self):
        """Периодический heartbeat-лог для мониторинга жизнеспособности бота"""
        try:
            logger.info("[HEARTBEAT] Bot is alive")
        except Exception as e:
            logger.error(f"Error in heartbeat: {e}")
    
    async def auto_recover_rods(self):
        """Автоматически восстанавливает прочность удочек игроков каждые 10 минут"""
        try:
            with db._connect() as conn:
                cursor = conn.cursor()
                # Получаем все удочки, у которых начато восстановление
                cursor.execute('''
                    SELECT user_id, rod_name, current_durability, max_durability, recovery_start_time
                    FROM player_rods
                    WHERE rod_name = ?
                      AND recovery_start_time IS NOT NULL
                      AND current_durability < max_durability
                      AND (chat_id IS NULL OR chat_id < 1)
                ''', (BAMBOO_ROD,))

                rods = cursor.fetchall()
                
                for user_id, rod_name, current_dur, max_dur, recovery_start in rods:
                    # Каждые 10 минут восстанавливается: max_dur / 30 прочности
                    # (т.е. полное восстановление за 5 часов = 300 минут = 30 интервалов по 10 минут)
                    recovery_amount = max(1, max_dur // 30)
                    
                    # Обновляем прочность
                    new_durability = min(max_dur, current_dur + recovery_amount)
                    
                    cursor.execute('''
                        UPDATE player_rods
                        SET current_durability = ?
                        WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                    ''', (new_durability, user_id, rod_name))
                    
                    # Если удочка полностью восстановилась
                    if new_durability == max_dur:
                        cursor.execute('''
                            UPDATE player_rods
                            SET recovery_start_time = NULL
                            WHERE user_id = ? AND (chat_id IS NULL OR chat_id < 1) AND rod_name = ?
                        ''', (user_id, rod_name))
                        # Уведомления в ЛС отключены, чтобы избежать 403 Forbidden
                        logger.info(f"Rod fully recovered for user {user_id}: {rod_name}")
                
                conn.commit()
                logger.info(f"Rod recovery job completed for {len(rods)} rods")
        except Exception as e:
            logger.error(f"Error in auto_recover_rods: {e}")
        
    async def welcome_new_member(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handler for new members is disabled to avoid auto-greeting."""
        # Greeting new members is intentionally disabled.
        return

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /start"""
        # Запускаем scheduler при первом запросе
        if self.scheduler and not self.scheduler.running:
            self.scheduler.start()
            # Добавляем job для автоматического восстановления удочек каждые 10 минут
            self.scheduler.add_job(
                self.auto_recover_rods,
                'interval',
                minutes=10,
                id='auto_recover_rods',
                replace_existing=True
            )
            # Добавляем heartbeat-лог каждую минуту
            self.scheduler.add_job(
                self.heartbeat,
                'interval',
                minutes=1,
                id='heartbeat',
                replace_existing=True
            )
            logger.info("AsyncIOScheduler запущен")

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        username = update.effective_user.username or update.effective_user.first_name

        player = db.get_player(user_id, chat_id)
        if not player:
            # Создаем нового игрока
            player = db.create_player(user_id, username, chat_id)
            welcome_text = f"""
🎣 Добро пожаловать в мир рыбалки, {username}!

🎣 Ваша рыболовная книга:
🪙 Монеты: {player['coins']} {COIN_NAME}
🎣 Удочка: {player['current_rod']}
📍 Локация: {player['current_location']}
🪱 Наживка: {player['current_bait']}

Используйте /menu чтобы начать рыбалку!
            """
        else:
            welcome_text = f"""
🎣 С возвращением, {username}!

🎣 Ваша статистика:
🪙 Монеты: {player['coins']} {COIN_NAME}
🎣 Удочка: {player['current_rod']}
📍 Локация: {player['current_location']}
🪱 Наживка: {player['current_bait']}

Используйте /menu чтобы начать рыбалку!
            """

        # Проверка целостности профиля (удочка, наживка, локация)
        if player:
            updates = {}
            if not player.get('current_rod'):
                updates['current_rod'] = 'Бамбуковая удочка'
            if not player.get('current_bait'):
                updates['current_bait'] = 'Черви'
            if not player.get('current_location'):
                updates['current_location'] = 'Городской пруд'
            if updates:
                db.update_player(user_id, chat_id, **updates)
                player = db.get_player(user_id, chat_id)
            if player:
                player_rod = db.get_player_rod(user_id, player['current_rod'], chat_id)
                if not player_rod:
                    if player['current_rod'] in TEMP_ROD_RANGES:
                        db.update_player(user_id, chat_id, current_rod=BAMBOO_ROD)
                        db.init_player_rod(user_id, BAMBOO_ROD, chat_id)
                        player = db.get_player(user_id, chat_id)
                    else:
                        db.init_player_rod(user_id, player['current_rod'], chat_id)

        await update.message.reply_text(welcome_text)

    async def stars_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Admin-only command in private chat: list chats and total stars they've brought."""
        admin_id = 793216884
        user_id = update.effective_user.id
        # Restrict to admin
        if user_id != admin_id:
            try:
                await update.message.reply_text("Команда доступна только владельцу бота.")
            except Exception:
                pass
            return

        # Only in private chat
        if update.effective_chat.type != 'private':
            try:
                await update.message.reply_text("Команду используйте в личном чате с ботом.")
            except Exception:
                pass
            return

        try:
            rows = db.get_all_chat_stars()
        except Exception as e:
            logger.exception("stars_command: db error: %s", e)
            await update.message.reply_text(f"Ошибка БД: {e}", parse_mode=None)
            return

        # Only show chats that have stars AND a real chat title (not @username, not empty)
        def _is_real_title(r):
            if (r.get('stars_total') or 0) <= 0:
                return False
            t = (r.get('chat_title') or '').strip()
            if not t:
                return False          # no title stored
            if t.startswith('@'):
                return False          # username-only, not a group name
            return True

        rows = [r for r in rows if _is_real_title(r)]

        if not rows:
            await update.message.reply_text("Нет данных по звёздам.", parse_mode=None)
            return

        total_stars = sum((r.get('stars_total') or 0) for r in rows)
        lines = []
        for r in rows:
            title = (r.get('chat_title') or '').strip()
            stars = r.get('stars_total') or 0
            lines.append(f"{title} — {stars}")

        header = f"Всего звёзд: {total_stars}\n\n"
        full_text = header + "\n".join(lines)

        # If text fits in one Telegram message (4096 bytes), send as text; otherwise as file
        if len(full_text.encode('utf-8')) <= 4000:
            try:
                await update.message.reply_text(full_text, parse_mode=None)
            except Exception as e:
                logger.exception("stars_command: send error: %s", e)
        else:
            import io
            file_bytes = full_text.encode('utf-8')
            bio = io.BytesIO(file_bytes)
            bio.name = "stars.txt"
            try:
                await update.message.reply_document(
                    document=bio,
                    filename="stars.txt",
                    caption=f"⭐ Всего звёзд: {total_stars} — {len(rows)} чатов",
                )
            except Exception as e:
                logger.exception("stars_command: send file error: %s", e)
    
    async def fish_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /fish - просто забросить удочку"""
        # Команда работает только в группах/каналах, не в личных чатах
        if update.effective_chat.type == 'private':
            try:
                await update.message.reply_text("Команда /fish работает только в чатах с группой. Для платежей проверьте входящие инвойсы.")
            except Exception as e:
                logger.error(f"Error replying to fish command: {e}")
            return
        
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        
        if not player:
            # Автоматически создаём профиль в этом чате при первом использовании /fish
            try:
                username = update.effective_user.username or update.effective_user.first_name
                player = db.create_player(user_id, username, chat_id)
                await update.message.reply_text("✅ Профиль создан автоматически для этого чата. Продолжаем рыбалку...")
            except Exception as e:
                logger.error(f"Error creating player from fish command: {e}")
                try:
                    await update.message.reply_text("Сначала создайте профиль командой /start")
                except Exception as e:
                    logger.error(f"Error replying to fish command: {e}")
                return
        
        # Проверяем кулдаун
        can_fish, message = game.can_fish(user_id, chat_id)
        if not can_fish:
            # Если удочка сломалась — предлагаем ремонт за 20 ⭐ (НЕ платный заброс)
            if "сломалась" in message:
                rod_name = player['current_rod']
                if rod_name in TEMP_ROD_RANGES:
                    # Временная/одноразовая удочка — просто купить новую
                    await update.message.reply_text(f"💥 {message}")
                    return
                # Бамбуковая / обычная удочка — предлагаем ремонт за 20 ⭐
                from config import BOT_TOKEN
                try:
                    from bot import TelegramBotAPI as _TelegramBotAPI
                    tg_api = _TelegramBotAPI(BOT_TOKEN)
                    repair_invoice_url = await tg_api.create_invoice_link(
                        title="Ремонт удочки",
                        description=f"Полное восстановление прочности удочки '{rod_name}'",
                        payload=f"repair_rod_{rod_name}",
                        currency="XTR",
                        prices=[{"label": f"Ремонт {rod_name}", "amount": 20}]
                    )
                    logger.info(f"[INVOICE] Repair invoice created for rod='{rod_name}' user={user_id}")
                except Exception as e:
                    logger.error(f"[INVOICE] Failed to create repair invoice: {e}")
                    repair_invoice_url = None
                if repair_invoice_url:
                    await self.send_invoice_url_button(
                        chat_id=chat_id,
                        invoice_url=repair_invoice_url,
                        text=(
                            "💔 Ваша удочка сломалась!\n\n"
                            "🔧 Оплатите 20 ⭐ Telegram Stars чтобы мгновенно восстановить её.\n"
                            "Или используйте /repair для бесплатного автовосстановления (займёт время)."
                        ),
                        user_id=user_id,
                        reply_to_message_id=update.effective_message.message_id if update.effective_message else None
                    )
                else:
                    await update.message.reply_text(
                        f"💔 {message}\n\nИспользуйте /repair для восстановления.",
                        parse_mode=None
                    )
                return
            # Кулдаун — обычная кнопка гарантированного улова за 1 ⭐
            from config import BOT_TOKEN, STAR_NAME
            import traceback
            invoice_error = None
            try:
                from bot import TelegramBotAPI as _TelegramBotAPI
                tg_api = _TelegramBotAPI(BOT_TOKEN)
                invoice_url = await tg_api.create_invoice_link(
                    title=f"Гарантированный улов",
                    description=f"Гарантированный улов — подтвердите оплату (1 {STAR_NAME})",
                    payload=f"guaranteed_{user_id}_{chat_id}_{int(datetime.now().timestamp())}",
                    currency="XTR",
                    prices=[{"label": f"Вход", "amount": 1}]
                )
                logger.info(f"[INVOICE] Got invoice_url: {invoice_url}")
            except Exception as e:
                logger.error(f"[INVOICE] Failed to get invoice_url: {e}")
                invoice_url = None
                invoice_error = str(e) + "\n" + traceback.format_exc()
            if invoice_url:
                await self.send_invoice_url_button(
                    chat_id=chat_id,
                    invoice_url=invoice_url,
                    text=f"⏰ {message}\n\n⭐ Оплатите 1 Telegram Stars для гарантированного улова на локации: {player['current_location']}",
                    user_id=user_id,
                    reply_to_message_id=update.effective_message.message_id if update.effective_message else None
                )
            else:
                error_text = f"⏰ {message}\n\n(Ошибка генерации ссылки для оплаты)"
                if invoice_error:
                    error_text += f"\nОшибка: {invoice_error}"
                await update.message.reply_text(error_text, parse_mode=None)
            return
        
        # Начинаем рыбалку на текущей локации
        try:
            # Обновляем состояние популяции рыб (отслеживаем забросы на локации)
            location_changed, consecutive_casts, show_warning = db.update_population_state(
                user_id, 
                player['current_location']
            )
            
            # Если игрок достиг 30 отдельных забросов на одной локации - показываем предупреждение
            if show_warning:
                warning_msg = (
                    "⚠️ <b>ВАЖНО! РЫБЫ ОСТАЛОСЬ МАЛО!</b>\n\n"
                    "Вы 30 раз подряд ловили на одной локации.\n"
                    "Рыба испугалась и её осталось мало в этом месте.\n\n"
                    "🗺️ <b>Смените локацию!</b>\n"
                    "Используйте /menu для выбора другого места.\n\n"
                    "Как снять штраф:\n"
                    "• Не ловить 60 минут\n"
                    "• Или сменить локацию и сделать 10 забросов на новой\n\n"
                    "Если продолжите ловить на одной локации, шансы будут падать:\n"
                    "• 30 забросов: -5%\n"
                    "• 40 забросов: -8%\n"
                    "• 50 забросов: -11%\n"
                    "• 60+ забросов: -15%"
                )
                try:
                    await update.message.reply_text(warning_msg)
                except Exception as e:
                    logger.error(f"Error sending population warning: {e}")
            
            result = game.fish(user_id, chat_id, player['current_location'])
        except Exception as e:
            logger.exception("Unhandled exception in game.fish for user %s chat %s", user_id, chat_id)
            try:
                await update.message.reply_text("❌ Неожиданная ошибка при рыбалке. Обратитесь в поддержку.")
            except Exception:
                pass
            return

        if result.get('nft_win'):
            nft_message = (
                "🎉 Поздравляю, вы выиграли NFT.\n"
                "Какой? Секрет.\n"
                "С вами свяжется админ для передачи.\n"
                "Если в течение дня никто не отпишет вам, свяжитесь через t.me/monkeys_giveaways"
            )
            try:
                await update.message.reply_text(nft_message)
            except Exception as e:
                logger.error(f"Error sending NFT win message: {e}")

            try:
                await self.application.bot.send_message(
                    chat_id=793216884,
                    text=(
                        "NFT win detected.\n"
                        f"User: {update.effective_user.id} ({update.effective_user.username or update.effective_user.full_name})\n"
                        f"Chat: {update.effective_chat.id} ({update.effective_chat.title or ''})"
                    )
                )
            except Exception as e:
                logger.error(f"Error sending NFT admin DM: {e}")
            return
        
        if result['success']:
            if result.get('is_trash'):
                trash = result['trash']
                xp_line = ""
                progress_line = ""
                if result.get('xp_earned'):
                    xp_line = f"\n✨ Опыт: +{result['xp_earned']}"
                    progress_line = f"\n{format_level_progress(result.get('level_info'))}"
                message = f"""
{trash.get('name', 'Мусор')}

⚖️ Вес: {trash.get('weight', 0)} кг
💰 Стоимость: {trash.get('price', 0)} 🪙
📍 Место: {result['location']}
{xp_line}{progress_line}
                """

                sticker_message = None
                if trash.get('name') in TRASH_STICKERS:
                    try:
                        trash_image = TRASH_STICKERS[trash['name']]
                        image_path = Path(__file__).parent / trash_image
                        if image_path.exists():
                            with open(image_path, 'rb') as f:
                                sticker_message = await self.application.bot.send_document(
                                    chat_id=update.effective_chat.id,
                                    document=f,
                                    reply_to_message_id=update.message.message_id
                                )
                            if sticker_message:
                                context.bot_data.setdefault("last_bot_stickers", {})[update.effective_chat.id] = sticker_message.message_id
                    except Exception as e:
                        logger.warning(f"Could not send trash image for {trash.get('name')}: {e}")

                if sticker_message:
                    await update.message.reply_text(message, reply_to_message_id=sticker_message.message_id)
                else:
                    await update.message.reply_text(message)
                
                # ===== ДРАГОЦЕННОСТИ =====
                if result.get('treasure_caught') and result.get('treasure_name'):
                    from treasures import get_treasure_name, get_treasure_price
                    from treasures_stickers import TREASURES_STICKERS
                    
                    treasure_name = result['treasure_name']
                    treasure_info = result['treasure_caught']
                    treasure_display_name = get_treasure_name(treasure_name)
                    treasure_price = get_treasure_price(treasure_name)
                    
                    # Формируем сообщение о драгоценности
                    treasure_message_text = f"""
✨ Чудо случилось! Между мусором ты нашёл драгоценность! ✨

{treasure_display_name}

💎 Стоимость: {treasure_price} 🪙
📍 Место: {result['location']}
                    """
                    
                    # Отправляем стикер драгоценности если он есть
                    if treasure_name in TREASURES_STICKERS:
                        try:
                            treasure_images = TREASURES_STICKERS[treasure_name]
                            # Если это список, выбираем случайный стикер
                            if isinstance(treasure_images, list):
                                treasure_image = random.choice(treasure_images)
                            else:
                                treasure_image = treasure_images
                            image_path = Path(__file__).parent / treasure_image
                            if image_path.exists():
                                with open(image_path, 'rb') as f:
                                    treasure_sticker = await self.application.bot.send_document(
                                        chat_id=update.effective_chat.id,
                                        document=f,
                                        reply_to_message_id=update.message.message_id
                                    )
                                if treasure_sticker:
                                    # Отправляем текст ответом на стикер
                                    await update.message.reply_text(
                                        treasure_message_text,
                                        reply_to_message_id=treasure_sticker.message_id
                                    )
                            else:
                                await update.message.reply_text(treasure_message_text)
                        except Exception as e:
                            logger.warning(f"Could not send treasure image for {treasure_name}: {e}")
                            await update.message.reply_text(treasure_message_text)
                    else:
                        await update.message.reply_text(treasure_message_text)

                if result.get('temp_rod_broken'):
                    await update.message.reply_text(
                        "💥 Временная удочка сломалась после удачного улова.\n"
                        "Теперь активна бамбуковая. Купить новую можно в магазине."
                    )
                return

            fish = result['fish']
            weight = result['weight']
            length = result['length']
            fish_price = result.get('fish_price', fish.get('price', 0))

            logger.info(
                "Catch: user=%s (%s) chat_id=%s chat_title=%s fish=%s location=%s bait=%s weight=%.2fkg length=%.1fcm",
                update.effective_user.id,
                update.effective_user.username or update.effective_user.full_name,
                update.effective_chat.id,
                update.effective_chat.title or "",
                fish['name'],
                result['location'],
                player['current_bait'],
                weight,
                length
            )
            
            # Формируем сообщение о пойманной рыбе
            rarity_emoji = {
                'Обычная': '⚪',
                'Редкая': '🔵',
                'Легендарная': '🟡',
                'Мифическая': '🔴'
            }
            fish_name_display = format_fish_name(fish['name'])
            
            xp_line = ""
            progress_line = ""
            if result.get('xp_earned'):
                xp_line = f"\n✨ Опыт: +{result['xp_earned']}"
                progress_line = f"\n{format_level_progress(result.get('level_info'))}"

            message = f"""
🎉 Поздравляю! Вы поймали рыбу!
{rarity_emoji.get(fish['rarity'], '⚪')} {fish_name_display}
📏 Размер: {length}см | Вес: {weight} кг
💰 Стоимость: {fish_price} 🪙
📍 Место: {result['location']}
⭐ Редкость: {fish['rarity']}{xp_line}{progress_line}

Вы можете продать эту рыбу в лавке! 🐟
            """
            
            if result.get('guaranteed'):
                message += "\n⭐ Гарантированный улов!"
            
            # Добавляем примечание о популяции если штраф активен
            population_penalty = db.get_population_penalty(user_id)
            consecutive_casts_count = db.get_consecutive_casts(user_id)
            if consecutive_casts_count >= 30 and population_penalty > 0:
                penalty_info = (
                    f"\n⚠️ <b>Популяция рыб снижена на {population_penalty:.0f}%</b>\n"
                    f"Забросов подряд: {consecutive_casts_count}/∞"
                )
                # Конвертируем message в HTML если нужно
                if '<b>' not in message:
                    # Заменяем ** на <b> для HTML форматирования
                    message = message.replace('**', '<b>').replace('**', '</b>')
                    # Или просто добавляем Info в конец
                    message += penalty_info
                else:
                    message += penalty_info
            
            # Отправляем фото рыбы если оно есть
            if fish['name'] in FISH_STICKERS:
                try:
                    fish_image = FISH_STICKERS[fish['name']]
                    image_path = Path(__file__).parent / fish_image
                    if image_path.exists():
                        with open(image_path, 'rb') as f:
                            sticker_message = await self.application.bot.send_document(
                                chat_id=update.effective_chat.id,
                                document=f,
                                reply_to_message_id=update.message.message_id
                            )
                        if sticker_message:
                            context.bot_data.setdefault("last_bot_stickers", {})[update.effective_chat.id] = sticker_message.message_id
                            context.bot_data.setdefault("sticker_fish_map", {})[sticker_message.message_id] = {
                                "fish_name": fish['name'],
                                "weight": weight,
                                "price": fish_price,
                                "location": result['location'],
                                "rarity": fish['rarity']
                            }
                except Exception as e:
                    logger.warning(f"Could not send fish image for {fish['name']}: {e}")
            
            await update.message.reply_text(message)

            if result.get('temp_rod_broken'):
                await update.message.reply_text(
                    "💥 Временная удочка сломалась после удачного улова.\n"
                    "Теперь активна бамбуковая. Купить новую можно в магазине."
                )
                return
            
            # ПОСЛЕ сообщения о рыбе проверяем и сообщаем о прочности удочки
            if player['current_rod'] == BAMBOO_ROD and result.get('rod_broken'):
                durability_message = f"""
💔 Удочка сломалась!

🔧 Прочность: 0/{result.get('max_durability', 100)}

Используйте /repair чтобы починить удочку или подождите автовосстановления.
                """
                await update.message.reply_text(durability_message)
            elif player['current_rod'] == BAMBOO_ROD and result.get('current_durability', 100) < result.get('max_durability', 100):
                # Показываем текущую прочность если она уменьшилась
                current = result.get('current_durability', 100)
                maximum = result.get('max_durability', 100)
                durability_message = f"🔧 Прочность удочки: {current}/{maximum}"
                await update.message.reply_text(durability_message)
            return
        else:
            if result.get('rod_broken'):
                message = f"""
💔 Удочка сломалась!

{result['message']}

Используйте /repair чтобы починить удочку.
                """
                await update.message.reply_text(message)
                return
            elif result.get('is_trash'):
                # Мусор пойман
                xp_line = ""
                progress_line = ""
                if result.get('xp_earned'):
                    xp_line = f"\n✨ Опыт: +{result['xp_earned']}"
                    progress_line = f"\n{format_level_progress(result.get('level_info'))}"
                message = f"""
{result['message']}

📦 Мусор: {result['trash']['name']}
⚖️ Вес: {result['trash']['weight']} кг
💰 Стоимость: {result['trash']['price']} 🪙
{xp_line}{progress_line}

Ваш баланс: {result['new_balance']} 🪙
                """
                
                # Отправляем фото мусора если оно есть
                if result['trash']['name'] in TRASH_STICKERS:
                    try:
                        trash_image = TRASH_STICKERS[result['trash']['name']]
                        image_path = Path(__file__).parent / trash_image
                        if image_path.exists():
                            with open(image_path, 'rb') as f:
                                sticker_message = await self.application.bot.send_document(
                                    chat_id=update.effective_chat.id,
                                    document=f
                                )
                            if sticker_message:
                                context.bot_data.setdefault("last_bot_stickers", {})[update.effective_chat.id] = sticker_message.message_id
                    except Exception as e:
                        logger.warning(f"Could not send trash image for {result['trash']['name']}: {e}")
                
                await update.message.reply_text(message)
                if result.get('temp_rod_broken'):
                    await update.message.reply_text(
                        "💥 Временная удочка сломалась после удачного улова.\n"
                        "Теперь активна бамбуковая. Купить новую можно в магазине."
                    )
                return
            elif result.get('no_bite'):
                # При no_bite также создаём invoice_url и кнопку
                from config import BOT_TOKEN, STAR_NAME
                import traceback
                invoice_error = None
                try:
                    from bot import TelegramBotAPI as _TelegramBotAPI
                    tg_api = _TelegramBotAPI(BOT_TOKEN)
                    invoice_url = await tg_api.create_invoice_link(
                        title=f"Гарантированный улов",
                        description=f"Гарантированный улов — подтвердите оплату (1 {STAR_NAME})",
                        payload=f"guaranteed_{user_id}_{chat_id}_{int(datetime.now().timestamp())}",
                        currency="XTR",
                        prices=[{"label": f"Вход", "amount": 1}]
                    )
                    logger.info(f"[INVOICE] Got invoice_url: {invoice_url}")
                except Exception as e:
                    logger.error(f"[INVOICE] Failed to get invoice_url: {e}")
                    invoice_url = None
                    invoice_error = str(e) + "\n" + traceback.format_exc()
                if invoice_url:
                    await self.send_invoice_url_button(
                        chat_id=chat_id,
                        invoice_url=invoice_url,
                        text=f"😔 {result['message']}\n\n⭐ Оплатите 1 Telegram Stars для гарантированного улова на локации: {result['location']}",
                        user_id=user_id,
                        reply_to_message_id=update.effective_message.message_id if update.effective_message else None
                    )
                else:
                    error_text = f"😔 {result['message']}\n\n(Ошибка генерации ссылки для оплаты)"
                    if invoice_error:
                        error_text += f"\nОшибка: {invoice_error}"
                    await update.message.reply_text(error_text, parse_mode=None)
                return
            else:
                # Если арест рыбнадзора — не показываем кнопку платного заброса
                if result.get('fish_inspector') or "рыбнадзор" in result.get('message', '').lower():
                    # Стикер рыбнадзора — только при свежем аресте (не при повторных попытках)
                    if result.get('fish_inspector'):
                        try:
                            inspector_image = FISH_STICKERS.get("Рыбнадзор")
                            if inspector_image:
                                image_path = Path(__file__).parent / inspector_image
                                if image_path.exists():
                                    with open(image_path, 'rb') as f:
                                        await self.application.bot.send_document(
                                            chat_id=update.effective_chat.id,
                                            document=f,
                                            reply_to_message_id=update.message.message_id
                                        )
                        except Exception as e:
                            logger.warning(f"Could not send fish inspector sticker: {e}")
                    await update.message.reply_text(result['message'], parse_mode=None)
                    return
                # Отправляем сообщение с причиной и кнопкой оплаты
                reply_markup = await self._build_guaranteed_invoice_markup(user_id, chat_id)
                await update.message.reply_text(
                    f"😔 {result['message']}",
                    reply_markup=reply_markup
                )
                return
    
    async def menu_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /menu - показать меню рыбалки"""
        # Команда работает только в группах/каналах, не в личных чатах
        if update.effective_chat.type == 'private':
            await update.message.reply_text("Команда /menu работает только в чатах с группой. Для платежей проверьте входящие инвойсы.")
            return
        
        chat_id = update.effective_chat.id
        player = db.get_player(update.effective_user.id, chat_id)
        
        if not player:
            await update.message.reply_text("Сначала создайте профиль командой /start")
            return
        
        await self.show_fishing_menu(update, context)

    async def show_fishing_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню рыбалки"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        player = db.get_player(user_id, chat_id)
        if not player:
            if update.message:
                await update.message.reply_text("Сначала создайте профиль командой /start")
            else:
                await update.callback_query.answer("Сначала создайте профиль командой /start", show_alert=True)
            return

        rod_name = player['current_rod']
        player_rod = db.get_player_rod(user_id, rod_name, chat_id)
        if not player_rod:
            if rod_name in TEMP_ROD_RANGES:
                db.update_player(user_id, chat_id, current_rod=BAMBOO_ROD)
                db.init_player_rod(user_id, BAMBOO_ROD, chat_id)
                player = db.get_player(user_id, chat_id)
                rod_name = player['current_rod']
                player_rod = db.get_player_rod(user_id, rod_name, chat_id)
            else:
                db.init_player_rod(user_id, rod_name, chat_id)
                player_rod = db.get_player_rod(user_id, rod_name, chat_id)
        durability_line = ""
        if player_rod and rod_name == BAMBOO_ROD:
            durability_line = f"🔧 Прочность: {player_rod['current_durability']}/{player_rod['max_durability']}\n"

        diamond_count = player.get('diamonds', 0)
        menu_text = f"""
    {FISHING_EMOJI_TAG} Меню рыбалки

    {COIN_EMOJI_TAG} Монеты: {html.escape(str(player['coins']))} {html.escape(COIN_NAME)}
    {DIAMOND_EMOJI_TAG} Бриллианты: {html.escape(str(diamond_count))}
    {FISHING_EMOJI_TAG} Удочка: {html.escape(str(player['current_rod']))}
    {LOCATION_EMOJI_TAG} Локация: {html.escape(str(player['current_location']))}
    {WORM_EMOJI_TAG} Наживка: {html.escape(str(player['current_bait']))}
    {durability_line}
        """

        keyboard = [
            [InlineKeyboardButton("🎣 Начать рыбалку", callback_data=f"start_fishing_{user_id}")],
            [InlineKeyboardButton("📍 Сменить локацию", callback_data=f"change_location_{user_id}")],
            [InlineKeyboardButton("🪱 Сменить наживку", callback_data=f"change_bait_{user_id}")],
            [InlineKeyboardButton("🧺 Лавка", callback_data=f"sell_fish_{user_id}"), InlineKeyboardButton("🛒 Магазин", callback_data=f"shop_{user_id}")],
            [InlineKeyboardButton("📊 Статистика", callback_data=f"stats_{user_id}"), InlineKeyboardButton("🎒 Инвентарь", callback_data=f"inventory_{user_id}")]
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.message:
            await update.message.reply_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
        else:
            await update.callback_query.edit_message_text(menu_text, reply_markup=reply_markup, parse_mode="HTML")
    
    async def handle_change_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка смены локации"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        locations = db.get_locations()
        keyboard = []
        
        for loc in locations:
            # Показываем актуальное количество человек в чате
            players_count = db.get_location_players_count(loc['name'], chat_id)
            players_info = f"👥 {players_count}"
            
            keyboard.append([InlineKeyboardButton(
                f"📍 {loc['name']} {players_info}",
                callback_data=f"select_location_{loc['name']}_{user_id}"
            )])
        
        # Добавляем кнопку возврата
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = "📍 Выберите новую локацию:"
        
        await query.edit_message_text(message, reply_markup=reply_markup)
    
    async def handle_change_bait(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка смены наживки - выбор между локацией/удочкой"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}") and not query.data.startswith(f"change_bait_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        # Получаем все локации
        locations = db.get_locations()
        
        keyboard = []
        for idx, location in enumerate(locations):
            keyboard.append([InlineKeyboardButton(
                f"📍 {location['name']}",
                callback_data=f"change_bait_loc_{idx}_{user_id}"
            )])
        
        # Добавляем кнопку выбора удочки
        keyboard.append([InlineKeyboardButton(
            "🎣 Выбрать удочку",
            callback_data=f"change_rod_{user_id}"
        )])
        
        # Добавляем кнопку выбора сети
        keyboard.append([InlineKeyboardButton(
            "🕸️ Выбрать сеть",
            callback_data=f"select_net_{user_id}"
        )])

        if db.is_echosounder_active(user_id, chat_id):
            keyboard.append([InlineKeyboardButton(
                "📡 Эхолот",
                callback_data=f"show_echosounder_{user_id}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 Меню", callback_data=f"back_to_menu_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = "🪱 Сменить наживку, удочку или сеть\n\nВыберите локацию для выбора наживки или используйте кнопки ниже:"
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup)
        except Exception as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка: {e}")
                try:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=message,
                        reply_markup=reply_markup
                    )
                except Exception as e2:
                    logger.error(f"Failed to send change_bait menu: {e2}")

    async def handle_show_echosounder(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать лучший клёв по погоде и ориентир по наживке/рыбе."""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        if not db.is_echosounder_active(user_id, chat_id):
            await query.answer("Эхолот не активен. Купите его в магазине.", show_alert=True)
            return

        locations = db.get_locations()
        if not locations:
            await query.answer("Локации не найдены.", show_alert=True)
            return

        best_location = None
        best_bonus = -999
        best_condition = ""

        for loc in locations:
            loc_name = loc.get('name')
            if not loc_name:
                continue
            weather = db.get_or_update_weather(loc_name)
            condition = weather.get('condition', 'Ясно') if weather else 'Ясно'
            bonus = weather_system.get_weather_bonus(condition)
            if bonus > best_bonus:
                best_bonus = bonus
                best_location = loc_name
                best_condition = condition

        if not best_location:
            await query.answer("Не удалось рассчитать лучший клёв.", show_alert=True)
            return

        season = get_current_season()
        fish_list = db.get_fish_by_location(best_location, season, min_level=999)
        top_fish = None
        if fish_list:
            top_fish = max(fish_list, key=lambda item: float(item.get('max_weight') or 0))

        if top_fish:
            fish_name = str(top_fish.get('name', 'Неизвестно'))
            max_weight = float(top_fish.get('max_weight') or 0)
            suitable = str(top_fish.get('suitable_baits') or 'Все')
            if suitable.strip().lower() == 'все':
                bait_tip = "Любая"
            else:
                bait_tip = suitable.split(',')[0].strip()
        else:
            fish_name = "нет данных"
            max_weight = 0
            bait_tip = "Любая"

        alert_text = (
            f"Лучшая локация: {best_location} ({best_condition}, {best_bonus:+d}%). "
            f"Макс рыба: {fish_name} до {max_weight:.1f}кг. Наживка: {bait_tip}."
        )
        if len(alert_text) > 200:
            alert_text = alert_text[:197] + "..."

        await query.answer(alert_text, show_alert=True)
    
    async def handle_change_bait_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать наживки игрока для выбранной локации"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        # Разбор: change_bait_loc_{loc_idx}_{user_id}_{page}
        try:
            parts = query.data.split('_')
            loc_idx = int(parts[3])
            page = int(parts[5]) if len(parts) > 5 else 1
        except (IndexError, ValueError) as e:
            logger.error("handle_change_bait_location: bad callback_data=%s: %s", query.data, e)
            await query.answer("Ошибка навигации", show_alert=True)
            return

        try:
            await query.answer()
        except Exception:
            pass

        try:
            locations = db.get_locations()
        except Exception as e:
            logger.exception("handle_change_bait_location: db.get_locations failed: %s", e)
            try:
                await query.edit_message_text("❌ Не удалось загрузить локации. Попробуйте позже.")
            except Exception:
                pass
            return

        if loc_idx >= len(locations):
            await query.edit_message_text("❌ Локация не найдена!")
            return
        location = locations[loc_idx]['name']
        
        # Получаем наживки игрока для этой локации
        try:
            baits = db.get_player_baits_for_location(user_id, location)
        except Exception as e:
            logger.exception("handle_change_bait_location: db error user=%s location=%s: %s", user_id, location, e)
            try:
                await query.edit_message_text("❌ Не удалось загрузить наживки. Попробуйте позже.")
            except Exception:
                pass
            return

        if not baits:
            keyboard = [
                [InlineKeyboardButton("🪱 Черви (∞)", callback_data=f"select_bait_Черви_{user_id}")],
                [
                    InlineKeyboardButton("🔙 Назад", callback_data=f"change_bait_{user_id}"),
                    InlineKeyboardButton("🛒 В магазин", callback_data=f"shop_baits_{user_id}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                f"❌ У вас нет наживок для {location}!\n\nМожно использовать червей или купить наживки в магазине.",
                reply_markup=reply_markup
            )
            return
        
        page_size = 5
        total_pages = max(1, (len(baits) + page_size - 1) // page_size)
        page = max(1, min(page, total_pages))
        start = (page - 1) * page_size
        end = start + page_size
        page_baits = baits[start:end]
        
        # Кнопки наживок с количеством (используем ID, чтобы не ломаться на пробелах)
        keyboard = []
        for bait in page_baits:
            cb_data = f"select_bait_id_{bait['id']}_{user_id}"
            if len(cb_data.encode('utf-8')) > 64:
                cb_data = f"sbi_{bait['id']}_{user_id}"

            keyboard.append([InlineKeyboardButton(
                f"🪱 {bait['name']} ({bait['player_quantity']} шт)",
                callback_data=cb_data
            )])
        
        # Добавляем бесконечные черви отдельной кнопкой
        keyboard.append([InlineKeyboardButton(
            "🪱 Черви (∞)",
            callback_data=f"select_bait_Черви_{user_id}"
        )])

        # Навигация
        nav_buttons = []
        if total_pages > 1:
            prev_page = page - 1 if page > 1 else total_pages
            next_page = page + 1 if page < total_pages else 1
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"change_bait_loc_{loc_idx}_{user_id}_{prev_page}"))
        
        nav_buttons.append(InlineKeyboardButton("🔙 Назад", callback_data=f"change_bait_{user_id}"))
        
        if total_pages > 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"change_bait_loc_{loc_idx}_{user_id}_{next_page}"))
        
        keyboard.append(nav_buttons)
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"🪱 Выберите наживку для {location} ({page}/{total_pages}):"
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup)
        except Exception as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка изменения меню наживок: {e}")
                logger.error(f"Callback data: {query.data}")
                for i, row in enumerate(keyboard):
                    for j, btn in enumerate(row):
                        logger.error(f"Button [{i}][{j}]: text='{btn.text}', callback_data='{btn.callback_data}' (len={len(btn.callback_data)})")
                try:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=message,
                        reply_markup=reply_markup
                    )
                except Exception as e2:
                    logger.error(f"Failed to send change_bait_location as new message: {e2}")

    async def handle_change_rod(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка смены удочки"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        player = db.get_player(user_id, chat_id)
        if player and player.get('current_rod') == HARPOON_NAME:
            db.init_player_rod(user_id, BAMBOO_ROD, chat_id)
            db.update_player(user_id, chat_id, current_rod=BAMBOO_ROD)
            player = db.get_player(user_id, chat_id)
        db.ensure_rod_catalog()
        all_rods = db.get_rods()
        
        keyboard = []
        
        # Добавляем бамбуковую удочку (всегда есть)
        bamboo_rod = db.get_rod("Бамбуковая удочка")
        if bamboo_rod:
            current = "✅" if player['current_rod'] == "Бамбуковая удочка" else ""
            kb_data = f"select_rod_Бамбуковая удочка_{user_id}"
            if len(kb_data.encode('utf-8')) > 64:
                kb_data = f"sr_bamboo_{user_id}"
            keyboard.append([InlineKeyboardButton(
                f"🎣 Бамбуковая удочка (всегда есть) {current}",
                callback_data=kb_data
            )])
        
        # Добавляем остальные удочки
        for rod in all_rods:
            if rod['name'] not in ("Бамбуковая удочка", HARPOON_NAME):  # Исключаем стартовую и гарпун (он отдельный инструмент)
                owned_rod = db.get_player_rod(user_id, rod['name'], chat_id)
                if not owned_rod:
                    continue
                current = "✅" if player['current_rod'] == rod['name'] else ""
                # Получаем текущую прочность удочки
                durability_str = ""
                if rod['name'] == BAMBOO_ROD:
                    player_rod = db.get_player_rod(user_id, rod['name'], chat_id)
                    if player_rod:
                        durability_str = f" ({player_rod['current_durability']}/{player_rod['max_durability']})"
                
                cb_data = f"select_rod_{rod['name']}_{user_id}"
                if len(cb_data.encode('utf-8')) > 64:
                    cb_data = f"sr_{rod['id']}_{user_id}"
                
                keyboard.append([InlineKeyboardButton(
                    f"🎣 {rod['name']}{durability_str} {current}",
                    callback_data=cb_data
                )])

        # Гарпун отдельным инструментом (не как удочка)
        harpoon_owned = db.get_player_rod(user_id, HARPOON_NAME, chat_id)
        if harpoon_owned:
            remaining = db.get_harpoon_cooldown_remaining(user_id, chat_id, HARPOON_COOLDOWN_MINUTES)
            if remaining > 0:
                harpoon_status = f"⏳ {self._format_seconds_compact(remaining)}"
            else:
                harpoon_status = "✅ Готов"

            keyboard.append([InlineKeyboardButton(
                f"🗡️ {HARPOON_NAME} ({harpoon_status})",
                callback_data=f"use_harpoon_{user_id}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"change_bait_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = "🎣 Выберите удочку:\n\n🗡️ Гарпун теперь используется отдельно и не влияет на выбор удочки."
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Ошибка смены удочки: {e}")
    
    async def handle_select_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора локации"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        # Извлекаем название локации (убираем префикс и user_id)
        location_name = query.data.replace(f"select_location_", "").replace(f"_{user_id}", "")
        
        # Обновляем локацию игрока
        db.update_player_location(user_id, chat_id, location_name)

        keyboard = [[
            InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}"),
            InlineKeyboardButton("📍 Сменить локацию", callback_data=f"change_location_{user_id}")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"📍 Локация изменена на: {location_name}",
            reply_markup=reply_markup
        )
    
    async def handle_select_bait(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора наживки"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        # Поддержка форматов: select_bait_id_{id}_{user_id}, sbi_{id}_{user_id}, select_bait_{name}_{user_id}
        bait_name = None
        if query.data.startswith("select_bait_id_") or query.data.startswith("sbi_"):
            parts = query.data.split('_')
            bait_id = None
            if query.data.startswith("select_bait_id_"):
                # Формат: select_bait_id_{id}_{user_id}
                if len(parts) >= 5:
                    try:
                        bait_id = int(parts[3])
                    except ValueError:
                        bait_id = None
            else:
                # Формат: sbi_{id}_{user_id}
                if len(parts) >= 3:
                    try:
                        bait_id = int(parts[1])
                    except ValueError:
                        bait_id = None

            if bait_id is not None:
                baits = db.get_baits()
                bait = next((b for b in baits if b['id'] == bait_id), None)
                if bait:
                    bait_name = bait['name']
        else:
            bait_name = query.data.replace("select_bait_", "").replace(f"_{user_id}", "")

        if not bait_name:
            await query.edit_message_text("❌ Наживка не найдена!")
            return

        # Обновляем наживку игрока
        db.update_player_bait(user_id, chat_id, bait_name)

        await query.edit_message_text(f"🪱 Наживка изменена на: {bait_name}")
    
    async def handle_select_net(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора сети в меню"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        # Показываем доступные сети игрока
        player_nets = db.get_player_nets(user_id, chat_id)
        if not player_nets:
            db.init_player_net(user_id, 'Базовая сеть', chat_id)
            player_nets = db.get_player_nets(user_id, chat_id)
        
        if not player_nets:
            keyboard = [
                [InlineKeyboardButton("🛒 Купить сети", callback_data=f"shop_nets_{user_id}")],
                [InlineKeyboardButton("🔙 Назад", callback_data=f"change_bait_{user_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(
                "❌ У вас нет сетей!\n\n"
                "Используйте магазин чтобы купить сети.",
                reply_markup=reply_markup
            )
            return
        
        # Показываем список сетей
        keyboard = []
        any_on_cooldown = False
        for net in player_nets:
            # Проверяем кулдаун
            cooldown = db.get_net_cooldown_remaining(user_id, net['net_name'], chat_id)
            
            if cooldown > 0:
                any_on_cooldown = True
                hours = cooldown // 3600
                minutes = (cooldown % 3600) // 60
                time_str = f"{hours}ч {minutes}м" if hours > 0 else f"{minutes}м"
                status = f"⏳ {time_str}"
            elif net['max_uses'] != -1 and net['uses_left'] <= 0:
                status = "❌ Использовано"
            else:
                uses_str = "∞" if net['max_uses'] == -1 else f"{net['uses_left']}"
                status = f"✅ ({uses_str} исп.)"
            
            button_text = f"🕸️ {net['net_name']} - {status}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"view_net_{net['net_name']}_{user_id}")])
        
        if any_on_cooldown:
            keyboard.append([InlineKeyboardButton("⚡ Сбросить КД сетей — 10 ⭐", callback_data=f"net_skip_cd_{user_id}")])
        keyboard.append([
            InlineKeyboardButton("🛒 Купить сети", callback_data=f"shop_nets_{user_id}"),
            InlineKeyboardButton("🔙 Назад", callback_data=f"change_bait_{user_id}")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = "🕸️ Ваши сети:\n\nВыберите сеть для просмотра информации:"
        
        await query.edit_message_text(message, reply_markup=reply_markup)

    async def handle_use_net(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка использования сети"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        # Формат: use_net_{net_name}_{user_id}
        parts = query.data.split('_')
        net_name = '_'.join(parts[2:-1])  # Все части между use_net и user_id
        
        player = db.get_player(user_id, chat_id)
        if not player:
            await query.answer("Профиль не найден", show_alert=True)
            return
        
        # Проверяем наличие сети у игрока
        player_net = db.get_player_net(user_id, net_name, chat_id)
        if not player_net:
            await query.answer("❌ У вас нет этой сети!", show_alert=True)
            return
        
        # Проверяем кулдаун
        cooldown = db.get_net_cooldown_remaining(user_id, net_name, chat_id)
        if cooldown > 0:
            hours = cooldown // 3600
            minutes = (cooldown % 3600) // 60
            time_str = f"{hours}ч {minutes}м" if hours > 0 else f"{minutes}м"
            await query.answer(f"⏳ Сеть можно использовать через {time_str}", show_alert=True)
            return
        
        # Проверяем использования
        if player_net['max_uses'] != -1 and player_net['uses_left'] <= 0:
            await query.answer("❌ У этой сети закончились использования!", show_alert=True)
            return
        
        await query.answer()
        
        # Используем сеть
        location = player['current_location']
        season = get_current_season()
        fish_count = player_net['fish_count']
        
        # Получаем рыбу для текущей локации и сезона
        available_fish = db.get_fish_by_location(location, season, min_level=player.get('level', 0) or 0)
        
        # Получаем мусор для локации
        available_trash = db.get_trash_by_location(location)
        
        if not available_fish and not available_trash:
            await query.edit_message_text(
                f"❌ В локации {location} нет доступного контента в сезон {season}!"
            )
            return
        
        # Вытаскиваем случайные рыбы и мусор
        catch_results = []
        total_value = 0
        feeder_bonus = db.get_active_feeder_bonus(user_id, chat_id)
        fish_chance = min(95, 80 + feeder_bonus)
        
        for i in range(fish_count):
            # Базово 80% шанс рыбы, кормушка увеличивает шанс
            is_trash = random.randint(1, 100) > fish_chance
            
            if is_trash and available_trash:
                # Ловим мусор
                trash = random.choice(available_trash)
                db.add_caught_fish(user_id, chat_id, trash['name'], trash['weight'], location, 0)

                logger.info(
                    "Net catch (trash): user=%s chat_id=%s chat_title=%s item=%s weight=%.2fkg location=%s",
                    user_id,
                    chat_id,
                    update.effective_chat.title or "",
                    trash['name'],
                    trash['weight'],
                    location
                )
                
                catch_results.append({
                    'type': 'trash',
                    'name': trash['name'],
                    'weight': trash['weight'],
                    'price': trash['price']
                })
                total_value += trash['price']
            elif available_fish:
                # Ловим рыбу — с весами по редкости (легенда/миф бьётся реже)
                _RARITY_WEIGHTS = {
                    'Обычная':    100,
                    'Редкая':      20,
                    'Легендарная':  0.5,
                    'Мифическая':   0.001,
                }
                _weights = [_RARITY_WEIGHTS.get(f.get('rarity', 'Обычная'), 100) for f in available_fish]
                fish = random.choices(available_fish, weights=_weights, k=1)[0]
                # Генерируем вес и длину рыбы
                weight = round(random.uniform(fish['min_weight'], fish['max_weight']), 2)
                length = round(random.uniform(fish['min_length'], fish['max_length']), 1)
                
                # Добавляем рыбу в улов игрока
                db.add_caught_fish(user_id, chat_id, fish['name'], weight, location, length)

                logger.info(
                    "Net catch (fish): user=%s chat_id=%s chat_title=%s fish=%s weight=%.2fkg length=%.1fcm location=%s",
                    user_id,
                    chat_id,
                    update.effective_chat.title or "",
                    fish['name'],
                    weight,
                    length,
                    location
                )
                
                fish_price = db.calculate_fish_price(fish, weight, length)

                catch_results.append({
                    'type': 'fish',
                    'name': fish['name'],
                    'weight': weight,
                    'length': length,
                    'price': fish_price
                })
                total_value += fish_price
        
        # Используем сеть
        db.use_net(user_id, net_name, chat_id)
        
        # Формируем сообщение
        message = f"🕸️ Сеть '{net_name}' использована!\n"
        message += f"📍 Локация: {location}\n"
        message += f"📦 Улов: {len(catch_results)} предметов\n\n"
        message += "─" * 30 + "\n"
        
        for i, item in enumerate(catch_results, 1):
            if item['type'] == 'fish':
                fish_name_display = format_fish_name(item['name'])
                message += f"{i}. {fish_name_display} - {item['weight']}кг, {item['length']}см\n"
            else:
                message += f"{i}. {item['name']} - {item['weight']}кг\n"
        
        message += "─" * 30 + "\n"
        message += f"💰 Итого: {total_value} {COIN_NAME}\n"
        if feeder_bonus > 0:
            message += f"🧺 Бонус кормушки: +{feeder_bonus}% (рыба {fish_chance}%)\n"
        
        # Обновляем оставшиеся использования
        player_net = db.get_player_net(user_id, net_name, chat_id)
        if player_net['max_uses'] != -1:
            message += f"🕸️ Осталось использований: {player_net['uses_left']}"
        
        # Добавляем кнопки
        keyboard = [
            [InlineKeyboardButton("🔙 Меню", callback_data=f"back_to_menu_{user_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message, reply_markup=reply_markup)

    async def handle_select_rod(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка выбора удочки"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        # Поддержка форматов: select_rod_{name}_{user_id}, sr_{rod_id}_{user_id}, sr_bamboo_{user_id}
        rod_name = None
        
        if query.data.startswith("select_rod_"):
            # Формат: select_rod_{name}_{user_id}
            rod_name = query.data.replace("select_rod_", "").replace(f"_{user_id}", "")
        elif query.data.startswith("sr_"):
            # Формат: sr_{rod_id}_{user_id} или sr_bamboo_{user_id}
            parts = query.data.split('_')
            if parts[1] == "bamboo":
                rod_name = "Бамбуковая удочка"
            else:
                try:
                    rod_id = int(parts[1])
                    rod = db.get_rod_by_id(rod_id)
                    if rod:
                        rod_name = rod['name']
                except (ValueError, IndexError):
                    pass
        
        if not rod_name:
            await query.edit_message_text("❌ Удочка не найдена!")
            return

        if rod_name == HARPOON_NAME:
            await query.edit_message_text(
                "🗡️ Гарпун больше не выбирается как удочка.\n"
                "Используйте кнопку гарпуна в меню выбора удочки."
            )
            return
        
        # Проверяем, что удочка есть у игрока (или бамбуковая)
        if rod_name != "Бамбуковая удочка":
            # Нужно проверить, куплена ли удочка
            player_rod = db.get_player_rod(user_id, rod_name, chat_id)
            if not player_rod:
                await query.edit_message_text("❌ Эта удочка не куплена!")
                return
        else:
            # Инициализируем бамбуковую удочку если её нет
            db.init_player_rod(user_id, "Бамбуковая удочка", chat_id)
        
        # Обновляем удочку игрока
        db.update_player(user_id, chat_id, current_rod=rod_name)
        
        # Возвращаемся в меню выбора удочек с подтверждением
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"change_rod_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(f"✅ Удочка '{rod_name}' выбрана!", reply_markup=reply_markup)

    async def handle_use_harpoon(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Использование гарпуна как отдельного инструмента."""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        harpoon_owned = db.get_player_rod(user_id, HARPOON_NAME, chat_id)
        if not harpoon_owned:
            await query.edit_message_text(
                "❌ У вас нет гарпуна. Купите его в магазине.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🛒 Магазин", callback_data=f"shop_rods_{user_id}")]])
            )
            return

        remaining = db.get_harpoon_cooldown_remaining(user_id, chat_id, HARPOON_COOLDOWN_MINUTES)
        if remaining > 0:
            keyboard = [
                [InlineKeyboardButton(
                    f"⭐ Пропустить КД за {HARPOON_SKIP_COST_STARS} Stars",
                    callback_data=f"use_harpoon_paid_{user_id}"
                )],
                [InlineKeyboardButton("🔙 Назад", callback_data=f"change_rod_{user_id}")]
            ]
            await query.edit_message_text(
                (
                    f"🗡️ Гарпун на перезарядке: {self._format_seconds_compact(remaining)}\n\n"
                    f"Можно подождать {HARPOON_COOLDOWN_MINUTES} минут или оплатить {HARPOON_SKIP_COST_STARS} Telegram Stars.\n"
                    "Пока идет КД гарпуна, вы можете спокойно рыбачить обычной удочкой."
                ),
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return

        await self._execute_harpoon_catch(user_id, chat_id, reply_to_message_id=query.message.message_id)

    async def handle_net_skip_cd(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка кнопки сброса КД сетей за 10 звезд."""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        # Проверяем, что хотя бы одна сеть действительно на КД
        player_nets = db.get_player_nets(user_id, chat_id)
        any_on_cooldown = any(
            db.get_net_cooldown_remaining(user_id, net['net_name'], chat_id) > 0
            for net in player_nets
        )
        if not any_on_cooldown:
            await query.answer("✅ Все сети уже свободны!", show_alert=True)
            return

        from config import BOT_TOKEN, STAR_NAME
        tg_api = TelegramBotAPI(BOT_TOKEN)
        payload = f"net_skip_cd_{user_id}_{chat_id}_{int(datetime.now().timestamp())}"

        invoice_url = await tg_api.create_invoice_link(
            title="Сброс КД сетей",
            description="Мгновенный сброс кулдауна всех ваших сетей (10 ⭐)",
            payload=payload,
            currency="XTR",
            prices=[{"label": "Сброс КД сетей", "amount": 10}],
        )

        if not invoice_url:
            await query.edit_message_text("❌ Не удалось создать ссылку оплаты. Попробуйте позже.")
            return

        await self.send_invoice_url_button(
            chat_id=chat_id,
            invoice_url=invoice_url,
            text="⚡ Оплатите 10 Telegram Stars чтобы сбросить кулдаун всех сетей и использовать их сразу.",
            user_id=user_id,
            reply_to_message_id=query.message.message_id if query.message else None,
        )

    async def handle_use_harpoon_paid(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Оплата пропуска КД гарпуна через Telegram Stars."""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        harpoon_owned = db.get_player_rod(user_id, HARPOON_NAME, chat_id)
        if not harpoon_owned:
            await query.edit_message_text("❌ У вас нет гарпуна. Купите его в магазине.")
            return

        remaining = db.get_harpoon_cooldown_remaining(user_id, chat_id, HARPOON_COOLDOWN_MINUTES)
        if remaining <= 0:
            await self._execute_harpoon_catch(user_id, chat_id, reply_to_message_id=query.message.message_id)
            return

        from config import BOT_TOKEN, STAR_NAME
        tg_api = TelegramBotAPI(BOT_TOKEN)
        payload = self._build_harpoon_skip_payload(user_id, chat_id)

        invoice_url = await tg_api.create_invoice_link(
            title="Пропуск КД гарпуна",
            description=f"Мгновенное использование гарпуна без ожидания ({HARPOON_SKIP_COST_STARS} {STAR_NAME})",
            payload=payload,
            currency="XTR",
            prices=[{"label": "Пропуск КД гарпуна", "amount": HARPOON_SKIP_COST_STARS}],
        )

        if not invoice_url:
            await query.edit_message_text("❌ Не удалось создать ссылку оплаты. Попробуйте позже.")
            return

        await self.send_invoice_url_button(
            chat_id=chat_id,
            invoice_url=invoice_url,
            text=f"⭐ Оплатите {HARPOON_SKIP_COST_STARS} Telegram Stars для мгновенного использования гарпуна.",
            user_id=user_id,
            reply_to_message_id=query.message.message_id if query and query.message else None,
            timeout_sec=600,
        )

        await query.edit_message_text("Ссылка на оплату отправлена. После оплаты гарпун сработает автоматически.")

    async def handle_instant_repair(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка мгновенного ремонта удочки"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        # Формат: instant_repair_{rod_name}_{user_id}
        rod_name = query.data.replace("instant_repair_", "").replace(f"_{user_id}", "")

        if rod_name in TEMP_ROD_RANGES:
            await query.edit_message_text("❌ Эта удочка одноразовая и не ремонтируется.")
            return
        
        # Получаем информацию об удочке
        player_rod = db.get_player_rod(user_id, rod_name, chat_id)
        if not player_rod:
            await query.edit_message_text("❌ Удочка не найдена!")
            return
        
        current_dur = player_rod['current_durability']
        max_dur = player_rod['max_durability']
        missing_durability = max_dur - current_dur
        
        if missing_durability <= 0:
            await query.edit_message_text("✅ Ваша удочка уже в идеальном состоянии!")
            return
        
        # Вычисляем стоимость
        repair_cost = max(1, int(20 * missing_durability / max_dur))
        
        # Отправляем инвойс на оплату
        await self.send_rod_repair_invoice(query, user_id, rod_name, repair_cost)
    
    async def send_rod_repair_invoice(self, query, user_id: int, rod_name: str, repair_cost: int):
        """Отправить инвойс на оплату ремонта удочки"""
        # Создаём invoice_url через TelegramBotAPI.create_invoice_link
        from config import BOT_TOKEN, STAR_NAME
        import traceback
        invoice_error = None
        try:
            from bot import TelegramBotAPI as _TelegramBotAPI
            tg_api = _TelegramBotAPI(BOT_TOKEN)
            logger.info(f"[INVOICE] Creating invoice link for repair: rod={rod_name}, user_id={user_id}, cost={repair_cost}")
            invoice_url = await tg_api.create_invoice_link(
                title=f"Мгновенный ремонт удочки",
                description=f"Восстановить '{rod_name}' до полной прочности",
                payload=f"repair_rod_{rod_name}_{user_id}_{int(datetime.now().timestamp())}",
                currency="XTR",
                prices=[{"label": f"Ремонт {rod_name}", "amount": repair_cost}]
            )
            logger.info(f"[INVOICE] Got invoice_url: {invoice_url}")
        except Exception as e:
            logger.error(f"[INVOICE] Failed to get invoice_url for repair: {e}")
            invoice_url = None
            invoice_error = str(e) + "\n" + traceback.format_exc()
        if invoice_url:
            await self.send_invoice_url_button(
                chat_id=query.message.chat_id,
                invoice_url=invoice_url,
                text=f"⭐ Оплатите {repair_cost} Telegram Stars для мгновенного восстановления удочки.",
                user_id=user_id,
                reply_to_message_id=query.message.message_id if query and query.message else None,
            )
        else:
            error_text = f"(Ошибка генерации ссылки для оплаты)"
            if invoice_error:
                error_text += f"\nОшибка: {invoice_error}"
            await query.edit_message_text(error_text, parse_mode=None)

        
    async def handle_back_to_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Возврат в главное меню"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        await self.show_fishing_menu(update, context)
    
    async def handle_shop_rods(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка магазина удочек"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_shop_rods")
            return
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()

        db.ensure_rod_catalog()
        rods = db.get_rods()
        keyboard = []
        player = db.get_player(user_id, chat_id)
        player_level = player.get('level', 0) if player else 0
        for rod in rods:
            # Гарпун только для 25+ уровня
            if rod['name'] == 'Гарпун' and player_level < 25:
                continue
            # Удачливая удочка только для 15+ уровня
            if rod['name'] == 'Удачливая удочка' and player_level < 15:
                continue
            keyboard.append([InlineKeyboardButton(
                f"🎣 {rod['name']} - {rod['price']} 🪙",
                callback_data=f"buy_rod_{rod['id']}_{user_id}"
            )])
        # Добавляем кнопку возврата в магазин
        keyboard.append([InlineKeyboardButton("🔙 Магазин", callback_data=f"shop_{user_id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = "🛒 Магазин удочек:"
        try:
            await query.edit_message_text(message, reply_markup=reply_markup)
        except Exception as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования магазина удочек: {e}")
    
    async def handle_buy_rod(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка покупки удочки"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_buy_rod")
            return
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        # Извлекаем ID удочки
        # Формат: buy_rod_{id}_{user_id}
        parts = query.data.split('_')
        rod_id = int(parts[2])
        
        await query.answer()
        db.ensure_rod_catalog()
        
        # Получаем название удочки по ID
        rods = db.get_rods()
        rod_name = None
        for rod in rods:
            if rod['id'] == rod_id:
                rod_name = rod['name']
                break
        
        if not rod_name:
            await query.edit_message_text("❌ Удочка не найдена!")
            return

        player = db.get_player(user_id, chat_id)
        player_level = int((player or {}).get('level', 0) or 0)
        if rod_name == 'Удачливая удочка' and player_level < 15:
            await query.edit_message_text("❌ Удачливая удочка открывается с 15 уровня.")
            return
        if rod_name == 'Гарпун' and player_level < 25:
            await query.edit_message_text("❌ Гарпун открывается с 25 уровня.")
            return
        
        # Покупаем удочку
        result = db.buy_rod(user_id, chat_id, rod_name)
        
        if result:
            await query.edit_message_text(f"✅ Удочка {rod_name} куплена!")
        else:
            await query.edit_message_text("❌ Недостаточно монет!")
    
    async def send_rod_repair_invoice(self, user_id: int, rod_name: str):
        """Отправить инвойс для восстановления удочки в личное сообщение"""
        try:
            rod = db.get_rod(rod_name)
            if not rod:
                logger.error(f"Rod not found: {rod_name}")
                return
            
            # Отправляем инвойс в ЛС
            prices = [LabeledPrice(label=f"Восстановление удочки '{rod_name}'", amount=20 * 100)]  # 20 звезд = 20 * 100 копеек
            
            await self.application.bot.send_invoice(
                chat_id=user_id,
                title=f"Восстановление удочки",
                description=f"Полное восстановление прочности удочки '{rod_name}'",
                payload=f"repair_rod_{rod_name}",
                provider_token="",  # Пусто для Telegram Stars
                currency="XTR",
                prices=prices,
                is_flexible=False
            )
            logger.info(f"Sent repair invoice for {rod_name} to user {user_id}")
        except Exception as e:
            logger.error(f"Error sending repair invoice to {user_id}: {e}")
    
    async def handle_shop_baits(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка магазина наживок - сначала выбор локации"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_shop_baits")
            return
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        if 'waiting_bait_quantity' in context.user_data:
            del context.user_data['waiting_bait_quantity']
        
        await query.answer()
        
        # Получаем все локации
        locations = db.get_locations()
        
        keyboard = []
        for idx, location in enumerate(locations):
            keyboard.append([InlineKeyboardButton(
                f"📍 {location['name']}",
                callback_data=f"shop_baits_loc_{idx}_{user_id}"
            )])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"shop_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = "🛒 Магазин наживок\n\nВыберите локацию:"
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup)
        except Exception as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования магазина наживок: {e}")
    
    async def handle_shop_baits_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать наживки для выбранной локации с пагинацией"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_shop_baits_location")
            return
        
        # Разбор: shop_baits_loc_{loc_idx}_{user_id}_{page}
        parts = query.data.split('_')
        loc_idx = int(parts[3])
        callback_user_id = int(parts[4])
        page = int(parts[5]) if len(parts) > 5 else 1
        
        # Проверка прав доступа
        if user_id != callback_user_id:
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        if 'waiting_bait_quantity' in context.user_data:
            del context.user_data['waiting_bait_quantity']
        
        # Получаем название локации по индексу
        locations = db.get_locations()
        if loc_idx >= len(locations):
            await query.edit_message_text("❌ Локация не найдена!")
            return
        location = locations[loc_idx]['name']
        
        await query.answer()
        
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        baits = db.get_baits_for_location(location)
        
        # Исключаем бесконечную наживку (черви) из магазина
        baits = [b for b in baits if b['name'].strip().lower() != 'черви']
        
        if not baits:
            await query.edit_message_text(f"❌ Нет наживок для локации {location}")
            return
        
        page_size = 5
        total_pages = max(1, (len(baits) + page_size - 1) // page_size)
        page = max(1, min(page, total_pages))
        start = (page - 1) * page_size
        end = start + page_size
        page_baits = baits[start:end]
        
        # Кнопки наживок с ценой
        keyboard = []
        for idx, bait in enumerate(page_baits):
            bait_id = bait.get('id')
            cb_data = f"select_bait_buy_{loc_idx}_{bait_id}_{user_id}"
            # Проверяем длину callback_data (максимум 64 байта)
            if len(cb_data.encode('utf-8')) > 64:
                logger.warning(f"Callback data too long: {cb_data}")
                cb_data = f"sb_{loc_idx}_{bait_id}_{user_id}"
            
            keyboard.append([InlineKeyboardButton(
                f"🪱 {bait['name']} - {bait['price']} 🪙",
                callback_data=cb_data
            )])
        
        # Навигация
        nav_buttons = []
        if total_pages > 1:
            prev_page = page - 1 if page > 1 else total_pages
            next_page = page + 1 if page < total_pages else 1
            nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"shop_baits_loc_{loc_idx}_{user_id}_{prev_page}"))
        
        nav_buttons.append(InlineKeyboardButton("🔙 Назад", callback_data=f"shop_baits_{user_id}"))
        
        if total_pages > 1:
            nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"shop_baits_loc_{loc_idx}_{user_id}_{next_page}"))
        
        keyboard.append(nav_buttons)
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"🛒 Наживки для {location} ({page}/{total_pages})\n💰 Баланс: {player['coins']} 🪙"
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"Error editing shop_baits_location: {e}")
            if "Message is not modified" not in str(e):
                # Попробуем отправить как обычное сообщение
                try:
                    await context.bot.send_message(chat_id=update.effective_chat.id, text=message, reply_markup=reply_markup)
                except Exception as e2:
                    logger.error(f"Failed to send as new message too: {e2}")
    
    async def handle_shop_nets(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка магазина сетей"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_shop_nets")
            return
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        nets = db.get_nets()
        nets_for_sale = [net for net in nets if net.get('price', 0) > 0]
        
        keyboard = []
        
        for net in nets_for_sale:
            # Проверяем, есть ли сеть у игрока
            player_net = db.get_player_net(user_id, net['name'], chat_id)
            
            if player_net:
                # Сеть уже куплена - показываем количество использований
                if net['max_uses'] == -1:
                    status = "✅ Бесконечная"
                else:
                    status = f"✅ ({player_net['uses_left']} исп.)"
                button_text = f"🕸️ {net['name']} - {status}"
                callback_data = f"buy_net_{net['name']}_{user_id}"  # Можно докупить
            else:
                # Сеть не куплена
                button_text = f"🕸️ {net['name']} - {net['price']} 🪙"
                callback_data = f"buy_net_{net['name']}_{user_id}"
            
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        keyboard.append([InlineKeyboardButton("🔙 Магазин", callback_data=f"shop_{user_id}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"🛒 Магазин сетей\n💰 Баланс: {player['coins']} 🪙\n\n"
        message += "🕸️ Сети позволяют ловить несколько рыб за раз!\n\n"
        
        for net in nets_for_sale:
            message += f"• {net['name']}: {net['fish_count']} рыб, кулдаун {net['cooldown_hours']}ч"
            if net['max_uses'] == -1:
                message += " (∞ использований)"
            else:
                message += f" ({net['max_uses']} использований)"
            message += f", цена {net['price']} 🪙"
            message += "\n"
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup)
        except Exception as e:
            if "Message is not modified" not in str(e):
                logger.error(f"Ошибка редактирования магазина сетей: {e}")

    async def handle_shop_feeders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Магазин кормушек и эхолота."""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        player = db.get_player(user_id, chat_id)
        if not player:
            await query.edit_message_text("❌ Профиль не найден. Используйте /start")
            return

        active_feeder = db.get_active_feeder(user_id, chat_id)
        feeder_remaining = db.get_feeder_cooldown_remaining(user_id, chat_id)
        echosounder_remaining = db.get_echosounder_remaining_seconds(user_id, chat_id)

        keyboard = []
        for feeder in FEEDER_ITEMS:
            if feeder["price_stars"] > 0:
                price_label = f"{feeder['price_stars']} ⭐"
                callback_data = f"buy_feeder_stars_{feeder['code']}_{user_id}"
            else:
                price_label = f"{feeder['price_coins']} 🪙"
                callback_data = f"buy_feeder_coins_{feeder['code']}_{user_id}"

            keyboard.append([
                InlineKeyboardButton(
                    f"🧺 {feeder['name']} (+{feeder['bonus']}% на 1ч) — {price_label}",
                    callback_data=callback_data,
                )
            ])

        keyboard.append([
            InlineKeyboardButton(
                f"📡 Эхолот (24ч) — {ECHOSOUNDER_COST_STARS} ⭐",
                callback_data=f"buy_echosounder_{user_id}",
            )
        ])
        keyboard.append([InlineKeyboardButton("🔙 Магазин", callback_data=f"shop_{user_id}")])

        status_lines = [f"💰 Баланс: {player.get('coins', 0)} 🪙"]
        if active_feeder:
            status_lines.append(
                f"🧺 Активна: +{active_feeder['bonus_percent']}% ({self._format_seconds_compact(feeder_remaining)})"
            )
        else:
            status_lines.append("🧺 Кормушка не активна")

        if echosounder_remaining > 0:
            status_lines.append(f"📡 Эхолот активен: {self._format_seconds_compact(echosounder_remaining)}")
        else:
            status_lines.append("📡 Эхолот не активен")

        message = (
            "🛒 Кормушки и эхолот\n\n"
            "Кормушка усиливает клёв для обычных, платных и сетевых забросов.\n"
            "Пока активна одна кормушка — другие купить нельзя.\n\n"
            + "\n".join(status_lines)
        )

        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_buy_feeder_coins(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Покупка кормушки за монеты."""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        feeder_code = query.data.replace("buy_feeder_coins_", "").replace(f"_{user_id}", "")
        feeder = self._get_feeder_by_code(feeder_code)
        if not feeder or feeder.get("price_coins", 0) <= 0:
            await query.edit_message_text("❌ Кормушка не найдена.")
            return

        active_feeder = db.get_active_feeder(user_id, chat_id)
        if active_feeder:
            remaining = db.get_feeder_cooldown_remaining(user_id, chat_id)
            await query.answer(
                f"Сначала дождитесь окончания активной кормушки ({self._format_seconds_compact(remaining)})",
                show_alert=True,
            )
            return

        player = db.get_player(user_id, chat_id)
        if not player:
            await query.edit_message_text("❌ Профиль не найден.")
            return

        price = int(feeder["price_coins"])
        if int(player.get("coins", 0)) < price:
            await query.edit_message_text(
                f"❌ Недостаточно монет. Нужно: {price} 🪙, у вас: {player.get('coins', 0)} 🪙"
            )
            return

        db.update_player(user_id, chat_id, coins=int(player.get("coins", 0)) - price)
        db.activate_feeder(
            user_id,
            chat_id,
            feeder_type=feeder["code"],
            bonus_percent=int(feeder["bonus"]),
            duration_minutes=int(feeder["duration_minutes"]),
        )

        await query.edit_message_text(
            f"✅ {feeder['name']} активирована на 1 час.\n"
            f"🎯 Бонус к клёву: +{feeder['bonus']}%\n"
            f"💰 Потрачено: {price} 🪙"
        )

    async def handle_buy_feeder_stars(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Покупка кормушки за Telegram Stars (через инвойс)."""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        feeder_code = query.data.replace("buy_feeder_stars_", "").replace(f"_{user_id}", "")
        feeder = self._get_feeder_by_code(feeder_code)
        if not feeder or feeder.get("price_stars", 0) <= 0:
            await query.edit_message_text("❌ Кормушка не найдена.")
            return

        active_feeder = db.get_active_feeder(user_id, chat_id)
        if active_feeder:
            remaining = db.get_feeder_cooldown_remaining(user_id, chat_id)
            await query.answer(
                f"Сначала дождитесь окончания активной кормушки ({self._format_seconds_compact(remaining)})",
                show_alert=True,
            )
            return

        tg_api = TelegramBotAPI(BOT_TOKEN)
        payload = self._build_booster_payload(feeder["code"], user_id, chat_id)
        invoice_url = await tg_api.create_invoice_link(
            title=feeder["name"],
            description=f"Активация кормушки +{feeder['bonus']}% на 1 час",
            payload=payload,
            currency="XTR",
            prices=[{"label": feeder["name"], "amount": int(feeder["price_stars"])}],
        )

        if not invoice_url:
            await query.edit_message_text("❌ Не удалось создать ссылку оплаты. Попробуйте позже.")
            return

        await self.send_invoice_url_button(
            chat_id=chat_id,
            invoice_url=invoice_url,
            text=(
                f"⭐ Оплатите {feeder['price_stars']} Telegram Stars для активации {feeder['name']} "
                f"(+{feeder['bonus']}% на 1 час)."
            ),
            user_id=user_id,
            timeout_sec=900,
            reply_to_message_id=query.message.message_id if query and query.message else None,
        )

        await query.edit_message_text("Ссылка на оплату отправлена. После оплаты кормушка активируется автоматически.")

    async def handle_buy_echosounder(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Покупка эхолота за Telegram Stars."""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        remaining = db.get_echosounder_remaining_seconds(user_id, chat_id)
        if remaining > 0:
            await query.answer(
                f"Эхолот уже активен: {self._format_seconds_compact(remaining)}",
                show_alert=True,
            )
            return

        tg_api = TelegramBotAPI(BOT_TOKEN)
        payload = self._build_booster_payload(ECHOSOUNDER_CODE, user_id, chat_id)
        invoice_url = await tg_api.create_invoice_link(
            title="Эхолот",
            description="Активация эхолота на 24 часа",
            payload=payload,
            currency="XTR",
            prices=[{"label": "Эхолот 24ч", "amount": ECHOSOUNDER_COST_STARS}],
        )

        if not invoice_url:
            await query.edit_message_text("❌ Не удалось создать ссылку оплаты. Попробуйте позже.")
            return

        await self.send_invoice_url_button(
            chat_id=chat_id,
            invoice_url=invoice_url,
            text=f"⭐ Оплатите {ECHOSOUNDER_COST_STARS} Telegram Stars для активации эхолота на 24 часа.",
            user_id=user_id,
            timeout_sec=900,
            reply_to_message_id=query.message.message_id if query and query.message else None,
        )

        await query.edit_message_text("Ссылка на оплату отправлена. После оплаты эхолот активируется автоматически.")
    
    async def handle_buy_net(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка покупки сети"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_buy_net")
            return
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        # Формат: buy_net_{net_name}_{user_id}
        parts = query.data.split('_')
        net_name = '_'.join(parts[2:-1])  # Все части между buy_net и user_id
        
        await query.answer()
        
        chat_id = update.effective_chat.id
        # Покупаем сеть
        result = db.buy_net(user_id, net_name, chat_id)
        
        if result:
            net = db.get_net(net_name)
            message = f"✅ Сеть '{net_name}' куплена!\n\n"
            message += f"🐟 Вытаскивает: {net['fish_count']} рыб\n"
            message += f"⏰ Кулдаун: {net['cooldown_hours']} часов\n"
            if net['max_uses'] == -1:
                message += "♾️ Использований: бесконечно"
            else:
                player_net = db.get_player_net(user_id, net_name, chat_id)
                message += f"📦 Использований: {player_net['uses_left']}"
        else:
            player = db.get_player(user_id, chat_id)
            net = db.get_net(net_name)
            if not net:
                message = "❌ Сеть не найдена!"
            elif player['coins'] < net['price']:
                message = f"❌ Недостаточно монет!\nНужно: {net['price']} 🪙\nУ вас: {player['coins']} 🪙"
            else:
                message = "❌ Эта сеть уже куплена (бесконечная)!"
        
        keyboard = [[InlineKeyboardButton("🔙 Магазин сетей", callback_data=f"shop_nets_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message, reply_markup=reply_markup)
    
    async def handle_select_bait_buy(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выбор наживки для покупки - запрос количества"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            return
        
        # Разбор: select_bait_buy_{loc_idx}_{bait_id}_{user_id} или sb_{loc_idx}_{bait_id}_{user_id}
        parts = query.data.split('_')
        if parts[0] == 'sb':
            # Короткий формат: sb_{loc_idx}_{bait_idx}_{user_id}
            loc_idx = int(parts[1])
            bait_id = int(parts[2])
            button_user_id = int(parts[3])
        else:
            # Полный формат: select_bait_buy_{loc_idx}_{bait_idx}_{user_id}
            loc_idx = int(parts[3])
            bait_id = int(parts[4])
            button_user_id = int(parts[5])
        
        # Проверка прав доступа
        if user_id != button_user_id:
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        # Получаем локацию
        locations = db.get_locations()
        if loc_idx >= len(locations):
            await query.edit_message_text("❌ Локация не найдена!")
            return
        location = locations[loc_idx]['name']
        
        # Получаем наживку
        baits = db.get_baits_for_location(location)
        baits = [b for b in baits if b['name'].strip().lower() != 'черви']
        bait = next((b for b in baits if b.get('id') == bait_id), None)
        if not bait:
            await query.edit_message_text("❌ Наживка не найдена!")
            return
        
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        
        # Рассчитываем максимальное количество
        max_qty = min(999, player['coins'] // bait['price'])
        
        if max_qty == 0:
            await query.edit_message_text(f"❌ Недостаточно монет для покупки {bait['name']}!\n\nЦена: {bait['price']} 🪙\nВаш баланс: {player['coins']} 🪙")
            return
        
        # Сохраняем состояние в context.user_data
        context.user_data['waiting_bait_quantity'] = {
            'bait_name': bait['name'],
            'loc_idx': loc_idx,
            'price': bait['price'],
            'max_qty': max_qty,
            'balance': player['coins']
        }
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data=f"shop_baits_loc_{loc_idx}_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = f"""🪱 {bait['name']}

💰 Цена: {bait['price']} 🪙
💰 Ваш баланс: {player['coins']} 🪙
📦 Максимум: {max_qty} шт

✍️ Напишите в чат количество для покупки (1-{max_qty}):"""
        
        try:
            logger.info(f"Showing bait buy prompt for {bait['name']}, callback_data: {query.data}")
            await query.edit_message_text(
                message,
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.error(f"Error in handle_select_bait_buy: {e}")
            logger.error(f"Callback data: {query.data}")
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=message,
                    reply_markup=reply_markup
                )
            except Exception as e2:
                logger.error(f"Failed to send as new message: {e2}")
    
    async def handle_buy_bait(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка покупки наживки - обработка текстового ввода количества"""
        # Проверяем, ждём ли мы ввод количества от этого пользователя
        if 'waiting_bait_quantity' not in context.user_data:
            return  # Не обрабатываем, если не ждём ввода
        
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_buy_bait")
            return
        
        # Получаем данные из context
        bait_data = context.user_data['waiting_bait_quantity']
        bait_name = bait_data['bait_name']
        price = bait_data['price']
        max_qty = bait_data['max_qty']
        
        # Получаем текст сообщения
        message = update.effective_message
        if not message or not message.text:
            return
        text = message.text.strip()
        
        # Проверяем, что это число
        try:
            qty = int(text)
        except ValueError:
            await update.message.reply_text(f"❌ Введите число от 1 до {max_qty}!")
            return
        
        # Проверяем диапазон
        if qty < 1 or qty > max_qty:
            await update.message.reply_text(f"❌ Количество должно быть от 1 до {max_qty}!")
            return
        
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        total_cost = price * qty
        
        if player['coins'] < total_cost:
            await update.message.reply_text(f"❌ Недостаточно монет!\n\nНужно: {total_cost} 🪙\nУ вас: {player['coins']} 🪙")
            return
        
        # Покупаем
        db.add_bait_to_inventory(user_id, bait_name, qty)
        db.update_player(user_id, chat_id, coins=player['coins'] - total_cost)
        
        # Автоматически применяем купленную наживку
        db.update_player_bait(user_id, chat_id, bait_name)
        
        new_balance = player['coins'] - total_cost
        
        # Очищаем состояние
        del context.user_data['waiting_bait_quantity']
        
        await update.message.reply_text(
            f"✅ Куплено: {bait_name} x{qty}\n"
            f"🪱 Наживка автоматически применена!\n\n"
            f"💰 Потрачено: {total_cost} 🪙\n"
            f"💰 Баланс: {new_balance} 🪙"
        )
    
    async def handle_shop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка магазина"""
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except AttributeError:
            logger.error("update.effective_user not found or id not accessible")
            return
        
        # Проверяем, это callback query или command
        if update.callback_query:
            query = update.callback_query
            # Проверка прав доступа
            if not query.data.endswith(f"_{user_id}"):
                await query.answer("Эта кнопка не для вас", show_alert=True)
                return
            await query.answer()
            is_callback = True
        else:
            # Это текстовая команда /shop
            is_callback = False
            query = None
        
        keyboard = [
            [InlineKeyboardButton("🎣 Удочки", callback_data=f"shop_rods_{user_id}")],
            [InlineKeyboardButton("🪱 Наживки", callback_data=f"shop_baits_{user_id}")],
            [InlineKeyboardButton("🕸️ Сети", callback_data=f"shop_nets_{user_id}")],
            [InlineKeyboardButton("🧺 Кормушки и эхолот", callback_data=f"shop_feeders_{user_id}")],
            [InlineKeyboardButton("💎 Обменник", callback_data=f"shop_exchange_{user_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = "🛒 Магазин:\n\nВыберите категорию:"
        
        if is_callback:
            try:
                await query.edit_message_text(message, reply_markup=reply_markup)
            except Exception as e:
                # Если сообщение уже отредактировано с тем же контентом, просто ничего не делаем
                if "Message is not modified" not in str(e):
                    logger.error(f"Ошибка редактирования сообщения магазина: {e}")
        else:
            # Это команда, отправляем новое сообщение
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message,
                reply_markup=reply_markup
            )
    
    async def handle_buy_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка покупки"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        callback_data = query.data
        
        # Проверка прав доступа
        if not callback_data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        if callback_data.startswith("buy_rod_"):
            # Извлекаем ID удочки (убираем префикс и user_id)
            # Формат: buy_rod_{id}_{user_id}
            parts = callback_data.split('_')
            rod_id = int(parts[2])
            
            # Получаем название удочки по ID
            rods = db.get_rods()
            rod_name = None
            for rod in rods:
                if rod['id'] == rod_id:
                    rod_name = rod['name']
                    break
            
            if not rod_name:
                await query.edit_message_text("❌ Удочка не найдена!")
                return
            
            # Покупаем удочку
            result = db.buy_rod(user_id, chat_id, rod_name)
            
            if result:
                await query.edit_message_text(f"✅ Удочка {rod_name} куплена!")
            else:
                await query.edit_message_text("❌ Недостаточно монет!")
        elif callback_data.startswith("buy_bait_"):
            # Извлекаем ID наживки (убираем префикс и user_id)
            # Формат: buy_bait_{id}_{user_id}
            parts = callback_data.split('_')
            bait_id = int(parts[2])
            
            # Получаем название наживки по ID
            baits = db.get_baits()
            bait_name = None
            for bait in baits:
                if bait['id'] == bait_id:
                    bait_name = bait['name']
                    break
            
            if not bait_name:
                await query.edit_message_text("❌ Наживка не найдена!")
                return

            if bait_name.strip().lower() == 'черви':
                await query.edit_message_text("❌ Черви бесконечные и не продаются.")
                return
            
            # Покупаем наживку
            result = db.add_bait_to_inventory(user_id, bait_name)
            
            if result:
                await query.edit_message_text(f"✅ Наживка {bait_name} куплена!")
            else:
                await query.edit_message_text("❌ Недостаточно монет!")
        else:
            await query.edit_message_text("❌ Неизвестный товар!")
    
    async def handle_repair_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка починки удочки"""
        query = update.callback_query
        
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        player = db.get_player(user_id, chat_id)
        if player:
            if player['current_rod'] in TEMP_ROD_RANGES:
                await query.edit_message_text("❌ Эта удочка одноразовая и не ремонтируется.")
                return
            db.repair_rod(user_id, player['current_rod'], chat_id)
            await query.edit_message_text("✅ Удочка починена!")
        else:
            await query.edit_message_text("❌ Ошибка: профиль не найден!")

    def _canonical_treasure_name(self, treasure_name: str) -> str:
        """Normalize treasure name so exchange can combine legacy/variant labels."""
        raw = (treasure_name or "").strip().lower()
        # Keep only letters/spaces to ignore emoji/punctuation noise in old rows.
        cleaned = re.sub(r"[^a-zа-яё\s]", "", raw, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        if "ракуш" in cleaned or "shell" in cleaned:
            return "Ракушка"
        if "жемч" in cleaned or "pearl" in cleaned:
            return "Жемчуг"
        return treasure_name or ""

    def _get_treasure_total(self, treasures: List[Dict[str, Any]], canonical_name: str) -> int:
        total = 0
        for item in treasures:
            name = item.get('treasure_name', '')
            qty = int(item.get('quantity', 0) or 0)
            if qty > 0 and self._canonical_treasure_name(name) == canonical_name:
                total += qty
        return total

    def _consume_treasure_total(self, user_id: int, chat_id: int, treasures: List[Dict[str, Any]], canonical_name: str, amount: int) -> int:
        """Remove amount from matching treasure variants. Returns removed quantity."""
        remaining = int(amount)
        removed = 0
        for item in treasures:
            if remaining <= 0:
                break

            name = item.get('treasure_name', '')
            qty = int(item.get('quantity', 0) or 0)
            if qty <= 0:
                continue
            if self._canonical_treasure_name(name) != canonical_name:
                continue

            take = min(qty, remaining)
            db.remove_treasure(user_id, chat_id, name, take)
            remaining -= take
            removed += take

        return removed
    
    async def handle_shop_exchange(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обменник драгоценностей и монет"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        player = db.get_player(user_id, chat_id)
        if not player:
            await query.edit_message_text("❌ Профиль не найден. Используйте /start")
            return

        # Получаем количество бриллиантов и монет
        diamonds = int(player.get('diamonds') or 0)
        coins = player.get('coins', 0)
        treasures = db.get_player_treasures(user_id, chat_id)

        shells_count = self._get_treasure_total(treasures, 'Ракушка')
        pearls_count = self._get_treasure_total(treasures, 'Жемчуг')

        keyboard = [
            [InlineKeyboardButton(
                "💎 Купить бриллиант (500k монет)", 
                callback_data=f"exchange_buy_diamond_{user_id}"
            )],
            [InlineKeyboardButton(
                "💎 Продать бриллиант (250k монет)", 
                callback_data=f"exchange_sell_diamond_{user_id}"
            )],
            [InlineKeyboardButton(
                "🐚 x10 -> 💎 Жемчуг x1",
                callback_data=f"exchange_shell_to_pearl_{user_id}"
            )],
            [InlineKeyboardButton(
                "💎 Жемчуг x100 -> Бриллиант x1",
                callback_data=f"exchange_pearl_to_diamond_{user_id}"
            )],
            [InlineKeyboardButton("🔙 Магазин", callback_data=f"shop_{user_id}")]
        ]

        message = f"""
💎 Обменник драгоценностей

Ваши ресурсы:
💰 Монеты: {coins:,} 🪙
💎 Бриллианты: {diamonds}
🐚 Ракушка: {shells_count}
🦪 Жемчуг: {pearls_count}

📊 Курсы обмена:
💎 1 Бриллиант = 500,000 монет (покупка)
💎 1 Бриллиант = 250,000 монет (продажа)
🐚 10 Ракушек = 1 Жемчуг
🦪 100 Жемчуга = 1 Бриллиант

Выберите операцию:
        """

        await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard))

    async def handle_exchange_buy_diamond(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Покупка бриллиантов за монеты"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        player = db.get_player(user_id, chat_id)
        if not player:
            await query.edit_message_text("❌ Профиль не найден.")
            return

        coins = player.get('coins', 0)

        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К обменнику", callback_data=f"shop_exchange_{user_id}")]
        ])

        if coins < DIAMOND_BUY_PRICE:
            needed = DIAMOND_BUY_PRICE - coins
            await query.edit_message_text(
                f"❌ Недостаточно монет\n\n"
                f"Нужно: {DIAMOND_BUY_PRICE:,} 🪙\n"
                f"У вас: {coins:,} 🪙\n"
                f"Не хватает: {needed:,} 🪙",
                reply_markup=back_keyboard
            )
            return

        # Списываем монеты и добавляем бриллиант
        db.update_player(user_id, chat_id, coins=coins - DIAMOND_BUY_PRICE)
        db.add_diamonds(user_id, chat_id, 1)

        new_coins = coins - DIAMOND_BUY_PRICE
        new_diamonds = player.get('diamonds', 0) + 1

        await query.edit_message_text(
            f"✅ Успешная покупка!\n\n"
            f"Потрачено: {DIAMOND_BUY_PRICE:,} 🪙\n"
            f"Получено: 1 💎\n\n"
            f"💰 Новый баланс: {new_coins:,} 🪙\n"
            f"💎 Бриллианты: {new_diamonds}",
            reply_markup=back_keyboard
        )

    async def handle_exchange_sell_diamond(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Продажа бриллиантов за монеты"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        player = db.get_player(user_id, chat_id)
        if not player:
            await query.edit_message_text("❌ Профиль не найден.")
            return

        diamonds = player.get('diamonds', 0)
        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К обменнику", callback_data=f"shop_exchange_{user_id}")]
        ])

        if diamonds <= 0:
            await query.edit_message_text(
                "❌ У вас нет бриллиантов для продажи.",
                reply_markup=back_keyboard
            )
            return

        # Добавляем монеты и вычитаем бриллиант
        coins = player.get('coins', 0)
        db.update_player(user_id, chat_id, coins=coins + DIAMOND_SELL_PRICE)
        db.subtract_diamonds(user_id, chat_id, 1)

        new_coins = coins + DIAMOND_SELL_PRICE
        new_diamonds = diamonds - 1

        await query.edit_message_text(
            f"✅ Успешно продано!\n\n"
            f"Получено: {DIAMOND_SELL_PRICE:,} 🪙\n"
            f"Продано: 1 💎\n\n"
            f"💰 Новый баланс: {new_coins:,} 🪙\n"
            f"💎 Бриллианты: {new_diamonds}",
            reply_markup=back_keyboard
        )

    async def handle_exchange_shell_to_pearl(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обмен 10 ракушек на 1 жемчуг"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К обменнику", callback_data=f"shop_exchange_{user_id}")]
        ])

        treasures = db.get_player_treasures(user_id, chat_id)
        shells = self._get_treasure_total(treasures, 'Ракушка')

        if shells < 10:
            await query.edit_message_text(
                f"❌ Недостаточно ракушек\n\n"
                f"Нужно: 10 🐚\n"
                f"У вас: {shells} 🐚",
                reply_markup=back_keyboard
            )
            return

        removed_shells = self._consume_treasure_total(user_id, chat_id, treasures, 'Ракушка', 10)
        if removed_shells < 10:
            await query.edit_message_text(
                "❌ Не удалось списать нужное количество ракушек. Попробуйте снова.",
                reply_markup=back_keyboard
            )
            return

        db.add_treasure(user_id, 'Жемчуг', 1, chat_id)

        await query.edit_message_text(
            f"✅ Обмен выполнен!\n\n"
            f"Списано: 10 🐚 Ракушка\n"
            f"Получено: 1 🦪 Жемчуг",
            reply_markup=back_keyboard
        )

    async def handle_exchange_pearl_to_diamond(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обмен 100 жемчуга на 1 бриллиант"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id

        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        await query.answer()

        back_keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 К обменнику", callback_data=f"shop_exchange_{user_id}")]
        ])

        treasures = db.get_player_treasures(user_id, chat_id)
        pearls = self._get_treasure_total(treasures, 'Жемчуг')

        if pearls < 100:
            await query.edit_message_text(
                f"❌ Недостаточно жемчуга\n\n"
                f"Нужно: 100 🦪\n"
                f"У вас: {pearls} 🦪",
                reply_markup=back_keyboard
            )
            return

        removed_pearls = self._consume_treasure_total(user_id, chat_id, treasures, 'Жемчуг', 100)
        if removed_pearls < 100:
            await query.edit_message_text(
                "❌ Не удалось списать нужное количество жемчуга. Попробуйте снова.",
                reply_markup=back_keyboard
            )
            return

        db.add_diamonds(user_id, chat_id, 1)

        await query.edit_message_text(
            f"✅ Обмен выполнен!\n\n"
            f"Списано: 100 🦪 Жемчуг\n"
            f"Получено: 1 💎 Бриллиант",
            reply_markup=back_keyboard
        )
    
    async def handle_sell_fish(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка лавки продажи рыбы"""
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_sell_fish")
            return

        if update.callback_query:
            query = update.callback_query
            # Проверка прав доступа
            if not query.data.endswith(f"_{user_id}"):
                await query.answer("Эта кнопка не для вас", show_alert=True)
                return
            try:
                await query.answer()
            except Exception:
                pass
        else:
            query = None

        try:
            # Получаем всю пойманную рыбу пользователя
            caught_fish = db.get_caught_fish(user_id, chat_id)
        except Exception as e:
            logger.exception("handle_sell_fish: db.get_caught_fish failed user=%s chat=%s: %s", user_id, chat_id, e)
            _err_text = "❌ Не удалось загрузить улов. Попробуйте позже."
            try:
                if query:
                    await query.edit_message_text(_err_text)
                else:
                    await update.message.reply_text(_err_text)
            except Exception:
                pass
            return

        # Фильтруем только непроданную рыбу (sold=0)
        unsold_fish = [f for f in caught_fish if f.get('sold', 0) == 0]
        
        if not unsold_fish:
            message = "🐟 Лавка рыбы\n\nУ вас нет непроданной рыбы для продажи."
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            if query:
                await query.edit_message_text(message, reply_markup=reply_markup)
            else:
                await update.message.reply_text(message, reply_markup=reply_markup)
            return
        
        # Группируем рыбу по названию и считаем количество/стоимость
        fish_counts = {}
        total_value = 0
        for fish in unsold_fish:
            name = fish['fish_name']
            price = fish.get('price') or 0
            if name not in fish_counts:
                fish_counts[name] = {
                    'count': 0,
                    'total_price': 0,
                    'fish_id': fish['id']
                }
            fish_counts[name]['count'] += 1
            fish_counts[name]['total_price'] += price
            total_value += price

        # --- ПАГИНАЦИЯ ---
        # Получаем текущую страницу из callback_data или context.user_data
        page = 0
        if query and query.data.startswith("sell_page_"):
            try:
                page = int(query.data.split('_')[2])
            except Exception:
                page = 0
        elif hasattr(context, 'user_data') and 'sell_page' in context.user_data:
            page = context.user_data['sell_page']
        else:
            page = 0
        fish_list = sorted(fish_counts.items())
        # Store full name mapping so handle_sell_species can look up by index
        context.user_data['sell_fish_names'] = {str(i): name for i, (name, _) in enumerate(fish_list)}
        page_size = 10
        total_pages = max(1, (len(fish_list) + page_size - 1) // page_size)
        page = max(0, min(page, total_pages - 1))
        context.user_data['sell_page'] = page
        start = page * page_size
        end = start + page_size
        page_fish = fish_list[start:end]

        keyboard = []
        for i, (fish_name, info) in enumerate(page_fish):
            full_idx = start + i
            button_text = f"{fish_name} (×{info['count']}) - {info['total_price']} 🪙"
            # Use numeric index to keep callback_data within Telegram's 64-byte limit
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"sell_sp_{full_idx}_{user_id}")])

        # Добавляем кнопку продажи всего
        if total_value > 0:
            keyboard.append([InlineKeyboardButton(f"💰 Продать всё ({total_value} 🪙)", callback_data=f"sell_all_{user_id}")])

        # Стрелки пагинации
        nav_buttons = []
        if total_pages > 1:
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"sell_page_{page-1}_{user_id}"))
            nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"sell_page_{page+1}_{user_id}"))
            keyboard.append(nav_buttons)

        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"""🐟 Лавка рыбы\n\nВсего рыбы к продаже: {len(unsold_fish)}\nОбщая стоимость: {total_value} 🪙\n\nВыберите что продать:"""

        try:
            if query:
                await query.edit_message_text(message, reply_markup=reply_markup)
            else:
                await update.message.reply_text(message, reply_markup=reply_markup)
        except Exception as e:
            if "Message is not modified" not in str(e):
                logger.exception("handle_sell_fish: failed to send menu user=%s: %s", user_id, e)

    async def handle_inventory(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка инвентаря с показом локаций"""
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_inventory")
            return
        
        if update.callback_query:
            query = update.callback_query
            # Проверка прав доступа
            if not query.data.endswith(f"_{user_id}"):
                await query.answer("Эта кнопка не для вас", show_alert=True)
                return
            await query.answer()
        else:
            query = None
        
        # Получаем все пойманные рыбы и их локации (только непроданные)
        caught_fish = db.get_caught_fish(user_id, chat_id)
        unsold_fish = [f for f in caught_fish if f.get('sold', 0) == 0]

        if not unsold_fish:
            message = "🎒 Инвентарь\n\nУ вас нет пойманной рыбы."
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            if query:
                await query.edit_message_text(message, reply_markup=reply_markup)
            else:
                await update.message.reply_text(message, reply_markup=reply_markup)
            return
        
        # Группируем по локациям (с фильтром на корректные названия)
        valid_locations = {loc['name'] for loc in db.get_locations()}
        locations = {}
        for fish in unsold_fish:
            loc = fish.get('location')
            if loc not in valid_locations:
                length_loc = str(fish.get('length'))
                if length_loc in valid_locations:
                    loc = length_loc
            if loc not in locations:
                locations[loc] = []
            locations[loc].append(fish)

        if not locations:
            message = "🎒 Инвентарь\n\nУ вас нет рыбы с корректной локацией."
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            if query:
                await query.edit_message_text(message, reply_markup=reply_markup)
            else:
                await update.message.reply_text(message, reply_markup=reply_markup)
            return

        # Создаем кнопки для каждой локации
        keyboard = []
        for location in sorted(locations.keys(), key=lambda v: str(v)):
            fish_count = len(locations[location])
            button_text = f"📍 {location} ({fish_count} рыб)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"inv_location_{location.replace(' ', '_')}_{user_id}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"""🎒 Инвентарь

Всего пойманной рыбы: {len(unsold_fish)}

Выберите локацию для просмотра:"""

        if query:
            await query.edit_message_text(message, reply_markup=reply_markup)
        else:
            await update.message.reply_text(message, reply_markup=reply_markup)
    
    async def handle_inventory_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать рыбу с определенной локации в инвентаре"""
        query = update.callback_query
        data = query.data or ""
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_inventory_location")
            return
        
        # Проверка прав доступа
        owner_id = None
        if data.startswith("inv_location_") and "_page_" in data:
            try:
                before_page = data.split("_page_", 1)[0]
                owner_id = int(before_page.rsplit("_", 1)[-1])
            except Exception:
                owner_id = None
        else:
            try:
                owner_id = int(data.rsplit("_", 1)[-1])
            except Exception:
                owner_id = None

        if owner_id != user_id:
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        # Извлекаем локацию из callback_data
        # Формат: inv_location_{location}_{user_id}
        # Для пагинации формат: inv_location_{location}_{user_id}_page_{page}
        if data.startswith("inv_location_") and "_page_" in data:
            location_part = data[len("inv_location_"):].split("_page_", 1)[0]
            location = '_'.join(location_part.split('_')[:-1]).replace('_', ' ')
        else:
            parts = data.split('_')
            # Локация может содержать подчеркивания, поэтому берем все до последнего user_id
            location = '_'.join(parts[2:-1]).replace('_', ' ')
        
        await query.answer()
        
        # Получаем рыбу с этой локации
        caught_fish = db.get_caught_fish(user_id, chat_id)
        location_fish = [f for f in caught_fish if f['location'] == location and f.get('sold', 0) == 0]

        if not location_fish:
            await query.edit_message_text(f"На локации {location} нет пойманной рыбы.")
            return

        # --- ПАГИНАЦИЯ ---
        page = 0
        # Формат callback_data: inv_location_{location}_{user_id}_page_{page}
        if data.startswith("inv_location_") and "_page_" in data:
            try:
                page = int(data.split("_page_")[-1])
            except Exception:
                page = 0
        elif hasattr(context, 'user_data') and 'inv_page' in context.user_data:
            page = context.user_data['inv_page']
        else:
            page = 0
        page_size = 10
        total_pages = max(1, (len(location_fish) + page_size - 1) // page_size)
        page = max(0, min(page, total_pages - 1))
        context.user_data['inv_page'] = page
        start = page * page_size
        end = start + page_size
        page_fish = location_fish[start:end]

        # Кнопки по каждой рыбе (индивидуально)
        keyboard = []
        rarity_emoji = {
            'Обычная': '⚪',
            'Редкая': '🔵',
            'Легендарная': '🟡',
            'Мифическая': '🔴'
        }
        for fish in page_fish:
            fish_name = fish.get('fish_name', '')
            weight = fish.get('weight', 0)
            length_val = fish.get('length', 0)
            length_str = f" | {length_val} см" if length_val and length_val > 0 else ""
            rarity = fish.get('rarity', 'Обычная')
            trash = fish.get('is_trash', False)
            btn_text = f"🗑️ {fish_name} ({weight} кг)" if trash else f"{rarity_emoji.get(rarity, '⚪')} {fish_name} ({weight} кг{length_str})"
            # Можно добавить callback для подробностей или продажи одной рыбы
            keyboard.append([InlineKeyboardButton(btn_text, callback_data="noop")])

        # Стрелки пагинации
        nav_buttons = []
        if total_pages > 1:
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"inv_location_{location.replace(' ', '_')}_{user_id}_page_{page-1}"))
            nav_buttons.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="noop"))
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"inv_location_{location.replace(' ', '_')}_{user_id}_page_{page+1}"))
            keyboard.append(nav_buttons)

        # Кнопка назад
        keyboard.append([InlineKeyboardButton("◀️ Назад к локациям", callback_data=f"inventory_{user_id}")])

        reply_markup = InlineKeyboardMarkup(keyboard)
        location_text = html.escape(str(location))
        message = (
            f"📍 {location_text}\n\n"
            "Рыба, поймана на этой локации:\n\n"
            f"<blockquote><span class=\"tg-spoiler\">{fish_list}</span></blockquote>\n\n"
            f"Всего рыбы: {len(location_fish)}"
        )
        
        keyboard = [
            [InlineKeyboardButton("◀️ Назад к локациям", callback_data=f"inventory_{user_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error editing treasures message: {e}")
            await query.edit_message_text(f"Ошибка при показе клада: {e}")
    
    async def handle_inventory_treasures(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать сокровища в инвентаре"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_inventory_treasures")
            return
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        from treasures import get_treasure_name
        
        # Получаем все сокровища игрока
        treasures = db.get_player_treasures(user_id, chat_id)
        
        if not treasures:
            message = "💎 Клад\n\nУ вас нет сокровищ."
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(message, reply_markup=reply_markup)
            return
        
        # Создаем кнопки для каждого сокровища
        keyboard = []
        for treasure in treasures:
            treasure_name = treasure.get('treasure_name', '')
            quantity = treasure.get('quantity', 0)
            if quantity > 0:
                display_name = get_treasure_name(treasure_name)
                button_text = f"{display_name} ({quantity})"
                callback_data = f"sell_treasure_{treasure_name.replace(' ', '_')}_{user_id}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        if not keyboard:
            message = "💎 Клад\n\nУ вас нет сокровищ."
            keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(message, reply_markup=reply_markup)
            return
        
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"back_to_menu_{user_id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = "💎 <b>Клад</b>\n\nВаши сокровища:"
        
        try:
            await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Error editing treasures inventory message: {e}")
            await query.edit_message_text("💎 Клад\n\nВаши сокровища:")
    
    async def handle_sell_treasure(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Продажа предмета из клада"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_sell_treasure")
            return
        
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        from treasures import get_treasure_name, get_treasure_sell_price, get_treasure_sell_xp
        
        # Парсим callback_data: sell_treasure_{treasure_name}_{user_id}
        parts = query.data.split('_')
        treasure_key = '_'.join(parts[2:-1])  # Все части кроме "sell_treasure" и user_id
        treasure_key = treasure_key.replace('_', ' ')
        
        player = db.get_player(user_id, chat_id)
        if not player:
            await query.edit_message_text("❌ Профиль не найден.")
            return
        
        # Получаем информацию о сокровище
        treasures = db.get_player_treasures(user_id, chat_id)
        treasure_obj = None
        for t in treasures:
            if t.get('treasure_name') == treasure_key:
                treasure_obj = t
                break
        
        if not treasure_obj or treasure_obj.get('quantity', 0) <= 0:
            await query.edit_message_text(f"❌ У вас нет предмета '{treasure_key}' для продажи.")
            return
        
        # Получаем награды
        sell_price = get_treasure_sell_price(treasure_key)
        sell_xp = get_treasure_sell_xp(treasure_key)
        display_name = get_treasure_name(treasure_key)
        
        # Выполняем продажу
        coins = player.get('coins', 0)
        xp = player.get('xp', 0)
        
        db.update_player(user_id, chat_id, coins=coins + sell_price, xp=xp + sell_xp)
        db.remove_treasure(user_id, chat_id, treasure_key, 1)  # Удаляем 1 предмет
        
        # Получаем обновленные данные
        remaining = treasure_obj.get('quantity', 0) - 1
        
        message = (
            f"✅ <b>Продано!</b>\n\n"
            f"{display_name}\n\n"
            f"Получено:\n"
            f"  💰 {sell_price} монет\n"
            f"  ✨ {sell_xp} опыта\n\n"
            f"Осталось: {remaining}"
        )
        
        keyboard = [[InlineKeyboardButton("◀️ Назад к кладу", callback_data=f"inv_treasures_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
    
    async def handle_sell_species(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Продажа конкретного вида рыбы"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_sell_species")
            return
        
        # Проверка прав доступа
        # Формат: sell_sp_{idx}_{user_id}
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return

        # Извлекаем индекс из callback_data и ищем имя в context.user_data
        parts = query.data.split('_')
        fish_idx_str = parts[2] if len(parts) > 2 else None
        fish_name = None
        if fish_idx_str is not None:
            fish_name = context.user_data.get('sell_fish_names', {}).get(fish_idx_str)
        if not fish_name:
            await query.answer("Сессия устарела, откройте лавку заново.", show_alert=True)
            return
        
        await query.answer()
        
        # Получаем всю рыбу этого вида
        caught_fish = db.get_caught_fish(user_id, chat_id)
        species_fish = [f for f in caught_fish if f['fish_name'] == fish_name and f.get('sold', 0) == 0]
        
        if not species_fish:
            await query.edit_message_text("Рыба этого вида не найдена.")
            return
        
        if len(species_fish) == 1:
            total_value = species_fish[0]['price']
            player = db.get_player(user_id, chat_id)
            db.mark_fish_as_sold([species_fish[0]['id']])
            db.update_player(user_id, chat_id, coins=player['coins'] + total_value)

            xp_earned, base_xp, rarity_bonus, weight_bonus, total_weight = calculate_sale_summary([species_fish[0]])
            level_info = db.add_player_xp(user_id, chat_id, xp_earned)
            progress_line = format_level_progress(level_info)
            total_xp_now = level_info.get('xp_total', 0)
            
            message = f"""✅ Продажа успешна!

🐟 Продано: {fish_name} (×1)
💰 Получено: {total_value} 🪙
⚖️ Вес продано: {total_weight:.2f} кг
🎯 Бонус за вес: +{weight_bonus} XP
✨ Опыт итого: +{xp_earned}
📈 Всего опыта: {total_xp_now}
{progress_line}
Новый баланс: {player['coins'] + total_value} 🪙"""
            
            keyboard = [
                [InlineKeyboardButton("🐟 Назад в лавку", callback_data=f"sell_fish_{user_id}")],
                [InlineKeyboardButton("🔙 В меню", callback_data=f"back_to_menu_{user_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.edit_message_text(message, reply_markup=reply_markup, parse_mode="HTML")
            return

        context.user_data['waiting_sell_quantity'] = {
            "user_id": user_id,
            "chat_id": chat_id,
            "fish_name": fish_name,
            "max_qty": len(species_fish),
            "rarity": species_fish[0].get('rarity')
        }

        keyboard = [
            [InlineKeyboardButton("❌ Отмена", callback_data=f"sell_quantity_cancel_{user_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"Сколько хотите продать?\nМаксимум: {len(species_fish)}\n\n"
            "Отправьте число в чат.",
            reply_markup=reply_markup
        )
    
    async def handle_sell_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Продажа всей рыбы"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_sell_all")
            return
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        # Получаем всю рыбу пользователя (только непроданную)
        caught_fish = db.get_caught_fish(user_id, chat_id)
        unsold_fish = [f for f in caught_fish if f.get('sold', 0) == 0]
        
        if not unsold_fish:
            await query.edit_message_text("У вас нет рыбы для продажи.")
            return
        
        total_value = sum(f['price'] for f in unsold_fish)
        fish_count = len(unsold_fish)

        keyboard = [
            [
                InlineKeyboardButton(
                    "✅ Да", callback_data=f"confirm_sell_all_{user_id}"
                ),
                InlineKeyboardButton(
                    "❌ Нет", callback_data=f"cancel_sell_all_{user_id}"
                )
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "Вы уверены, что хотите продать всю рыбу?\n\n"
            f"🐟 Количество: {fish_count}\n"
            f"💰 Сумма: {total_value} 🪙",
            reply_markup=reply_markup
        )
        
    async def handle_confirm_sell_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Подтверждение продажи всей рыбы"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_confirm_sell_all")
            return
        
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        caught_fish = db.get_caught_fish(user_id, chat_id)
        unsold_fish = [f for f in caught_fish if f.get('sold', 0) == 0]
        if not unsold_fish:
            await query.edit_message_text("У вас нет рыбы для продажи.")
            return
        
        total_value = sum(f['price'] for f in unsold_fish)
        fish_count = len(unsold_fish)
        
        player = db.get_player(user_id, chat_id)
        fish_ids = [f['id'] for f in unsold_fish]
        db.mark_fish_as_sold(fish_ids)
        db.update_player(user_id, chat_id, coins=player['coins'] + total_value)

        xp_earned, base_xp, rarity_bonus, weight_bonus, total_weight = calculate_sale_summary(unsold_fish)
        level_info = db.add_player_xp(user_id, chat_id, xp_earned)
        progress_line = format_level_progress(level_info)
        total_xp_now = level_info.get('xp_total', 0)
        
        message = f"""✅ Продажа успешна!

🐟 Продано: {fish_count} рыб
💰 Получено: {total_value} 🪙
⚖️ Вес продано: {total_weight:.2f} кг
🎯 Бонус за вес: +{weight_bonus} XP
✨ Опыт итого: +{xp_earned}
📈 Всего опыта: {total_xp_now}
    {progress_line}
Новый баланс: {player['coins'] + total_value} 🪙"""
        
        keyboard = [
            [InlineKeyboardButton("🔙 В меню", callback_data=f"back_to_menu_{user_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message, reply_markup=reply_markup)

    async def handle_cancel_sell_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена продажи всей рыбы"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_cancel_sell_all")
            return
        
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        keyboard = [
            [InlineKeyboardButton("🐟 Назад в лавку", callback_data=f"sell_fish_{user_id}")],
            [InlineKeyboardButton("🔙 В меню", callback_data=f"back_to_menu_{user_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text("Продажа отменена.", reply_markup=reply_markup)

    async def handle_sell_quantity_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена продажи выбранного вида рыбы"""
        query = update.callback_query
        try:
            user_id = update.effective_user.id
        except (AttributeError, TypeError):
            logger.error("Failed to get user_id in handle_sell_quantity_cancel")
            return
        
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        context.user_data.pop('waiting_sell_quantity', None)
        
        keyboard = [
            [InlineKeyboardButton("🐟 Назад в лавку", callback_data=f"sell_fish_{user_id}")],
            [InlineKeyboardButton("🔙 В меню", callback_data=f"back_to_menu_{user_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text("Продажа отменена.", reply_markup=reply_markup)
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /stats - показать статистику"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        
        if not player:
            await update.message.reply_text("Сначала создайте профиль командой /start")
            return
        
        stats = db.get_player_stats(user_id, chat_id)
        total_species = db.get_total_fish_species()
        caught_fish = db.get_caught_fish(user_id, chat_id)
        
        message = f"""
📊 Ваша статистика

🎣 Всего поймано рыбы: {stats['total_fish']}
📏 Общий вес: {stats['total_weight']} кг
🗑️ Мусорный вес: {stats.get('trash_weight', 0)} кг
💰 Продано: {stats.get('sold_fish_count', 0)} рыб ({stats.get('sold_fish_weight', 0)} кг)
🔢 Уникальных видов: {stats['unique_fish']}/{total_species}
🏆 Самая большая рыба: {stats['biggest_fish']} ({stats['biggest_weight']} кг)

💰 Баланс: {player['coins']} 🪙
🏅 Уровень: {player.get('level', 0)} ({player.get('xp', 0)} XP)
🎣 Текущая удочка: {player['current_rod']}
📍 Текущая локация: {player['current_location']}
🪱 Текущая наживка: {player['current_bait']}
        """

        keyboard = [[InlineKeyboardButton("🔙 В меню", callback_data=f"back_to_menu_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.message:
            await update.message.reply_text(message, reply_markup=reply_markup)
        else:
            await update.callback_query.edit_message_text(message, reply_markup=reply_markup)

    async def rules_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /rules - показать правила"""
        message = f"Привет, рыбак! Правила можно прочитать по этой ссылке: {RULES_LINK}"
        if update.message:
            await update.message.reply_text(message)
        else:
            await update.callback_query.edit_message_text(message)

    async def info_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /info - показать ссылку с информацией"""
        message = f"Привет, рыбак! Информацию можно прочитать по этой ссылке: {INFO_LINK}"
        if update.message:
            await update.message.reply_text(message)
        else:
            await update.callback_query.edit_message_text(message)

    async def treasureinfo_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /treasureinfo - показать информацию о кладе и шансах выпадения"""
        from treasures import get_treasures_info
        try:
            user_id = update.effective_user.id
            message = get_treasures_info()
            
            keyboard = [[InlineKeyboardButton("🔙 Меню", callback_data=f"back_to_menu_{user_id}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if update.message:
                await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='HTML')
            else:
                await update.callback_query.edit_message_text(message, reply_markup=reply_markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error in treasureinfo_command: {e}")
            await update.message.reply_text("❌ Ошибка при загрузке информации о кладе")

    async def topl_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /topl - топ по уровню (глобально)"""
        rows = db.get_level_leaderboard(limit=10)
        if not rows:
            body = "Нет данных"
        else:
            lines = []
            for i, row in enumerate(rows, 1):
                medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                username = html.escape(str(row.get('username') or 'Неизвестно'))
                level = row.get('level', 0)
                xp = row.get('xp', 0)
                lines.append(f"{medal} {username}: {level} ур. ({xp} XP)")
            body = "\n".join(lines)

        message = f"""
🏆 Топ по уровню (глобально)
<blockquote><span class="tg-spoiler">{body}</span></blockquote>
        """
        if update.message:
            await update.message.reply_text(message, parse_mode="HTML")
        else:
            await update.callback_query.edit_message_text(message, parse_mode="HTML")
    
    async def leaderboard_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /leaderboard - таблица лидеров"""
        import logging as _logging
        from datetime import datetime, timedelta
        _lb_logger = _logging.getLogger(__name__)

        chat_id = update.effective_chat.id
        now = datetime.now()
        week_since = now - timedelta(days=7)
        day_since = now - timedelta(days=1)

        def format_leaderboard(title: str, rows: list) -> str:
            if not rows:
                body = "Нет уловов"
            else:
                lines = []
                for i, player in enumerate(rows, 1):
                    raw_username = str(player.get('username') or '').strip()
                    # Используем user_id как запасное отображение
                    display = raw_username if raw_username else f"id{player.get('user_id', '?')}"
                    medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
                    username = html.escape(display)
                    weight_value = float(player.get('total_weight') or 0)
                    lines.append(f"{medal} {username}: {weight_value:.2f} кг")
                body = "\n".join(lines) if lines else "Нет уловов"
            return f"{title}\n<blockquote><span class=\"tg-spoiler\">{body}</span></blockquote>"

        global_week = db.get_leaderboard_period(limit=10, since=week_since)
        global_day = db.get_leaderboard_period(limit=10, since=day_since)

        is_group = update.effective_chat.type in ('group', 'supergroup', 'channel')
        if is_group:
            _lb_logger.info('leaderboard_command: chat_id=%s type=%s, querying chat leaderboard', chat_id, update.effective_chat.type)
            chat_week = db.get_chat_leaderboard_period(chat_id=chat_id, limit=10, since=week_since)
            chat_day = db.get_chat_leaderboard_period(chat_id=chat_id, limit=10, since=day_since)
            _lb_logger.info('leaderboard_command: chat_week=%d rows, chat_day=%d rows', len(chat_week), len(chat_day))
        else:
            chat_week = []
            chat_day = []

        message = "🏆 Таблица лидеров\n\n"

        if is_group:
            message += "💬 Топ этого чата\n"
            message += format_leaderboard("За неделю", chat_week)
            message += "\n"
            message += format_leaderboard("За день", chat_day)
            message += "\n\n"

        message += "🌍 Глобальный топ\n"
        message += format_leaderboard("За неделю", global_week)
        message += "\n"
        message += format_leaderboard("За день", global_day)

        if update.message:
            await update.message.reply_text(message, parse_mode="HTML")
        else:
            await update.callback_query.edit_message_text(message, parse_mode="HTML")
    
    async def repair_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /repair - показать информацию о ремонте удочки"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        player = db.get_player(user_id, chat_id)
        if not player:
            await update.message.reply_text("❌ Профиль не найден!")
            return
        
        # Получаем информацию об удочке
        rod_name = player['current_rod']
        if not rod_name or not db.get_rod(rod_name):
            rod_name = BAMBOO_ROD
            db.update_player(user_id, chat_id, current_rod=rod_name)

        if rod_name in TEMP_ROD_RANGES:
            await update.message.reply_text(
                "❌ Эта удочка одноразовая и не ремонтируется.\n"
                "Купите новую в магазине."
            )
            return
        player_rod = db.get_player_rod(user_id, rod_name, chat_id)
        
        if not player_rod:
            # Инициализируем удочку, если записи нет
            db.init_player_rod(user_id, rod_name, chat_id)
            player_rod = db.get_player_rod(user_id, rod_name, chat_id)
        if not player_rod:
            await update.message.reply_text("❌ Ошибка: удочка не найдена.")
            return
        
        current_dur = player_rod['current_durability']
        max_dur = player_rod['max_durability']
        recovery_start = player_rod.get('recovery_start_time')
        
        # Вычисляем стоимость ремонта
        missing_durability = max_dur - current_dur
        if missing_durability <= 0:
            await update.message.reply_text("✅ Ваша удочка в идеальном состоянии! Ремонт не требуется.")
            return
        
        # Стоимость: 20 звезд за 100% урона, пропорционально меньше
        repair_cost = max(1, int(20 * missing_durability / max_dur))
        
        # Формируем сообщение
        message = f"🔧 Ремонт удочки\n\n"
        message += f"🎣 Удочка: {rod_name}\n"
        message += f"💪 Прочность: {current_dur}/{max_dur}\n"
        
        # Рассчитываем время до полного восстановления
        if recovery_start:
            from datetime import datetime
            recovery_started = datetime.fromisoformat(recovery_start)
            recovery_per_10min = max(1, max_dur // 30)
            intervals_needed = (missing_durability + recovery_per_10min - 1) // recovery_per_10min
            total_minutes = intervals_needed * 10
            
            hours = total_minutes // 60
            minutes = total_minutes % 60
            message += f"⏱ Автовосстановление: {hours}ч {minutes}мин\n\n"
        else:
            # Начинаем восстановление, если еще не начато
            if current_dur < max_dur:
                db.start_rod_recovery(user_id, rod_name, chat_id)
            
            recovery_per_10min = max(1, max_dur // 30)
            intervals_needed = (missing_durability + recovery_per_10min - 1) // recovery_per_10min
            total_minutes = intervals_needed * 10
            
            hours = total_minutes // 60
            minutes = total_minutes % 60
            message += f"⏱ До полного восстановления: {hours}ч {minutes}мин\n\n"
        
        message += f"💰 Мгновенный ремонт: {repair_cost} ⭐\n"
        message += f"(Восстановит до {max_dur}/{max_dur})"
        
        # Кнопка оплаты
        keyboard = [[InlineKeyboardButton(
            f"⚡ Починить за {repair_cost} ⭐", 
            callback_data=f"instant_repair_{rod_name}_{user_id}"
        )]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(message, reply_markup=reply_markup)
    
    async def test_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Тестовая команда для проверки всех функций"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        try:
            # Тест получения игрока
            player = db.get_player(user_id, chat_id)
            if player:
                await update.message.reply_text(f"✅ Игрок найден: {player['username']}")
            else:
                await update.message.reply_text("❌ Игрок не найден")
                return
            
            # Тест получения локаций
            locations = db.get_locations()
            await update.message.reply_text(f"✅ Локаций найдено: {len(locations)}")
            
            # Тест получения удочек
            rods = db.get_rods()
            await update.message.reply_text(f"✅ Удочек найдено: {len(rods)}")
            
            # Тест получения наживок
            baits = db.get_baits()
            await update.message.reply_text(f"✅ Наживок найдено: {len(baits)}")
            
            # Тест проверки возможности рыбалки
            can_fish, message = game.can_fish(user_id, chat_id)
            await update.message.reply_text(f"✅ Проверка рыбалки: {can_fish} - {message}")
            
            await update.message.reply_text("🎉 Все тесты пройдены!")
            
        except Exception as e:
            await update.message.reply_text(f"❌ Ошибка в тесте: {e}")
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /help - помощь"""
        help_text = """
🎣 Помощь по боту для рыбалки

Команды:
/start - создать профиль
/menu - меню рыбалки
/fish - начать рыбалку
/net - использовать сеть
/dynamite - взорвать динамит (12 роллов)
/shop - магазин
/weather - погода на локации
/stats - ваша статистика
/leaderboard - таблица лидеров
/repair - починить удочку
/help - эта помощь

Как играть:
1. Используйте /fish чтобы начать рыбалку
2. Если рыба сорвалась, можете оплатить гарантированный улов
3. Собирайте разные виды рыбы
4. Улучшайте снасти в магазине
5. Используйте сети для массового улова

Удачной рыбалки! 🎣
        """
        
        await update.message.reply_text(help_text)
    
    async def net_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда /net - использовать сеть"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        
        if not player:
            await update.message.reply_text("Сначала создайте профиль командой /start")
            return
        
        # Показываем доступные сети игрока
        player_nets = db.get_player_nets(user_id, chat_id)
        if not player_nets:
            db.init_player_net(user_id, 'Базовая сеть', chat_id)
            player_nets = db.get_player_nets(user_id, chat_id)
        
        if not player_nets:
            await update.message.reply_text(
                "❌ У вас нет сетей!\n\n"
                "Используйте /shop чтобы купить сети."
            )
            return
        
        # Показываем меню выбора сети
        keyboard = []
        any_on_cooldown = False
        for net in player_nets:
            cooldown = db.get_net_cooldown_remaining(user_id, net['net_name'], chat_id)
            if cooldown > 0:
                any_on_cooldown = True
                hours = cooldown // 3600
                minutes = (cooldown % 3600) // 60
                time_str = f"{hours}ч {minutes}м" if hours > 0 else f"{minutes}м"
                status = f"⏳ {time_str}"
                callback_disabled = True
            elif net['max_uses'] != -1 and net['uses_left'] <= 0:
                status = "❌ Использовано"
                callback_disabled = True
            else:
                uses_str = "∞" if net['max_uses'] == -1 else f"{net['uses_left']}"
                status = f"✅ ({uses_str} исп.)"
                callback_disabled = False
            button_text = f"🕸️ {net['net_name']} - {status}"
            callback_data = f"use_net_{net['net_name']}_{user_id}" if not callback_disabled else "net_disabled"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        if any_on_cooldown:
            keyboard.append([InlineKeyboardButton("⚡ Сбросить КД сетей — 10 ⭐", callback_data=f"net_skip_cd_{user_id}")])
        keyboard.append([InlineKeyboardButton("🔙 Меню", callback_data=f"back_to_menu_{user_id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"🕸️ Выберите сеть для использования:\n\n📍 Локация: {player['current_location']}"
        await update.message.reply_text(message, reply_markup=reply_markup)

    def _pick_dynamite_fish(self, location: str, season: str, player_level: int, target_rarity: str) -> Optional[Dict[str, Any]]:
        available_fish = db.get_fish_by_location(location, season, min_level=player_level)
        if not available_fish:
            return None
        same_rarity = [f for f in available_fish if f.get('rarity') == target_rarity]
        pool = same_rarity if same_rarity else available_fish
        return random.choice(pool) if pool else None

    async def _execute_dynamite_blast(self, user_id: int, chat_id: int, guaranteed: bool = False, reply_to_message_id: Optional[int] = None) -> None:
        player = db.get_player(user_id, chat_id)
        if not player:
            await self._safe_send_message(
                chat_id=chat_id,
                text="❌ Профиль не найден. Используйте /start в этом чате.",
                reply_to_message_id=reply_to_message_id,
            )
            return

        location = player.get('current_location', 'Городской пруд')
        season = get_current_season()
        player_level = int(player.get('level') or 0)
        weather = db.get_or_update_weather(location)
        weather_bonus = weather_system.get_weather_bonus(weather['condition']) if weather else 0
        feeder_bonus = db.get_active_feeder_bonus(user_id, chat_id)
        population_penalty = db.get_population_penalty(user_id)

        if guaranteed:
            roll_max = 20000
            trash_max = 7999
            common_max = 16999
            rare_max = 18999
            legendary_max = 19899
        else:
            roll_max = 15000
            no_bite_max = 3749
            trash_max = 7499
            common_max = 11999
            rare_max = 14849
            legendary_max = 14997

        logger.info(
            "[DYNAMITE] start user=%s chat=%s location=%s guaranteed=%s season=%s level=%s weather_bonus=%+d feeder_bonus=%+d population_penalty=%.2f%%",
            user_id,
            chat_id,
            location,
            guaranteed,
            season,
            player_level,
            weather_bonus,
            feeder_bonus,
            population_penalty,
        )

        result_lines: List[str] = []
        fish_count = 0
        trash_count = 0
        fail_count = 0
        total_trash_coins = 0
        total_haul_coins = 0
        pending_catches: List[Dict[str, Any]] = []

        for idx in range(1, DYNAMITE_BATCH_ROLLS + 1):
            roll = random.randint(0, roll_max)
            adjusted_roll = roll + (weather_bonus * 50) + (feeder_bonus * 250)
            adjusted_roll = max(0, min(roll_max, adjusted_roll))

            penalty_points = int((population_penalty / 100) * roll_max)
            adjusted_roll = max(0, adjusted_roll - penalty_points)

            logger.info(
                "[DYNAMITE] roll=%s raw=%s/%s weather_points=%+d feeder_points=%+d penalty_points=-%d adjusted=%s/%s",
                idx,
                roll,
                roll_max,
                weather_bonus * 50,
                feeder_bonus * 250,
                penalty_points,
                adjusted_roll,
                roll_max,
            )

            if not guaranteed and adjusted_roll <= no_bite_max:
                fail_count += 1
                result_lines.append(f"{idx}. Срыв")
                logger.info("[DYNAMITE] roll=%s branch=NO_BITE threshold<=%s", idx, no_bite_max)
                continue

            if adjusted_roll <= trash_max:
                trash = db.get_random_trash(location)
                if trash:
                    trash_name = trash.get('name', 'Мусор')
                    trash_price = int(trash.get('price', 0) or 0)
                    pending_catches.append({
                        'name': trash_name,
                        'weight': float(trash.get('weight', 0) or 0),
                        'length': 0.0,
                    })
                    total_trash_coins += trash_price
                    total_haul_coins += trash_price
                    trash_count += 1
                    result_lines.append(f"{idx}. {trash_name}")
                    logger.info(
                        "[DYNAMITE] roll=%s branch=TRASH name=%s weight=%skg price=%s",
                        idx,
                        trash_name,
                        trash.get('weight', 0),
                        trash_price,
                    )
                    logger.info(
                        "[DYNAMITE] roll=%s treasure_roll=SKIPPED reason=dynamite_has_no_second_treasure_roll",
                        idx,
                    )
                else:
                    fail_count += 1
                    result_lines.append(f"{idx}. Срыв")
                    logger.info("[DYNAMITE] roll=%s branch=TRASH but no trash in location -> NO_BITE", idx)
                continue

            if adjusted_roll <= common_max:
                target_rarity = "Обычная"
            elif adjusted_roll <= rare_max:
                target_rarity = "Редкая"
            elif adjusted_roll <= legendary_max:
                target_rarity = "Легендарная"
            else:
                # Для динамита NFT отключен: верхний ролл тоже считается мификом.
                target_rarity = "Мифическая"

            fish = self._pick_dynamite_fish(location, season, player_level, target_rarity)
            if not fish:
                fail_count += 1
                result_lines.append(f"{idx}. Срыв")
                logger.info("[DYNAMITE] roll=%s branch=FISH rarity=%s but pool empty -> NO_BITE", idx, target_rarity)
                continue

            weight = round(random.uniform(float(fish['min_weight']), float(fish['max_weight'])), 2)
            length = round(random.uniform(float(fish['min_length']), float(fish['max_length'])), 1)
            rarity_circle = {
                'Обычная': '⚪',
                'Редкая': '🔵',
                'Легендарная': '🟡',
                'Мифическая': '🔴',
            }.get(target_rarity, '⚪')
            pending_catches.append({
                'name': fish['name'],
                'weight': weight,
                'length': length,
            })
            fish_value = int(db.calculate_fish_price(fish, weight, length))
            total_haul_coins += fish_value
            fish_count += 1
            result_lines.append(
                f"{idx}. {rarity_circle} {format_fish_name(fish['name'])} ({length} см, {weight} кг)"
            )
            logger.info(
                "[DYNAMITE] roll=%s branch=FISH rarity=%s name=%s length=%scm weight=%skg price=%s",
                idx,
                target_rarity,
                fish['name'],
                length,
                weight,
                fish_value,
            )

        # Очень редкая отдельная механика для динамита: рыбохрана.
        if random.random() < DYNAMITE_GUARD_CHANCE:
            db.set_dynamite_ban(user_id, chat_id, DYNAMITE_GUARD_BAN_HOURS)
            logger.info(
                "[DYNAMITE] guard_triggered user=%s chat=%s chance=%.4f ban_hours=%s catches_confiscated=%s",
                user_id,
                chat_id,
                DYNAMITE_GUARD_CHANCE,
                DYNAMITE_GUARD_BAN_HOURS,
                len(pending_catches),
            )

            sticker_path = Path(__file__).parent / "fishdef.webp"
            if sticker_path.exists():
                try:
                    with open(sticker_path, 'rb') as f:
                        await self._safe_send_document(
                            chat_id=chat_id,
                            document=f,
                            reply_to_message_id=reply_to_message_id,
                        )
                except Exception as e:
                    logger.warning("Could not send fishdef.webp on dynamite arrest: %s", e)

            await self._safe_send_message(
                chat_id=chat_id,
                text=(
                    "🚨 Вас поймала рыбохрана при использовании динамита!\n"
                    "Весь текущий улов конфискован.\n"
                    f"⛔ Динамит арестован на {DYNAMITE_GUARD_BAN_HOURS} часа.\n"
                    f"💫 Откуп: {DYNAMITE_GUARD_FINE_STARS} {STAR_NAME}."
                ),
                reply_to_message_id=reply_to_message_id,
            )
            return

        for item in pending_catches:
            db.add_caught_fish(
                user_id,
                chat_id,
                item['name'],
                float(item['weight']),
                location,
                float(item['length']),
            )

        logger.info(
            "[DYNAMITE] finish user=%s chat=%s fish=%s trash=%s fail=%s catches_saved=%s total_trash_coins=%s total_haul_coins=%s",
            user_id,
            chat_id,
            fish_count,
            trash_count,
            fail_count,
            len(pending_catches),
            total_trash_coins,
            total_haul_coins,
        )

        new_coins = int(player.get('coins', 0) or 0) + total_trash_coins
        db.update_player(
            user_id,
            chat_id,
            coins=new_coins,
            last_dynamite_use_time=datetime.now().isoformat(),
        )

        header = "🧨 <b>Вы взорвали динамит!</b>"
        if guaranteed:
            header += "\n⭐ Гарантированный динамит"

        message = (
            f"{header}\n\n"
            f"📍 Локация: {location}\n"
            f"🎯 Результатов: {DYNAMITE_BATCH_ROLLS}\n"
            f"🐟 Рыбы: {fish_count}\n"
            f"📦 Мусор: {trash_count}\n"
            f"❌ Срывы: {fail_count}\n"
            f"💰 Монеты за весь улов: +{total_haul_coins} {COIN_NAME}\n\n"
            + "\n".join(result_lines)
        )

        await self._safe_send_sticker(
            chat_id=chat_id,
            sticker=DYNAMITE_STICKER_FILE_ID,
            reply_to_message_id=reply_to_message_id,
        )

        await self._safe_send_message(
            chat_id=chat_id,
            text=message,
            reply_to_message_id=reply_to_message_id,
        )

    async def dynamite_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда/слово динамит: 12 независимых роллов с КД 8 часов."""
        if update.effective_chat.type == 'private':
            await update.message.reply_text("Команда динамита работает только в группах.")
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)

        if not player:
            await update.message.reply_text("Сначала создайте профиль командой /start")
            return

        ban_remaining = db.get_dynamite_ban_remaining(user_id, chat_id)
        if ban_remaining > 0:
            hours = ban_remaining // 3600
            minutes = (ban_remaining % 3600) // 60
            seconds = ban_remaining % 60
            ban_text = f"{hours}ч {minutes}м {seconds}с" if hours > 0 else f"{minutes}м {seconds}с"

            tg_api = TelegramBotAPI(BOT_TOKEN)
            payload = self._build_dynamite_fine_payload(user_id, chat_id)
            invoice_url = await tg_api.create_invoice_link(
                title="Выкуп у рыбохраны",
                description=f"Снять арест динамита ({DYNAMITE_GUARD_FINE_STARS} {STAR_NAME})",
                payload=payload,
                currency="XTR",
                prices=[{"label": "Выкуп динамита", "amount": DYNAMITE_GUARD_FINE_STARS}],
            )

            if not invoice_url:
                await update.message.reply_text(
                    f"⛔ Вас арестовала рыбохрана. До снятия ареста: {ban_text}.\n"
                    "❌ Не удалось создать ссылку оплаты. Попробуйте позже."
                )
                return

            await self.send_invoice_url_button(
                chat_id=chat_id,
                invoice_url=invoice_url,
                text=(
                    f"⛔ Вас арестовала рыбохрана. До снятия ареста: {ban_text}.\n\n"
                    f"⭐ Оплатите {DYNAMITE_GUARD_FINE_STARS} Telegram Stars, чтобы снять арест с динамита."
                ),
                user_id=user_id,
                reply_to_message_id=update.effective_message.message_id if update.effective_message else None,
            )
            return

        remaining = db.get_dynamite_cooldown_remaining(user_id, chat_id, DYNAMITE_COOLDOWN_HOURS)
        if remaining > 0:
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            seconds = remaining % 60
            cooldown_text = f"{hours}ч {minutes}м {seconds}с" if hours > 0 else f"{minutes}м {seconds}с"

            tg_api = TelegramBotAPI(BOT_TOKEN)
            payload = self._build_dynamite_skip_payload(user_id, chat_id)
            invoice_url = await tg_api.create_invoice_link(
                title="Гарантированный динамит",
                description=f"Мгновенный взрыв динамита без ожидания ({DYNAMITE_SKIP_COST_STARS} {STAR_NAME})",
                payload=payload,
                currency="XTR",
                prices=[{"label": "Гарантированный динамит", "amount": DYNAMITE_SKIP_COST_STARS}],
            )

            if not invoice_url:
                await update.message.reply_text(
                    f"⏳ Динамит на перезарядке: {cooldown_text}\n"
                    "❌ Не удалось создать ссылку оплаты. Попробуйте позже."
                )
                return

            await self.send_invoice_url_button(
                chat_id=chat_id,
                invoice_url=invoice_url,
                text=(
                    f"⏳ Динамит на перезарядке: {cooldown_text}\n\n"
                    f"⭐ Оплатите {DYNAMITE_SKIP_COST_STARS} Telegram Stars для гарантированного взрыва динамита прямо сейчас."
                ),
                user_id=user_id,
                reply_to_message_id=update.effective_message.message_id if update.effective_message else None,
            )
            return

        await self._execute_dynamite_blast(user_id, chat_id, guaranteed=False, reply_to_message_id=update.effective_message.message_id if update.effective_message else None)
    
    async def handle_fish_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка сообщения 'рыбалка' и других текстовых сообщений"""
        # Игнорируем сообщения, отправленные ДО запуска бота (старые рыбалки не срабатывают)
        if update.message and update.message.date:
            msg_ts = update.message.date.replace(tzinfo=None)
            if msg_ts < self.bot_start_time:
                return

        if context.user_data.get('new_tour'):
            consumed = await self.handle_new_tour_input(update, context)
            if consumed:
                return

        if 'waiting_sell_selection' in context.user_data:
            data = context.user_data['waiting_sell_selection']
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            if data.get('user_id') != user_id:
                return

            message = update.effective_message
            if not message or not message.text:
                return

            raw_value = message.text.strip()
            indices = [int(x) for x in re.findall(r"\d+", raw_value)]
            required_qty = int(data.get('qty', 0))
            items = data.get('items', [])

            if not indices or len(indices) != required_qty or len(set(indices)) != len(indices):
                await update.message.reply_text(
                    f"Введите ровно {required_qty} номер(ов) из списка, например: 1 3"
                )
                return

            if any(idx < 1 or idx > len(items) for idx in indices):
                await update.message.reply_text("Номера вне диапазона списка.")
                return

            selected = [items[idx - 1] for idx in indices]
            fish_ids = [f['id'] for f in selected]
            total_value = sum(f['price'] for f in selected)
            player = db.get_player(user_id, chat_id)
            db.mark_fish_as_sold(fish_ids)
            db.update_player(user_id, chat_id, coins=player['coins'] + total_value)

            xp_earned, base_xp, rarity_bonus, weight_bonus, total_weight = calculate_sale_summary(selected)
            level_info = db.add_player_xp(user_id, chat_id, xp_earned)
            progress_line = format_level_progress(level_info)
            total_xp_now = level_info.get('xp_total', 0)

            context.user_data.pop('waiting_sell_selection', None)
            context.user_data.pop('waiting_sell_quantity', None)

            keyboard = [
                [InlineKeyboardButton("🐟 Назад в лавку", callback_data=f"sell_fish_{user_id}")],
                [InlineKeyboardButton("🔙 В меню", callback_data=f"back_to_menu_{user_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"✅ Продажа успешна!\n\n"
                f"🐟 Продано: {data.get('fish_name')} (×{required_qty})\n"
                f"💰 Получено: {total_value} 🪙\n"
                f"⚖️ Вес продано: {total_weight:.2f} кг\n"
                f"🎯 Бонус за вес: +{weight_bonus} XP\n"
                f"✨ Опыт итого: +{xp_earned}\n"
                f"📈 Всего опыта: {total_xp_now}\n"
                f"{progress_line}\n"
                f"Новый баланс: {player['coins'] + total_value} 🪙",
                reply_markup=reply_markup
            )
            return

        if 'waiting_sell_quantity' in context.user_data:
            data = context.user_data['waiting_sell_quantity']
            user_id = update.effective_user.id
            chat_id = update.effective_chat.id
            if data.get('user_id') != user_id:
                return

            message = update.effective_message
            if not message or not message.text:
                return

            raw_value = message.text.strip().lower()
            if raw_value in ("все", "all", "max", "макс"):
                qty = int(data.get('max_qty', 0))
            elif raw_value.isdigit():
                qty = int(raw_value)
            else:
                await update.message.reply_text(
                    f"Введите число от 1 до {data.get('max_qty', 0)} или слово 'все'."
                )
                return

            max_qty = int(data.get('max_qty', 0))
            if qty < 1 or qty > max_qty:
                await update.message.reply_text(
                    f"Введите число от 1 до {max_qty} или слово 'все'."
                )
                return

            fish_name = data.get('fish_name')
            caught_fish = db.get_caught_fish(user_id, chat_id)
            species_fish = [f for f in caught_fish if f['fish_name'] == fish_name and f.get('sold', 0) == 0]
            if not species_fish:
                context.user_data.pop('waiting_sell_quantity', None)
                await update.message.reply_text("Рыба этого вида не найдена.")
                return

            rarity = data.get('rarity')
            if rarity in ('Легендарная', 'Мифическая') and qty < len(species_fish):
                items = sorted(species_fish, key=lambda f: float(f.get('weight') or 0), reverse=True)
                lines = []
                for idx, item in enumerate(items, 1):
                    details = db.calculate_item_xp_details(item)
                    lines.append(
                        f"{idx}. {item.get('weight', 0)} кг — {details['xp_total']} XP (+{details['rarity_bonus']} редк., +{details['weight_bonus']} вес)"
                    )

                context.user_data.pop('waiting_sell_quantity', None)
                context.user_data['waiting_sell_selection'] = {
                    "user_id": user_id,
                    "chat_id": chat_id,
                    "fish_name": fish_name,
                    "qty": qty,
                    "items": items
                }

                await update.message.reply_text(
                    "Выберите рыбу для продажи (введите номера через пробел):\n\n"
                    + "\n".join(lines)
                )
                return

            fish_ids = [f['id'] for f in species_fish[:qty]]
            total_value = sum(f['price'] for f in species_fish[:qty])
            player = db.get_player(user_id, chat_id)
            db.mark_fish_as_sold(fish_ids)
            db.update_player(user_id, chat_id, coins=player['coins'] + total_value)

            xp_earned, base_xp, rarity_bonus, weight_bonus, total_weight = calculate_sale_summary(species_fish[:qty])
            level_info = db.add_player_xp(user_id, chat_id, xp_earned)
            progress_line = format_level_progress(level_info)
            total_xp_now = level_info.get('xp_total', 0)

            context.user_data.pop('waiting_sell_quantity', None)

            keyboard = [
                [InlineKeyboardButton("🐟 Назад в лавку", callback_data=f"sell_fish_{user_id}")],
                [InlineKeyboardButton("🔙 В меню", callback_data=f"back_to_menu_{user_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await update.message.reply_text(
                f"✅ Продажа успешна!\n\n"
                f"🐟 Продано: {fish_name} (×{qty})\n"
                f"💰 Получено: {total_value} 🪙\n"
                f"⚖️ Вес продано: {total_weight:.2f} кг\n"
                f"🎯 Бонус за вес: +{weight_bonus} XP\n"
                f"✨ Опыт итого: +{xp_earned}\n"
                f"📈 Всего опыта: {total_xp_now}\n"
                f"{progress_line}\n"
                f"Новый баланс: {player['coins'] + total_value} 🪙",
                reply_markup=reply_markup
            )
            return

        # Сначала проверяем, не ждём ли мы ввод количества наживки
        if 'waiting_bait_quantity' in context.user_data:
            await self.handle_buy_bait(update, context)
            return
        
        # Обычная обработка текстовых сообщений
        message = update.effective_message
        if not message or not message.text:
            return
        message_text = message.text.lower()
        if re.match(r"^\s*меню\b", message_text):
            await self.show_fishing_menu(update, context)
            return
        if re.match(r"^\s*(фиш|fish)\b", message_text):
            await self.fish_command(update, context)
            return
        if re.match(r"^\s*(погода|weather)\b", message_text):
            await self.weather_command(update, context)
            return
        if re.match(r"^\s*сеть\b", message_text):
            await self.net_command(update, context)
            return
        if re.match(r"^\s*(динамит|диномит|dynamite)\b", message_text):
            await self.dynamite_command(update, context)
            return
    
    async def weather_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка команды /weather и слова 'погода'"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        
        if not player:
            await update.message.reply_text("Сначала создайте профиль командой /start")
            return
        
        location = player['current_location']
        weather = db.get_or_update_weather(location)
        
        season = get_current_season()
        weather_info = weather_system.get_weather_info(weather['condition'], weather['temperature'], season)
        weather_desc = weather_system.get_weather_description(weather['condition'])
        bonus = weather_system.get_weather_bonus(weather['condition'])
        
        message = f"""🌍 Погода в локации {location}

{weather_info}
Сезон: {season}

{weather_desc}

💡 Влияние на клёв: {bonus:+d}%

Погода обновляется несколько раз в день."""
        
        await update.message.reply_text(message)
    
    async def test_weather_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Тестовая команда для проверки влияния погоды на броски"""
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        player = db.get_player(user_id, chat_id)
        
        if not player:
            await update.message.reply_text("Сначала создайте профиль командой /start")
            return
        
        location = player['current_location']
        weather = db.get_or_update_weather(location)
        
        bonus = weather_system.get_weather_bonus(weather['condition'])
        
        # Симулируем броски
        message = f"""🧪 Тестирование влияния погоды

📍 Локация: {location}
🌦️ Погода: {weather['condition']} ({bonus:+d}%)
🌡️ Температура: {weather['temperature']}°C

Диапазоны:
• 1-30: Ничего не клюёт (NO_BITE)
• 31-50: Мусор (TRASH)
• 51-100: Рыба (CATCH)

Примеры бросков с текущей погодой:
"""
        
        test_rolls = [10, 25, 35, 50, 60, 80, 95]
        
        for roll in test_rolls:
            adjusted = roll + bonus
            adjusted = max(1, min(100, adjusted))
            
            if adjusted <= 30:
                result = "❌ Ничего не клюёт"
            elif adjusted <= 50:
                result = "🗑️ Мусор"
            else:
                if adjusted <= 80:
                    result = "🐟 Рыба (обычная)"
                elif adjusted <= 95:
                    result = "🐟 Рыба (редкая)"
                else:
                    result = "🐟 Рыба (легендарная)"
            
            message += f"\nБросок {roll}: → {adjusted} = {result}"
        
        message += f"""

Как это работает:
1. Сначала выпадает случайный бросок (1-100)
2. К нему прибавляется бонус/штраф погоды ({bonus:+d}%)
3. Результат ограничивается от 1 до 100
4. По результату определяется исход"""
        
        await update.message.reply_text(message)
    
    async def handle_stats_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка кнопки статистики"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        player = db.get_player(user_id, chat_id)
        
        if not player:
            await query.edit_message_text("Сначала создайте профиль командой /start")
            return
        
        stats = db.get_player_stats(user_id, chat_id)
        total_species = db.get_total_fish_species()
        caught_fish = db.get_caught_fish(user_id, chat_id)
        
        message = f"""
📊 Ваша статистика

🎣 Всего поймано рыбы: {stats['total_fish']}
📏 Общий вес: {stats['total_weight']} кг
💰 Продано: {stats.get('sold_fish_count', 0)} рыб ({stats.get('sold_fish_weight', 0)} кг)
🔢 Уникальных видов: {stats['unique_fish']}/{total_species}
🏆 Самая большая рыба: {stats['biggest_fish']} ({stats['biggest_weight']} кг)

💰 Баланс: {player['coins']} 🪙
🏅 Уровень: {player.get('level', 0)} ({player.get('xp', 0)} XP)
🎣 Текущая удочка: {player['current_rod']}
📍 Текущая локация: {player['current_location']}
🪱 Текущая наживка: {player['current_bait']}
        """
        
        keyboard = [[InlineKeyboardButton("🔙 В меню", callback_data=f"back_to_menu_{user_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(message, reply_markup=reply_markup)
    
    async def handle_leaderboard_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка кнопки таблицы лидеров"""
        query = update.callback_query
        await query.answer()
        await self.leaderboard_command(update, context)
    
    async def handle_start_fishing(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка начала рыбалки"""
        query = update.callback_query
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Проверка прав доступа
        if not query.data.endswith(f"_{user_id}"):
            await query.answer("Эта кнопка не для вас", show_alert=True)
            return
        
        await query.answer()
        
        player = db.get_player(user_id, chat_id)
        
        # Проверяем кулдаун
        can_fish, message = game.can_fish(user_id, chat_id)
        if not can_fish:
            # Отправляем сообщение с причиной и кнопкой оплаты
            reply_markup = await self._build_guaranteed_invoice_markup(user_id, chat_id)
            
            await query.edit_message_text(
                f"⏰ {message}", 
                reply_markup=reply_markup
            )
            return
        
        # Начинаем рыбалку на текущей локации
        result = game.fish(user_id, chat_id, player['current_location'])
        
        if result['success']:
            if result.get('is_trash'):
                trash = result.get('trash') or {}
                trash_name = (trash.get('name') or '').strip()
                location_val = result.get('location') or player.get('current_location') or chat_id
                message = f"""
{trash_name or 'Мусор'}

📏 Вес: {trash.get('weight', 0)} кг
💰 Стоимость: {trash.get('price', 0)} 🪙
📍 Место: {location_val}
                """
                sticker_message = None
                # Нормализуем имя мусора для поиска
                trash_name_normalized = trash_name.strip().title()
                trash_sticker_file = TRASH_STICKERS.get(trash_name) or TRASH_STICKERS.get(trash_name_normalized)

                if trash_sticker_file:
                    try:
                        trash_image = trash_sticker_file
                        image_path = Path(__file__).parent / trash_image
                        if image_path.exists():
                            reply_to_id = query.message.message_id if query and query.message else None
                            try:
                                with open(image_path, 'rb') as f:
                                    sticker_message = await self.application.bot.send_document(
                                        chat_id=update.effective_chat.id,
                                        document=f,
                                        reply_to_message_id=reply_to_id
                                    )
                                if sticker_message:
                                    context.bot_data.setdefault("last_bot_stickers", {})[update.effective_chat.id] = sticker_message.message_id
                            except Exception as send_exc:
                                logger.error(f"[TRASH SEND ERROR] Could not send trash image for '{trash_name}' (file: {image_path}): {send_exc}")
                        else:
                            logger.error(f"[TRASH FILE MISSING] Trash sticker file missing: {image_path}")
                    except Exception as e:
                        logger.error(f"[TRASH LOGIC ERROR] Unexpected error preparing trash image for '{trash_name}': {e}")
                else:
                    logger.warning(f"Trash sticker not found for name: '{trash_name}' (normalized: '{trash_name_normalized}')")

                await query.edit_message_text(message)
                return

            fish = result.get('fish')
            if not fish:
                logger.error("Guaranteed catch missing fish data for user %s", user_id)
                await self.application.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ Не удалось получить данные улова. Звезды будут возвращены."
                )
                telegram_payment_charge_id_val = context.user_data.get('telegram_payment_charge_id')
                await self.refund_star_payment(user_id, telegram_payment_charge_id_val)
                return

            weight = result['weight']
            length = result['length']

            logger.info(
                "Catch: user=%s (%s) fish=%s location=%s bait=%s weight=%.2fkg length=%.1fcm",
                update.effective_user.id,
                update.effective_user.username or update.effective_user.full_name,
                fish['name'],
                result['location'],
                player['current_bait'],
                weight,
                length
            )
            
            # Формируем сообщение о пойманной рыбе
            rarity_emoji = {
                'Обычная': '⚪',
                'Редкая': '🔵',
                'Легендарная': '🟡',
                'Мифическая': '🔴'
            }
            fish_name_display = format_fish_name(fish['name'])
            
            message = f"""
🎉 Поздравляю! Вы поймали рыбу!

{rarity_emoji.get(fish['rarity'], '⚪')} {fish_name_display}
📏 Размер: {length}см | Вес: {weight} кг
💰 Стоимость: {fish['price']} 🪙
📍 Место: {result['location']}
⭐ Редкость: {fish['rarity']}

Ваш баланс: {result['new_balance']} 🪙
            """
            
            if result.get('guaranteed'):
                message += "\n⭐ Гарантированный улов!"
            
            # Отправляем стикер рыбы если он есть
            if fish['name'] in FISH_STICKERS:
                try:
                    fish_image = FISH_STICKERS[fish['name']]
                    image_path = Path(__file__).parent / fish_image
                    if image_path.exists():
                        with open(image_path, 'rb') as f:
                            sticker_message = await self.application.bot.send_document(
                                chat_id=update.effective_chat.id,
                                document=f,
                                reply_to_message_id=query.message.reply_to_message.message_id if query.message.reply_to_message else None
                            )
                        if sticker_message:
                            context.bot_data.setdefault("last_bot_stickers", {})[update.effective_chat.id] = sticker_message.message_id
                            context.bot_data.setdefault("sticker_fish_map", {})[sticker_message.message_id] = {
                                "fish_name": fish['name'],
                                "weight": weight,
                                "price": fish['price'],
                                "location": result['location'],
                                "rarity": fish['rarity']
                            }
                except Exception as e:
                    logger.warning(f"Could not send fish image for {fish['name']}: {e}")
            
            await query.edit_message_text(message)

            if result.get('temp_rod_broken'):
                await self.application.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=(
                        "💥 Временная удочка сломалась после удачного улова.\n"
                        "Теперь активна бамбуковая. Купить новую можно в магазине."
                    )
                )
                return
            
            # ПОСЛЕ сообщения о рыбе проверяем и сообщаем о прочности удочки
            if player['current_rod'] == BAMBOO_ROD and result.get('rod_broken'):
                durability_message = f"""
💔 Удочка сломалась!

🔧 Прочность: 0/{result.get('max_durability', 100)}

Используйте /repair чтобы починить удочку или подождите автовосстановления.
                """
                await self.application.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=durability_message
                )
            elif player['current_rod'] == BAMBOO_ROD and result.get('current_durability', 100) < result.get('max_durability', 100):
                # Показываем текущую прочность если она уменьшилась
                current = result.get('current_durability', 100)
                maximum = result.get('max_durability', 100)
                durability_message = f"🔧 Прочность удочки: {current}/{maximum}"
                await self.application.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=durability_message
                )
            return
        else:
            if result.get('snap'):
                # Срыв на неправильной наживке
                snap_message = f"""
⚠️ СРЫВ РЫБЫ!

{result['message']}

🪱 Вы использовали: {result['wrong_bait']}
📍 Локация: {result['location']}

💡 Совет: Попробуйте другую наживку!
                """
                
                await query.edit_message_text(snap_message)
                return
            elif result.get('rod_broken'):
                message = f"""
💔 Удочка сломалась!

{result['message']}

Используйте /repair чтобы починить удочку.
                """
            elif result.get('is_trash'):
                # Мусор пойман
                xp_line = ""
                progress_line = ""
                if result.get('xp_earned'):
                    xp_line = f"\n✨ Опыт: +{result['xp_earned']}"
                    progress_line = f"\n{format_level_progress(result.get('level_info'))}"
                message = f"""
{result['message']}

📦 Мусор: {result['trash']['name']}
⚖️ Вес: {result['trash']['weight']} кг
💰 Стоимость: {result['trash']['price']} 🪙
{xp_line}{progress_line}

Ваш баланс: {result['new_balance']} 🪙
                """
                
                # Отправляем стикер мусора если он есть
                if result['trash']['name'] in TRASH_STICKERS:
                    try:
                        trash_image = TRASH_STICKERS[result['trash']['name']]
                        image_path = Path(__file__).parent / trash_image
                        with open(image_path, 'rb') as f:
                            sticker_message = await self.application.bot.send_document(
                                chat_id=update.effective_chat.id,
                                document=f
                            )
                        if sticker_message:
                            context.bot_data.setdefault("last_bot_stickers", {})[update.effective_chat.id] = sticker_message.message_id
                    except Exception as e:
                        logger.warning(f"Could not send trash image for {result['trash']['name']}: {e}")
                
                await query.edit_message_text(message)
                if result.get('temp_rod_broken'):
                    await self.application.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=(
                            "💥 Временная удочка сломалась после удачного улова.\n"
                            "Теперь активна бамбуковая. Купить новую можно в магазине."
                        )
                    )
                return
            elif result.get('no_bite'):
                # Отправляем сообщение с причиной и кнопкой оплаты
                reply_markup = await self._build_guaranteed_invoice_markup(user_id, chat_id)
                
                message = f"""
😔 {result['message']}

📍 Локация: {result['location']}
                """
                
                await query.edit_message_text(message, reply_markup=reply_markup)
                return
            else:
                # Отправляем сообщение с причиной и кнопкой оплаты
                reply_markup = await self._build_guaranteed_invoice_markup(user_id, chat_id)
                
                message = f"""
😔 {result['message']}

📍 Локация: {result['location']}
                """
                
                await query.edit_message_text(message, reply_markup=reply_markup)
                return
        
        await query.edit_message_text(message)
    
    async def precheckout_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка precheckout для Telegram Stars"""
        query = update.pre_checkout_query
        payload = getattr(query, "invoice_payload", "") or ""
        user_id = query.from_user.id
        if payload.startswith("guaranteed_"):
            parsed = self._parse_guaranteed_payload(payload)
            if not parsed:
                await query.answer(ok=False, error_message="Инвойс устарел. Запросите новый.")
                return

            payload_user_id = parsed.get("payload_user_id")
            if payload_user_id is not None and payload_user_id != user_id:
                await query.answer(ok=False, error_message="Этот инвойс создан для другого пользователя.")
                return

            created_ts = parsed.get("created_ts")
            now_ts = int(datetime.now().timestamp())
            if isinstance(created_ts, int) and now_ts - created_ts > 900:
                await query.answer(ok=False, error_message="Срок действия инвойса истек. Запросите новый.")
                return
        elif payload.startswith("harpoon_skip_"):
            parsed_harpoon = self._parse_harpoon_skip_payload(payload)
            if not parsed_harpoon:
                await query.answer(ok=False, error_message="Инвойс гарпуна устарел. Запросите новый.")
                return

            if parsed_harpoon.get("payload_user_id") != user_id:
                await query.answer(ok=False, error_message="Этот инвойс создан для другого пользователя.")
                return

            created_ts = parsed_harpoon.get("created_ts")
            now_ts = int(datetime.now().timestamp())
            if isinstance(created_ts, int) and now_ts - created_ts > 900:
                await query.answer(ok=False, error_message="Срок действия инвойса истек. Запросите новый.")
                return
        elif payload.startswith("booster_"):
            parsed_booster = self._parse_booster_payload(payload)
            if not parsed_booster:
                await query.answer(ok=False, error_message="Инвойс бустера устарел. Запросите новый.")
                return

            if parsed_booster.get("payload_user_id") != user_id:
                await query.answer(ok=False, error_message="Этот инвойс создан для другого пользователя.")
                return

            created_ts = parsed_booster.get("created_ts")
            now_ts = int(datetime.now().timestamp())
            if isinstance(created_ts, int) and now_ts - created_ts > 900:
                await query.answer(ok=False, error_message="Срок действия инвойса истек. Запросите новый.")
                return

            booster_code = str(parsed_booster.get("booster_code") or "")
            payload_chat_id = int(parsed_booster.get("group_chat_id") or 0)
            if booster_code == ECHOSOUNDER_CODE:
                if db.is_echosounder_active(user_id, payload_chat_id):
                    await query.answer(ok=False, error_message="Эхолот уже активен. Дождитесь окончания.")
                    return
            else:
                active_feeder = db.get_active_feeder(user_id, payload_chat_id)
                if active_feeder:
                    await query.answer(ok=False, error_message="Кормушка уже активна. Дождитесь окончания.")
                    return
        elif payload.startswith("net_skip_cd_"):
            # format: net_skip_cd_{user_id}_{chat_id}_{ts}
            parts = payload.split("_")
            try:
                payload_user_id = int(parts[3])
                created_ts = int(parts[5])
                if payload_user_id != user_id:
                    await query.answer(ok=False, error_message="Этот инвойс создан для другого пользователя.")
                    return
                if int(datetime.now().timestamp()) - created_ts > 900:
                    await query.answer(ok=False, error_message="Срок действия инвойса истек. Запросите новый.")
                    return
            except (ValueError, IndexError):
                await query.answer(ok=False, error_message="Инвойс устарел. Запросите новый.")
                return
        elif payload.startswith("dynamite_skip_cd_"):
            parsed_dynamite = self._parse_dynamite_skip_payload(payload)
            if not parsed_dynamite:
                await query.answer(ok=False, error_message="Инвойс динамита устарел. Запросите новый.")
                return

            if parsed_dynamite.get("payload_user_id") != user_id:
                await query.answer(ok=False, error_message="Этот инвойс создан для другого пользователя.")
                return

            created_ts = parsed_dynamite.get("created_ts")
            now_ts = int(datetime.now().timestamp())
            if isinstance(created_ts, int) and now_ts - created_ts > 900:
                await query.answer(ok=False, error_message="Срок действия инвойса истек. Запросите новый.")
                return
            payload_chat_id = int(parsed_dynamite.get("group_chat_id") or 0)
            if payload_chat_id and db.get_dynamite_ban_remaining(user_id, payload_chat_id) > 0:
                await query.answer(ok=False, error_message="Динамит под арестом рыбохраны. Сначала оплатите выкуп.")
                return
        elif payload.startswith("dynamite_fine_"):
            parsed_dynamite_fine = self._parse_dynamite_fine_payload(payload)
            if not parsed_dynamite_fine:
                await query.answer(ok=False, error_message="Инвойс выкупа устарел. Запросите новый.")
                return

            if parsed_dynamite_fine.get("payload_user_id") != user_id:
                await query.answer(ok=False, error_message="Этот инвойс создан для другого пользователя.")
                return

            created_ts = parsed_dynamite_fine.get("created_ts")
            now_ts = int(datetime.now().timestamp())
            if isinstance(created_ts, int) and now_ts - created_ts > 900:
                await query.answer(ok=False, error_message="Срок действия инвойса истек. Запросите новый.")
                return
        # Проверяем, не был ли этот инвойс уже оплачен
        if payload and payload in self.paid_payloads:
            await query.answer(ok=False, error_message="Этот инвойс уже был оплачен. Запросите новый.")
            return
        await query.answer(ok=True)
    
    async def successful_payment_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка успешной оплаты через Telegram Stars"""
        payment = update.message.successful_payment
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        payload = payment.invoice_payload or ""
        active_invoice = self.active_invoices.get(user_id) or {}

        # Защита от двойной выдачи: если payload уже обработан — игнорируем
        if payload and payload in self.paid_payloads:
            logger.warning("Duplicate payment ignored for payload=%s user_id=%s", payload, user_id)
            return
        # Сразу помечаем payload как оплаченный
        if payload:
            if len(self.paid_payloads) >= self._paid_payloads_max:
                # Удаляем половину старых записей, чтобы не расти бесконечно
                old_entries = list(self.paid_payloads)
                self.paid_payloads = set(old_entries[len(old_entries)//2:])
                logger.info("paid_payloads trimmed to %d entries", len(self.paid_payloads))
            self.paid_payloads.add(payload)

        accounting_chat_id = chat_id
        parsed_guaranteed_payload = None
        parsed_harpoon_payload = None
        parsed_booster_payload = None
        parsed_dynamite_payload = None
        parsed_dynamite_fine_payload = None
        if payload.startswith("guaranteed_"):
            parsed_guaranteed_payload = self._parse_guaranteed_payload(payload)
            if parsed_guaranteed_payload and parsed_guaranteed_payload.get("group_chat_id"):
                accounting_chat_id = int(parsed_guaranteed_payload["group_chat_id"])
        elif payload.startswith("harpoon_skip_"):
            parsed_harpoon_payload = self._parse_harpoon_skip_payload(payload)
            if parsed_harpoon_payload and parsed_harpoon_payload.get("group_chat_id"):
                accounting_chat_id = int(parsed_harpoon_payload["group_chat_id"])
        elif payload.startswith("booster_"):
            parsed_booster_payload = self._parse_booster_payload(payload)
            if parsed_booster_payload and parsed_booster_payload.get("group_chat_id"):
                accounting_chat_id = int(parsed_booster_payload["group_chat_id"])
        elif payload.startswith("net_skip_cd_"):
            # format: net_skip_cd_{user_id}_{chat_id}_{ts}
            try:
                _parts = payload.split("_")
                accounting_chat_id = int(_parts[4])
            except (ValueError, IndexError):
                accounting_chat_id = active_invoice.get("group_chat_id") or chat_id
        elif payload.startswith("dynamite_skip_cd_"):
            parsed_dynamite_payload = self._parse_dynamite_skip_payload(payload)
            if parsed_dynamite_payload and parsed_dynamite_payload.get("group_chat_id"):
                accounting_chat_id = int(parsed_dynamite_payload["group_chat_id"])
            else:
                accounting_chat_id = active_invoice.get("group_chat_id") or chat_id
        elif payload.startswith("dynamite_fine_"):
            parsed_dynamite_fine_payload = self._parse_dynamite_fine_payload(payload)
            if parsed_dynamite_fine_payload and parsed_dynamite_fine_payload.get("group_chat_id"):
                accounting_chat_id = int(parsed_dynamite_fine_payload["group_chat_id"])
            else:
                accounting_chat_id = active_invoice.get("group_chat_id") or chat_id
        elif active_invoice.get("group_chat_id"):
            try:
                accounting_chat_id = int(active_invoice.get("group_chat_id"))
            except (TypeError, ValueError):
                accounting_chat_id = chat_id

        accounting_chat_title = None
        if accounting_chat_id == chat_id:
            try:
                accounting_chat_title = update.effective_chat.title
            except Exception:
                accounting_chat_title = None
        else:
            try:
                accounting_chat_title = db.get_chat_title(accounting_chat_id)
            except Exception:
                accounting_chat_title = None

        telegram_payment_charge_id = getattr(payment, "telegram_payment_charge_id", None)
        total_amount = getattr(payment, "total_amount", 0)

        # Сохраняем транзакцию
        if telegram_payment_charge_id:
            try:
                # If DB supports chat_id/chat_title columns, add them via migration-aware method
                db.add_star_transaction(
                    user_id=user_id,
                    telegram_payment_charge_id=telegram_payment_charge_id,
                    total_amount=total_amount,
                    refund_status="none",
                    chat_id=accounting_chat_id,
                    chat_title=accounting_chat_title,
                )
                # update chat-level aggregate (this will also save chat_title in chat_configs)
                db.increment_chat_stars(accounting_chat_id, total_amount, chat_title=accounting_chat_title)
            except Exception as e:
                logger.warning("Failed to record star transaction or increment chat stars: %s", e)
            # If DB has explicit star_transactions chat columns we will keep them in migration
        
        # Убираем запланированный таймаут для этого сообщения
        timeout_key = f"payment_{update.effective_chat.id}_{update.message.message_id}"
        if timeout_key in self.active_timeouts:
            del self.active_timeouts[timeout_key]
        
        # Извлекаем локацию и chat_id из payload (если есть) или используем текущую
        if payload and payload.startswith("net_skip_cd_"):
            # Сброс кулдауна всех сетей
            skip_reply_id = None
            if user_id in self.active_invoices:
                skip_reply_id = self.active_invoices[user_id].get('group_message_id')
                del self.active_invoices[user_id]
            db.reset_net_cooldowns(user_id)
            await self._safe_send_message(
                chat_id=accounting_chat_id,
                text="✅ Кулдаун всех сетей сброшен! Используйте /net чтобы закинуть сети снова.",
                reply_to_message_id=skip_reply_id,
            )
            return
        elif payload and payload.startswith("dynamite_skip_cd_"):
            if not parsed_dynamite_payload:
                parsed_dynamite_payload = self._parse_dynamite_skip_payload(payload)

            group_chat_id = accounting_chat_id
            if parsed_dynamite_payload and parsed_dynamite_payload.get("group_chat_id"):
                group_chat_id = int(parsed_dynamite_payload["group_chat_id"])

            dynamite_reply_id = None
            if user_id in self.active_invoices:
                dynamite_reply_id = self.active_invoices[user_id].get('group_message_id')
                del self.active_invoices[user_id]

            await self._execute_dynamite_blast(
                user_id=user_id,
                chat_id=group_chat_id,
                guaranteed=True,
                reply_to_message_id=dynamite_reply_id,
            )
            return
        elif payload and payload.startswith("dynamite_fine_"):
            if not parsed_dynamite_fine_payload:
                parsed_dynamite_fine_payload = self._parse_dynamite_fine_payload(payload)

            group_chat_id = accounting_chat_id
            if parsed_dynamite_fine_payload and parsed_dynamite_fine_payload.get("group_chat_id"):
                group_chat_id = int(parsed_dynamite_fine_payload["group_chat_id"])

            fine_reply_id = None
            if user_id in self.active_invoices:
                fine_reply_id = self.active_invoices[user_id].get('group_message_id')
                del self.active_invoices[user_id]

            db.clear_dynamite_ban(user_id, group_chat_id)
            await self._safe_send_message(
                chat_id=group_chat_id,
                text="✅ Выкуп принят. Арест рыбохраны снят, динамит снова доступен.",
                reply_to_message_id=fine_reply_id,
            )
            return
        elif payload and payload.startswith("repair_rod_"):
            # Обработка восстановления удочки
            rod_name = payload.replace("repair_rod_", "")
            repair_reply_id = None
            if user_id in self.active_invoices:
                repair_reply_id = self.active_invoices[user_id].get('group_message_id')
                del self.active_invoices[user_id]
            if rod_name in TEMP_ROD_RANGES:
                try:
                    await self._safe_send_message(
                        chat_id=accounting_chat_id,
                        text="❌ Эта удочка одноразовая и не ремонтируется.",
                        reply_to_message_id=repair_reply_id,
                    )
                except Exception as e:
                    logger.warning(f"Could not send temp rod repair rejection to {user_id}: {e}")
                return
            db.repair_rod(user_id, rod_name, accounting_chat_id)
            try:
                await self._safe_send_message(
                    chat_id=accounting_chat_id,
                    text=f"✅ Удочка '{rod_name}' полностью восстановлена!",
                    reply_to_message_id=repair_reply_id,
                )
            except Exception as e:
                logger.warning(f"Could not send repair confirmation to {user_id}: {e}")
            return
        elif payload and payload.startswith("harpoon_skip_"):
            if not parsed_harpoon_payload:
                parsed_harpoon_payload = self._parse_harpoon_skip_payload(payload)

            group_chat_id = accounting_chat_id
            if parsed_harpoon_payload and parsed_harpoon_payload.get("group_chat_id"):
                group_chat_id = int(parsed_harpoon_payload["group_chat_id"])

            group_message_id = None
            if user_id in self.active_invoices:
                group_message_id = self.active_invoices[user_id].get('group_message_id')
                del self.active_invoices[user_id]

            await self._execute_harpoon_catch(
                user_id=user_id,
                group_chat_id=group_chat_id,
                reply_to_message_id=group_message_id,
            )
            return
        elif payload and payload.startswith("booster_"):
            if not parsed_booster_payload:
                parsed_booster_payload = self._parse_booster_payload(payload)

            if not parsed_booster_payload:
                await update.message.reply_text("❌ Не удалось обработать оплату бустера. Попробуйте ещё раз.")
                return

            booster_code = str(parsed_booster_payload.get("booster_code") or "")
            group_chat_id = int(parsed_booster_payload.get("group_chat_id") or accounting_chat_id)

            booster_reply_id = None
            if user_id in self.active_invoices:
                booster_reply_id = self.active_invoices[user_id].get('group_message_id')
                del self.active_invoices[user_id]

            if booster_code == ECHOSOUNDER_CODE:
                db.activate_echosounder(user_id, group_chat_id, ECHOSOUNDER_DURATION_HOURS)
                await self._safe_send_message(
                    chat_id=group_chat_id,
                    text=(
                        f"✅ Эхолот активирован на {ECHOSOUNDER_DURATION_HOURS} часа!\n"
                        "Откройте меню наживки и нажмите кнопку 'Эхолот'."
                    ),
                    reply_to_message_id=booster_reply_id,
                )
                return

            feeder = self._get_feeder_by_code(booster_code)
            if not feeder:
                await update.message.reply_text("❌ Неизвестный тип кормушки.")
                return

            db.activate_feeder(
                user_id,
                group_chat_id,
                feeder_type=booster_code,
                bonus_percent=int(feeder["bonus"]),
                duration_minutes=int(feeder["duration_minutes"]),
            )
            await self._safe_send_message(
                chat_id=group_chat_id,
                text=(
                    f"✅ {feeder['name']} активирована на 1 час!\n"
                    f"🎯 Бонус к клёву: +{feeder['bonus']}%"
                ),
                reply_to_message_id=booster_reply_id,
            )
            return
        elif payload and payload.startswith("guaranteed_"):
            parsed = parsed_guaranteed_payload or self._parse_guaranteed_payload(payload)
            if parsed:
                group_chat_id = parsed.get("group_chat_id", update.effective_chat.id)
                location = parsed.get("location")
            else:
                location = None
                group_chat_id = update.effective_chat.id

            if not location:
                location = "Неизвестно"
                try:
                    player_by_group = db.get_player(user_id, group_chat_id)
                    if player_by_group and player_by_group.get('current_location'):
                        location = player_by_group['current_location']
                except Exception as e:
                    logger.warning(f"Could not resolve location for guaranteed payload user={user_id}, chat={group_chat_id}: {e}")
        else:
            # Получаем текущую локацию игрока
            player = db.get_player(user_id, chat_id)
            location = player['current_location']
            group_chat_id = update.effective_chat.id
        
        # Получаем и сохраняем информацию о сообщении с кнопкой ДО удаления из active_invoices
        group_message_id = None
        if user_id in self.active_invoices:
            group_message_id = self.active_invoices[user_id].get('group_message_id')
            # Теперь удаляем инвойс из активных
            del self.active_invoices[user_id]
        
        # Выполняем гарантированный улов (все проверки уже пройдены в precheckout)
        # Дополнительно: если бамбуковая/обычная удочка сломана — возвращаем звезду
        player_rod_check = db.get_player(user_id, group_chat_id)
        if player_rod_check:
            _current_rod = player_rod_check.get('current_rod', BAMBOO_ROD)
            if _current_rod not in TEMP_ROD_RANGES:
                _rod_data = db.get_player_rod(user_id, _current_rod, group_chat_id)
                if _rod_data and _rod_data.get('current_durability', 100) <= 0:
                    await self.refund_star_payment(user_id, telegram_payment_charge_id)
                    await self._safe_send_message(
                        chat_id=group_chat_id,
                        text=(
                            "💔 Гарантированный улов отменён — ваша удочка сломана!\n"
                            "Оплата возвращена. Используйте /repair или кнопку ремонта за 20 ⭐."
                        ),
                        reply_to_message_id=group_message_id,
                    )
                    return
        # Если игрок арестован рыбнадзором — возвращаем звезду
        if player_rod_check and player_rod_check.get('is_banned'):
            _ban_until = player_rod_check.get('ban_until')
            if _ban_until:
                from datetime import datetime as _dt
                if _dt.now() < _dt.fromisoformat(_ban_until):
                    await self.refund_star_payment(user_id, telegram_payment_charge_id)
                    await self._safe_send_message(
                        chat_id=group_chat_id,
                        text="⛔️ Гарантированный улов отменён — вы под арестом рыбнадзора!\nОплата возвращена. Откупитесь командой /payfine (15 ⭐).",
                        reply_to_message_id=group_message_id,
                    )
                    return

        try:
            result = game.fish(user_id, group_chat_id, location, guaranteed=True)
        except Exception as e:
            logger.error(f"Critical error in guaranteed catch for user {user_id}: {e}", exc_info=True)
            message = f"❌ Произошла критическая ошибка при выполнении улова: {str(e)}. Пожалуйста, обратитесь в поддержку."
            await self._safe_send_message(
                chat_id=update.effective_chat.id,
                text=message
            )

            # Возвращаем звезды, если оплата прошла, но улов не был обработан
            await self.refund_star_payment(user_id, telegram_payment_charge_id)
            return
        
        # If result indicates trash (even when success==False in game logic), handle it here
        if result.get('is_trash'):
            trash = result.get('trash') or {}
            message = f"""
{trash.get('name', 'Мусор')}

📏 Вес: {trash.get('weight', 0)} кг
💰 Стоимость: {trash.get('price', 0)} 🪙
📍 Место: {result.get('location', location)}
            """

            # Try to send trash sticker in reply to the original group message (invoice button)
            sticker_message = None
            try:
                trash_name = trash.get('name')
                if trash_name in TRASH_STICKERS:
                    trash_image = TRASH_STICKERS[trash_name]
                    image_path = Path(__file__).parent / trash_image
                    # Send document immediately (send in the same handler so it's delivered on payment)
                    try:
                        with open(image_path, 'rb') as f:
                            await self._safe_send_document(chat_id=group_chat_id, document=f, reply_to_message_id=group_message_id)
                    except Exception as e:
                        logger.warning("Immediate send of trash image failed for notification: %s", e)
            except Exception as e:
                logger.warning(f"Could not send trash image for {trash.get('name')}: {e}")

            # If we had a sticker, reply with info to the sticker; otherwise reply to the original group message
            # Send text reply to group immediately
            await self._safe_send_message(chat_id=group_chat_id, text=message, reply_to_message_id=group_message_id)
            return

        fish = result.get('fish')
        if not fish:
            logger.error("Guaranteed catch missing fish data for user %s", user_id)
            await self._safe_send_message(chat_id=update.effective_chat.id, text="❌ Не удалось получить данные улова. Звезды будут возвращены.")
            await self.refund_star_payment(user_id, telegram_payment_charge_id)
            return

        weight = result['weight']
        length = result['length']

        player = db.get_player(user_id, chat_id)
        logger.info(
            "Catch: user=%s (%s) fish=%s location=%s bait=%s weight=%.2fkg length=%.1fcm guaranteed=True",
            update.effective_user.id,
            update.effective_user.username or update.effective_user.full_name,
            fish['name'],
            result['location'],
            player['current_bait'] if player else "",
            weight,
            length
        )

        # Отправляем сообщение с характеристиками рыбы
        fish_name_display = format_fish_name(fish['name'])
        message = f"""
🐟 {fish_name_display}

📏 Размер: {length}см | Вес: {weight} кг
💰 Стоимость: {fish['price']} 🪙
📍 Место: {result['location']}
⭐ Редкость: {fish['rarity']}
        """

        # Получаем информацию о сообщении с кнопкой (уже получена выше перед удалением из active_invoices)
        logger.info(f"Using group_message_id for user {user_id}: {group_message_id}")

        # Отправляем стикер рыбы если он есть - в ответ на сообщение с кнопкой
        sticker_message = None
        if fish['name'] in FISH_STICKERS:
            fish_image = FISH_STICKERS[fish['name']]
            image_path = Path(__file__).parent / fish_image
            if image_path.exists():
                try:
                    with open(image_path, 'rb') as f:
                        await self._safe_send_document(chat_id=group_chat_id, document=f, reply_to_message_id=group_message_id)
                except Exception as e:
                    logger.warning("Failed to send fish image %s: %s", fish_image, e)
            else:
                logger.debug("Fish image not found: %s (fish=%s)", image_path, fish['name'])

        # Всегда отправляем текстовое сообщение о рыбе (вынесено из блока стикера)
        await self._safe_send_message(chat_id=group_chat_id, text=message, reply_to_message_id=group_message_id)

        if result.get('temp_rod_broken'):
            await self._safe_send_message(chat_id=group_chat_id, text=(
                "💥 Временная удочка сломалась после удачного улова.\n"
                "Теперь активна бамбуковая. Купить новую можно в магазине."
            ))

    async def refund_star_payment(self, user_id: int, telegram_payment_charge_id: str) -> bool:
        """Возврат Telegram Stars пользователю"""
        if not telegram_payment_charge_id:
            logger.error("refund_star_payment: отсутствует telegram_payment_charge_id")
            return False

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/refundStarPayment"
        payload = {
            "user_id": user_id,
            "telegram_payment_charge_id": telegram_payment_charge_id
        }

        try:
            response = await asyncio.to_thread(requests.post, url, data=payload, timeout=15)
            data = response.json() if response is not None else {}
            if response is not None and response.status_code == 200 and data.get("ok"):
                db.update_star_refund_status(telegram_payment_charge_id, "ref")
                logger.info("Stars refund successful for user=%s, charge_id=%s", user_id, telegram_payment_charge_id)
                return True

            logger.error("Stars refund failed: status=%s, response=%s", response.status_code if response else None, data)
            return False
        except Exception as e:
            logger.error("Stars refund exception: %s", e)
            return False

    async def refunded_payment_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка возврата оплаты (если пользователь вернул звезды сам)"""
        message = update.message
        refunded_payment = getattr(message, "refunded_payment", None) if message else None
        if not refunded_payment:
            return

        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        telegram_payment_charge_id = getattr(refunded_payment, "telegram_payment_charge_id", None)
        total_amount = getattr(refunded_payment, "total_amount", 0)

        existing = db.get_star_transaction(telegram_payment_charge_id)
        if not existing:
            db.add_star_transaction(
                user_id=user_id,
                telegram_payment_charge_id=telegram_payment_charge_id,
                total_amount=total_amount,
                refund_status="need to ban"
            )
        else:
            if existing.get("refund_status") != "ref":
                db.update_star_refund_status(telegram_payment_charge_id, "need to ban")
    
    async def handle_sticker(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка полученного стикера - отправка информации о рыбе"""
        if not update.message.sticker:
            return

        reply = update.message.reply_to_message
        if not reply or not reply.sticker or not reply.from_user:
            return

        # Реагируем только на стикер рыбы бота
        if not reply.from_user.is_bot:
            return

        last_bot_stickers = context.bot_data.get("last_bot_stickers", {})
        if last_bot_stickers.get(update.effective_chat.id) != reply.message_id:
            return

        fish_info_map = context.bot_data.get("sticker_fish_map", {})
        fish_info = fish_info_map.get(reply.message_id)
        if not fish_info:
            return

        fish_name_display = format_fish_name(fish_info.get('fish_name', 'Неизвестно'))
        message = f"""
    {fish_name_display}

📏 Ваш размер: {fish_info.get('weight', 'N/A')} кг
💰 Стоимость: {fish_info.get('price', 'N/A')} 🪙
📍 Место ловли: {fish_info.get('location', 'N/A')}
⭐ Редкость: {fish_info.get('rarity', 'N/A')}
            """
        await update.message.reply_text(message)
    
    async def handle_pay_telegram_star_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатия на кнопку оплаты Telegram Stars"""
        query = update.callback_query
        try:
            await query.answer()
        except BadRequest as exc:
            if "Query is too old" not in str(exc):
                raise
        
        user_id = update.effective_user.id
        chat_id = update.effective_chat.id
        
        # Извлекаем локацию из callback_data
        callback_data = query.data
        if callback_data.startswith("pay_telegram_star_"):
            parts = callback_data.split("_", 4)
            if len(parts) < 5:
                await query.answer("Некорректные данные кнопки", show_alert=True)
                return
            target_user_id = parts[3]
            location = parts[4]
            if str(user_id) != str(target_user_id):
                await query.answer("Эта кнопка не для вас", show_alert=True)
                return
        else:
            location = "Неизвестно"

        existing_invoice = self.active_invoices.get(user_id)
        if existing_invoice:
            created_at = existing_invoice.get("created_at")
            if isinstance(created_at, datetime):
                created_time = created_at
            elif isinstance(created_at, str):
                try:
                    created_time = datetime.fromisoformat(created_at)
                except ValueError:
                    created_time = None
            else:
                created_time = None

            if created_time:
                age_seconds = (datetime.now() - created_time).total_seconds()
                if age_seconds < 120:
                    await query.answer("Инвойс уже отправлен в личные сообщения", show_alert=True)
                    return

            await self.cancel_previous_invoice(user_id)

        # Legacy callback: преобразуем в URL-кнопку на месте без дополнительных сообщений
        reply_markup = await self._build_guaranteed_invoice_markup(user_id, chat_id)
        if not reply_markup:
            await query.answer("Не удалось создать ссылку оплаты", show_alert=True)
            return
        try:
            await query.edit_message_reply_markup(reply_markup=reply_markup)
        except BadRequest:
            pass
        await query.answer("Ссылка оплаты обновлена. Нажмите кнопку ещё раз.", show_alert=False)
    
    async def handle_invoice_sent_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатия на сообщение об отправленном инвойсе"""
        query = update.callback_query
        try:
            await query.answer("Инвойс уже отправлен в личные сообщения", show_alert=True)
        except BadRequest as exc:
            if "Query is too old" not in str(exc):
                raise
    
    async def handle_payment_timeout(self, chat_id: int, message_id: int):
        """Обработка таймаута платежа - делаем кнопку неактивной"""
        try:
            # Находим сообщение с инвойсом и делаем кнопку неактивной
            keyboard = [
                [InlineKeyboardButton(
                    f"⏰ Время оплаты вышло", 
                    callback_data="payment_expired"
                )]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Обновляем сообщение с неактивной кнопкой
            await self.application.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="Время для оплаты истекло",
                reply_markup=reply_markup
            )
            for user_id, invoice_info in list(self.active_invoices.items()):
                if invoice_info.get('group_message_id') == message_id:
                    del self.active_invoices[user_id]
        except Exception as e:
            # Инвойсы нельзя редактировать после оплаты или если они уже изменены
            logger.error(f"Ошибка обновления сообщения с таймаутом: {e}")
            # Просто удаляем таймер, ничего не делаем с сообщением
    
    async def handle_payment_expired_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатия на просроченную кнопку оплаты"""
        query = update.callback_query
        await query.answer("Время для оплаты истекло", show_alert=True)
    
    async def handle_invoice_cancelled_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка нажатия на отмененный инвойс"""
        query = update.callback_query
        await query.answer("Срок действия инвойса истек", show_alert=True)
    
    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик ошибок с улучшенным логированием"""
        error = context.error

        # Частый кейс при запуске двух инстансов с одним токеном
        if isinstance(error, Conflict):
            logger.warning("Conflict: запущено несколько инстансов бота с одним токеном")
            return

        # Временные сетевые ошибки Telegram API не требуют сообщения пользователю
        if isinstance(error, NetworkError):
            logger.warning(f"Сетевая ошибка Telegram API: {error}")
            return

        logger.error(f"Update {update} caused error {error}")
        
        # Проверяем тип ошибки
        if isinstance(error, requests.exceptions.ConnectionError):
            logger.error("Проблема с подключением к Telegram API. Проверьте интернет-соединение.")
        elif isinstance(error, requests.exceptions.Timeout):
            logger.error("Таймаут подключения к Telegram API. Попробуйте позже.")
        elif isinstance(error, requests.exceptions.HTTPError):
            logger.error(f"HTTP ошибка: {error}")
        else:
            logger.error(f"Неизвестная ошибка: {type(error).__name__}: {error}")
        
        # Пытаемся отправить сообщение пользователю об ошибке
        if update and hasattr(update, 'effective_chat'):
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="⚠️ Произошла ошибка. Попробуйте позже."
                )
            except Exception as e:
                logger.error(f"Не удалось отправить сообщение об ошибке: {e}")

def main():
    """Основная функция"""
    # Парсинг аргументов командной строки
    import argparse
    parser = argparse.ArgumentParser(description='Бот для рыбалки')
    parser.add_argument('--proxy', help='URL прокси (например: socks5://127.0.0.1:1080)')
    parser.add_argument('--offline', action='store_true', help='Офлайн режим для тестирования')
    parser.add_argument('--check-only', action='store_true', help='Только проверить соединение')
    
    args = parser.parse_args()
    
    if BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
        print("Ошибка: Укажите токен бота в config.py или в переменной окружения BOT_TOKEN")
        return
    
    # Устанавливаем переменные окружения из аргументов
    if args.proxy:
        os.environ['TELEGRAM_PROXY'] = args.proxy
    if args.offline:
        os.environ['OFFLINE_MODE'] = '1'
    
    # Проверка соединения
    if args.check_only:
        print("🔍 Проверка соединения с Telegram API...")
        try:
            import requests
            response = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getMe",
                timeout=10
            )
            if response.status_code == 200:
                bot_info = response.json()
                print(f"✅ Соединение успешно! Бот: @{bot_info['result']['username']}")
                return
            else:
                print(f"❌ Ошибка API: {response.status_code}")
                return
        except Exception as e:
            print(f"❌ Ошибка соединения: {e}")
            return
    
    # Проверяем офлайн режим
    offline_mode = os.environ.get('OFFLINE_MODE') == '1'
    if offline_mode:
        print("🔧 Офлайн режим - пропускаем проверку API")
    else:
        # Проверяем подключение к Telegram API
        print("🔍 Проверка подключения к Telegram API...")
        try:
            import requests
            response = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getMe",
                timeout=10
            )
            if response.status_code == 200:
                bot_info = response.json()
                print(f"✅ Подключение успешно! Бот: @{bot_info['result']['username']}")
            else:
                print(f"❌ Ошибка подключения: {response.status_code}")
                print(f"Ответ: {response.text}")
                return
        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка подключения к Telegram API: {e}")
            print("Проверьте интернет-соединение или используйте прокси:")
            print("python bot.py --proxy socks5://127.0.0.1:1080")
            return
        except Exception as e:
            print(f"❌ Неизвестная ошибка: {e}")
            return
    
    # Создаем экземпляр бота
    bot_instance = FishBot()

    # NOTE: DB fixer run removed. Manual fixes should be performed with tools/fix_caught_fish_chatid.py
    
    # Создаем приложение
    defaults = Defaults(parse_mode="HTML")
    # Таймауты сети для предотвращения зависания бота.
    # Передаём их в HTTPXRequest, т.к. при использовании .bot() в builder'е
    # нельзя задавать таймауты через builder — они должны быть на уровне Request.
    from telegram.request import HTTPXRequest
    _read_timeout = float(os.getenv('TG_READ_TIMEOUT', '7'))
    _write_timeout = float(os.getenv('TG_WRITE_TIMEOUT', '10'))
    _connect_timeout = float(os.getenv('TG_CONNECT_TIMEOUT', '10'))
    _pool_timeout = float(os.getenv('TG_POOL_TIMEOUT', '3'))
    _request = HTTPXRequest(
        read_timeout=_read_timeout,
        write_timeout=_write_timeout,
        connect_timeout=_connect_timeout,
        pool_timeout=_pool_timeout,
    )
    emoji_bot = EmojiBot(token=BOT_TOKEN, defaults=defaults, request=_request)

    async def _post_init(application: Application):
        try:
            # Ensure DB table exists synchronously, then schedule the async worker
            notifications.init_notifications_table()
            application.create_task(notifications.start_worker(application))
        except Exception as e:
            logger.exception("post_init: failed to start notifications worker: %s", e)

    application = (
        Application.builder()
        .bot(emoji_bot)
        .post_init(_post_init)
        .build()
    )

    # Устанавливаем приложение в экземпляр бота
    bot_instance.application = application
    
    # Создаем asyncio scheduler
    bot_instance.scheduler = AsyncIOScheduler()
    # Scheduler будет запущен после запуска приложения
    print("✅ Application создана успешно")

    async def dbinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Owner-only helper to inspect runtime DB file on the container
        owner_id = 793216884
        user_id = getattr(update.effective_user, 'id', None)
        if user_id != owner_id:
            await update.message.reply_text("Нет доступа.")
            return

        path = os.environ.get('FISHBOT_DB_PATH', DB_PATH)
        lines = []
        try:
            st = os.stat(path)
            lines.append(f"Path: {path}")
            lines.append(f"Size: {st.st_size} bytes")
            lines.append(f"Mtime: {datetime.fromtimestamp(st.st_mtime)}")
            with open(path, 'rb') as f:
                header = f.read(16)
            try:
                header_text = header.decode('ascii', errors='replace')
            except Exception:
                header_text = str(header)
            lines.append(f"Header: {header.hex()}  ({header_text})")
        except Exception as e:
            lines.append("DB read error: " + str(e))

        backups_list = []
        try:
            backups_dir = Path('/data/backups')
            if backups_dir.exists():
                for b in sorted(backups_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)[:5]:
                    backups_list.append(f"{b.name}  {b.stat().st_size} bytes  {datetime.fromtimestamp(b.stat().st_mtime)}")
        except Exception:
            pass

        if backups_list:
            lines.append("Backups:\n" + "\n".join(backups_list))
        else:
            lines.append("Backups: none")

        await update.message.reply_text("\n\n".join(lines))

    async def dbstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return

        out_lines = []
        try:
            # Use db connection wrapper (works with sqlite or Postgres depending on DATABASE_URL)
            conn = db._connect()
            cur = conn.cursor()
            # Basic counts
            for q, label in [
                ("SELECT COUNT(*) FROM players", "Players"),
                ("SELECT COUNT(*) FROM chat_configs", "Chat configs"),
                ("SELECT COUNT(*) FROM caught_fish", "Caught fish"),
                ("SELECT COUNT(*) FROM star_transactions", "Star transactions"),
            ]:
                try:
                    cur.execute(q)
                    val = cur.fetchone()[0]
                except Exception:
                    val = 'n/a'
                out_lines.append(f"{label}: {val}")

            # Top players by coins
            out_lines.append("\nTop players by coins:")
            try:
                cur.execute("SELECT user_id, username, coins, stars FROM players ORDER BY coins DESC LIMIT 5")
                rows = cur.fetchall()
                if rows:
                    for r in rows:
                        out_lines.append(f"{r[1]} ({r[0]}): coins={r[2]} stars={r[3]}")
                else:
                    out_lines.append("(none)")
            except Exception as e:
                out_lines.append("Top query failed: " + str(e))

            conn.close()
        except Exception as e:
            out_lines.append("DB error: " + str(e))

        await update.message.reply_text("\n".join(out_lines))

    # debug notification commands removed — notifications are sent automatically on successful payments

    async def backupdb_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return
        try:
            import shutil, time, os
            src = os.environ.get('FISHBOT_DB_PATH', DB_PATH)
            dst_dir = os.path.join(os.path.dirname(src), 'backups')
            os.makedirs(dst_dir, exist_ok=True)
            ts = int(time.time())
            dst = os.path.join(dst_dir, f'fishbot.db.{ts}')
            shutil.copy2(src, dst)
            await update.message.reply_text(f"Backup created: {dst}")
        except Exception as e:
            await update.message.reply_text("Backup failed: " + str(e))

    async def getbackup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Owner-only: send the most recent backup (or live DB) to owner in private chat as .gz"""
        owner_id = 793216884
        user_id = getattr(update.effective_user, 'id', None)
        if user_id != owner_id:
            await update.message.reply_text("Нет доступа.")
            return

        try:
            import os, gzip, shutil
            from pathlib import Path

            src = os.environ.get('FISHBOT_DB_PATH', DB_PATH)
            src_path = Path(src)
            backups_dir = src_path.parent / 'backups'

            candidate = None
            if backups_dir.exists():
                files = sorted(backups_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
                if files:
                    candidate = files[0]
            if not candidate:
                candidate = src_path

            if not candidate.exists():
                await update.message.reply_text("Файл базы данных не найден.")
                return

            gz_path = candidate.with_suffix(candidate.suffix + '.gz')
            # create gzipped copy
            with open(candidate, 'rb') as f_in, gzip.open(gz_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

            # send in private chat
            try:
                with open(gz_path, 'rb') as f:
                    await context.bot.send_document(chat_id=user_id, document=f)
                await update.message.reply_text(f"Отправил {gz_path.name} в личку.")
            except Exception as e:
                await update.message.reply_text(f"Ошибка при отправке: {e}")
            finally:
                try:
                    gz_path.unlink()
                except Exception:
                    pass
        except Exception as e:
            await update.message.reply_text("Ошибка: " + str(e))

    async def restore_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Owner-only: restore the most recent backup found in backups/ to DB_PATH."""
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return
        try:
            import shutil, os
            src = os.environ.get('FISHBOT_DB_PATH', DB_PATH)
            backups_dir = os.path.join(os.path.dirname(src), 'backups')
            if not os.path.isdir(backups_dir):
                await update.message.reply_text(f"Backups directory not found: {backups_dir}")
                return
            files = sorted([os.path.join(backups_dir, f) for f in os.listdir(backups_dir)], key=lambda p: os.path.getmtime(p), reverse=True)
            if not files:
                await update.message.reply_text("No backup files found in backups directory.")
                return
            latest = files[0]
            # Make a safety copy of current DB
            current = src
            safe_copy = current + ".pre_restore"
            shutil.copy2(current, safe_copy)
            shutil.copy2(latest, current)
            await update.message.reply_text(f"Restored DB from {os.path.basename(latest)}. Saved previous DB as {os.path.basename(safe_copy)}.\nPlease restart the bot service.")
        except Exception as e:
            await update.message.reply_text("Restore failed: " + str(e))

    async def restart_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Owner-only: ask the container host to restart by exiting the process."""
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return
        try:
            await update.message.reply_text("Перезапускаю процесс бота для применения изменений...")
            # flush and exit immediately; container orchestrator should restart the service
            import os, sys, threading
            def _exit():
                try:
                    os._exit(0)
                except Exception:
                    sys.exit(0)
            # run exit shortly after replying to ensure message is sent
            t = threading.Timer(0.5, _exit)
            t.start()
        except Exception as e:
            await update.message.reply_text(f"Не удалось перезапустить: {e}")

    async def drop_trigger_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Owner-only: drop the caught_fish trigger if present."""
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return
        try:
            conn = db._connect()
            cur = conn.cursor()
            cur.execute('DROP TRIGGER IF EXISTS caught_fish_fix_chatid_after_insert')
            try:
                conn.commit()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            await update.message.reply_text('Trigger dropped (if existed). Please restart the bot service.')
        except Exception as e:
            await update.message.reply_text('Failed to drop trigger: ' + str(e))

    async def upload_backup_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Owner-only: save an uploaded backup file to the container backups directory.
        Send the .db file as a document with caption 'upload_backup' (case-insensitive) to save it.
        """
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            return
        try:
            msg = update.message
            doc = getattr(msg, 'document', None)
            if not doc:
                await update.message.reply_text("Пришлите файл базы данных как документ с подписью 'upload_backup'.")
                return
            import os, time
            src_env = os.environ.get('FISHBOT_DB_PATH', DB_PATH)
            backups_dir = os.path.join(os.path.dirname(src_env), 'backups')
            os.makedirs(backups_dir, exist_ok=True)
            filename = doc.file_name or f"uploaded_{int(time.time())}.db"
            dest_path = os.path.join(backups_dir, filename)
            file = await context.bot.get_file(doc.file_id)
            await file.download_to_drive(dest_path)
            await update.message.reply_text(f"Сохранено: {filename}")
        except Exception as e:
            await update.message.reply_text(f"Ошибка при сохранении файла: {e}")

    async def list_backups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Owner-only: list files in backups directory."""
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return
        try:
            import os
            src = os.environ.get('FISHBOT_DB_PATH', DB_PATH)
            backups_dir = os.path.join(os.path.dirname(src), 'backups')
            if not os.path.isdir(backups_dir):
                await update.message.reply_text(f"Папка бэкапов не найдена: {backups_dir}")
                return
            files = sorted(os.listdir(backups_dir), key=lambda f: os.path.getmtime(os.path.join(backups_dir, f)), reverse=True)
            if not files:
                await update.message.reply_text("В папке бэкапов нет файлов.")
                return
            text = "Последние бэкапы:\n" + "\n".join(files[:20])
            await update.message.reply_text(text)
        except Exception as e:
            await update.message.reply_text(f"Ошибка: {e}")

    async def chatstar_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Owner-only: return list of chats and stars_total. Use in private chat."""
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return

        # Ensure command is used in private
        chat = update.effective_chat
        if chat is None or getattr(chat, 'type', None) != 'private':
            await update.message.reply_text("Эту команду можно запускать только в личных сообщениях боту.")
            return

        try:
            chats = db.get_all_chat_stars()
        except Exception as e:
            logger.exception("chatstar: DB error: %s", e)
            await update.message.reply_text("Ошибка доступа к БД.")
            return

        if not chats:
            await update.message.reply_text("Нет данных по чатам.")
            return

        total = sum(int(c.get('stars_total', 0)) for c in chats)
        lines = [f"Всего звёзд: {total}", ""]
        for c in chats:
            title = c.get('chat_title') or ''
            if not title:
                # try fetching title from Telegram and update DB
                try:
                    chat_id = c.get('chat_id')
                    if chat_id:
                        chat_obj = await bot_instance.application.bot.get_chat(chat_id)
                        fetched_title = getattr(chat_obj, 'title', None) or getattr(chat_obj, 'username', None) or (getattr(chat_obj, 'first_name', None) or '')
                        if fetched_title:
                            title = fetched_title
                            try:
                                db.update_chat_title(chat_id, title)
                            except Exception:
                                pass
                except Exception:
                    title = f"chat:{c.get('chat_id')}"
            if not title:
                title = f"chat:{c.get('chat_id')}"
            stars = c.get('stars_total', 0)
            lines.append(f"{title} — {stars}")

        # Send as multiple messages if too long
        text = "\n".join(lines)
        if len(text) > 3900:
            # chunk by lines
            chunk = []
            cur_len = 0
            for ln in lines:
                if cur_len + len(ln) + 1 > 3900:
                    await bot_instance._safe_send_message(chat_id=owner_id, text="\n".join(chunk))
                    chunk = [ln]
                    cur_len = len(ln) + 1
                else:
                    chunk.append(ln)
                    cur_len += len(ln) + 1
            if chunk:
                await bot_instance._safe_send_message(chat_id=owner_id, text="\n".join(chunk))
        else:
            await bot_instance._safe_send_message(chat_id=owner_id, text=text)

    async def grant_net_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return

        parts = (update.message.text or '').split()
        if len(parts) < 3:
            await update.message.reply_text("Использование: /grant_net <user_id> <net_name|netN> [count]")
            return
        try:
            target_user = int(parts[1])
        except Exception:
            await update.message.reply_text("Неверный user_id. Пример: /grant_net 123456 net0 1")
            return
        raw_net = parts[2]
        count = 1
        if len(parts) >= 4:
            try:
                count = int(parts[3])
            except Exception:
                count = 1

        # Map netN -> index in nets list (0-based)
        net_name = raw_net
        m = re.match(r'^net(\d+)$', raw_net, re.I)
        if m:
            idx = int(m.group(1))
            nets = db.get_nets()
            if 0 <= idx < len(nets):
                net_name = nets[idx]['name']
            else:
                await update.message.reply_text(f"Нет сети с индексом {idx}")
                return

        ok = db.grant_net(target_user, net_name, getattr(update.effective_chat, 'id', -1), count)
        if ok:
            await update.message.reply_text(f"Сеть '{net_name}' выдана пользователю {target_user} (x{count}).")
            # Попытаться отправить личное сообщение получателю
            sender = update.effective_user
            sender_name = getattr(sender, 'username', None) or getattr(sender, 'first_name', 'Пользователь')
            dm_text = f"{sender_name} подарил вам: {net_name}." 
            try:
                # use bot_instance safe wrapper
                res = await bot_instance._safe_send_message(chat_id=target_user, text=dm_text)
                if res is None:
                    await update.message.reply_text(f"Не удалось доставить уведомление пользователю {target_user} (возможно, он не писал боту).")
            except Exception as e:
                logger.exception("Failed to send DM after grant_net: %s", e)
                await update.message.reply_text("Не удалось отправить личное сообщение получателю.")
        else:
            await update.message.reply_text(f"Не удалось выдать сеть '{net_name}'. Проверьте имя сети.")

    async def grant_rod_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
        owner_id = 793216884
        if getattr(update.effective_user, 'id', None) != owner_id:
            await update.message.reply_text("Нет доступа.")
            return

        parts = (update.message.text or '').split()
        if len(parts) < 3:
            await update.message.reply_text("Использование: /grant_rod <user_id> <rod_name|rodN>")
            return
        try:
            target_user = int(parts[1])
        except Exception:
            await update.message.reply_text("Неверный user_id. Пример: /grant_rod 123456 rod0")
            return
        raw_rod = parts[2]

        rod_name = raw_rod
        m = re.match(r'^rod(\d+)$', raw_rod, re.I)
        if m:
            idx = int(m.group(1))
            rods = db.get_rods()
            if 0 <= idx < len(rods):
                rod_name = rods[idx]['name']
            else:
                await update.message.reply_text(f"Нет удочки с индексом {idx}")
                return

        ok = db.grant_rod(target_user, rod_name, getattr(update.effective_chat, 'id', -1))
        if ok:
            await update.message.reply_text(f"Удочка '{rod_name}' выдана пользователю {target_user}.")
            sender = update.effective_user
            sender_name = getattr(sender, 'username', None) or getattr(sender, 'first_name', 'Пользователь')
            dm_text = f"{sender_name} подарил вам: {rod_name}."
            try:
                res = await bot_instance._safe_send_message(chat_id=target_user, text=dm_text)
                if res is None:
                    await update.message.reply_text(f"Не удалось доставить уведомление пользователю {target_user} (возможно, он не писал боту).")
            except Exception as e:
                logger.exception("Failed to send DM after grant_rod: %s", e)
                await update.message.reply_text("Не удалось отправить личное сообщение получателю.")
        else:
            await update.message.reply_text(f"Не удалось выдать удочку '{rod_name}'. Проверьте имя удочки.")

    # Добавление обработчиков
    application.add_handler(CommandHandler("dbinfo", dbinfo_command))
    application.add_handler(CommandHandler("start", bot_instance.start))
    application.add_handler(CommandHandler("dbstats", dbstats_command))
    application.add_handler(CommandHandler("backupdb", backupdb_command))
    application.add_handler(CommandHandler("getbackup", getbackup_command))
    application.add_handler(CommandHandler("list_backups", list_backups_command))
    application.add_handler(CommandHandler("restore_backup", restore_backup_command))
    application.add_handler(CommandHandler("restart", restart_bot_command))
    application.add_handler(CommandHandler("drop_trigger", drop_trigger_command))
    # Owner can upload a backup file as a document with caption 'upload_backup'
    application.add_handler(MessageHandler(filters.Document.ALL & filters.CaptionRegex('(?i)upload_backup') & filters.User(793216884), upload_backup_handler))
    application.add_handler(CommandHandler("grant_net", grant_net_command))
    application.add_handler(CommandHandler("grant_rod", grant_rod_command))
    application.add_handler(CommandHandler("chatstar", chatstar_command))
    application.add_handler(CommandHandler("ref", bot_instance.ref_command))
    application.add_handler(CommandHandler("new_ref", bot_instance.new_ref_command))
    application.add_handler(CommandHandler("check", bot_instance.check_command))
    application.add_handler(CommandHandler("new_tour", bot_instance.new_tour_command))
    application.add_handler(CommandHandler("tour", bot_instance.tour_command))
    application.add_handler(CommandHandler("ozero", bot_instance.ozero_command))
    application.add_handler(CommandHandler("reka", bot_instance.reka_command))
    application.add_handler(CommandHandler("more", bot_instance.more_command))
    application.add_handler(CommandHandler("prud", bot_instance.prud_command))
    application.add_handler(CommandHandler("mes", bot_instance.mes_command))
    application.add_handler(CommandHandler("tour", bot_instance.tour_command))
    application.add_handler(CommandHandler("ozero", bot_instance.ozero_command))
    application.add_handler(CommandHandler("reka", bot_instance.reka_command))
    application.add_handler(CommandHandler("more", bot_instance.more_command))
    application.add_handler(CommandHandler("prud", bot_instance.prud_command))
    application.add_handler(CommandHandler("mes", bot_instance.mes_command))
    # debug handlers removed
    application.add_handler(CommandHandler("fish", bot_instance.fish_command))
    application.add_handler(CommandHandler("menu", bot_instance.menu_command))
    application.add_handler(CommandHandler("shop", bot_instance.handle_shop))
    application.add_handler(CommandHandler("net", bot_instance.net_command))
    application.add_handler(CommandHandler("dynamite", bot_instance.dynamite_command))
    application.add_handler(CommandHandler("weather", bot_instance.weather_command))
    application.add_handler(CommandHandler("testweather", bot_instance.test_weather_command))
    application.add_handler(CommandHandler("stats", bot_instance.stats_command))
    application.add_handler(CommandHandler("rules", bot_instance.rules_command))
    application.add_handler(CommandHandler("info", bot_instance.info_command))
    application.add_handler(CommandHandler("treasureinfo", bot_instance.treasureinfo_command))
    application.add_handler(CommandHandler("stars", bot_instance.stars_command))
    application.add_handler(CommandHandler("topl", bot_instance.topl_command))
    application.add_handler(CommandHandler("leaderboard", bot_instance.leaderboard_command))
    application.add_handler(CommandHandler("repair", bot_instance.repair_command))
    application.add_handler(CommandHandler("help", bot_instance.help_command))
    application.add_handler(CommandHandler("test", bot_instance.test_command))
    
    # Обработчики платежей
    application.add_handler(PreCheckoutQueryHandler(bot_instance.precheckout_callback))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, bot_instance.successful_payment_callback))
    
    # Обработчик новых участников группы отключён — не присылаем автоматические приветствия
    # (application.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, bot_instance.welcome_new_member)))
    
    # Ввод для сценариев /ref и /new_ref.
    # В python-telegram-bot в рамках одной группы выполняется только первый подошедший handler,
    # поэтому разносим обработчики по группам: сценарные -> общий текстовый.
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot_instance.handle_withdraw_stars_input), group=0)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot_instance.handle_new_ref_input), group=1)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot_instance.handle_check_input), group=2)

    # Обработчик сообщений о рыбалке и покупке наживки
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot_instance.handle_fish_message), group=3)
    
    # Обработчик стикеров
    application.add_handler(MessageHandler(filters.Sticker.ALL, bot_instance.handle_sticker))
    
    # Обработчик возврата платежей (использует refunded_payment)
    application.add_handler(MessageHandler(filters.ALL, bot_instance.refunded_payment_callback))
    
    # Обработчики callback
    application.add_handler(CallbackQueryHandler(bot_instance.handle_start_fishing, pattern="^start_fishing_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_change_location, pattern="^change_location_"))
    # Важно: более специфичные паттерны должны идти первыми
    application.add_handler(CallbackQueryHandler(bot_instance.handle_change_bait_location, pattern="^change_bait_loc_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_change_rod, pattern="^change_rod_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_change_bait, pattern=r"^change_bait_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_select_location, pattern="^select_location_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_select_rod, pattern="^select_rod_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_select_rod, pattern="^sr_"))  # Короткий формат
    application.add_handler(CallbackQueryHandler(bot_instance.handle_instant_repair, pattern="^instant_repair_"))  # Мгновенный ремонт
    application.add_handler(CallbackQueryHandler(bot_instance.handle_select_bait_buy, pattern="^select_bait_buy_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_select_bait_buy, pattern="^sb_"))  # Короткий формат
    application.add_handler(CallbackQueryHandler(bot_instance.handle_select_bait, pattern="^select_bait_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_select_bait, pattern="^sbi_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_select_net, pattern="^select_net_"))  # Выбор сети в меню
    application.add_handler(CallbackQueryHandler(bot_instance.handle_pay_invoice_callback, pattern="^pay_invoice:"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_use_harpoon_paid, pattern="^use_harpoon_paid_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_use_harpoon, pattern="^use_harpoon_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_use_net, pattern="^use_net_"))  # Использование сетей
    application.add_handler(CallbackQueryHandler(bot_instance.handle_net_skip_cd, pattern="^net_skip_cd_"))  # Сброс КД сетей
    application.add_handler(CallbackQueryHandler(bot_instance.handle_back_to_menu, pattern="^back_to_menu_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_sell_fish, pattern=r"^sell_fish_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_sell_fish, pattern=r"^sell_page_\d+_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_sell_species, pattern="^sell_sp_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_sell_all, pattern=r"^sell_all_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_confirm_sell_all, pattern=r"^confirm_sell_all_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_cancel_sell_all, pattern=r"^cancel_sell_all_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_sell_quantity_cancel, pattern=r"^sell_quantity_cancel_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_inventory, pattern=r"^inventory_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_sell_treasure, pattern=r"^sell_treasure_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_inventory_treasures, pattern=r"^inv_treasures_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_inventory_location, pattern="^inv_location_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_shop, pattern=r"^shop_\d+$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_shop_rods, pattern="^shop_rods_"))
    # Важно: более специфичные паттерны должны идти первыми
    application.add_handler(CallbackQueryHandler(bot_instance.handle_shop_baits_location, pattern="^shop_baits_loc_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_shop_baits, pattern="^shop_baits_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_shop_nets, pattern="^shop_nets_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_shop_feeders, pattern="^shop_feeders_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_shop_exchange, pattern="^shop_exchange_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_exchange_buy_diamond, pattern="^exchange_buy_diamond_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_exchange_sell_diamond, pattern="^exchange_sell_diamond_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_exchange_shell_to_pearl, pattern="^exchange_shell_to_pearl_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_exchange_pearl_to_diamond, pattern="^exchange_pearl_to_diamond_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_buy_rod, pattern="^buy_rod_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_buy_net, pattern="^buy_net_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_buy_feeder_coins, pattern="^buy_feeder_coins_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_buy_feeder_stars, pattern="^buy_feeder_stars_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_buy_echosounder, pattern="^buy_echosounder_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_show_echosounder, pattern="^show_echosounder_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_repair_callback, pattern="^repair_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_stats_callback, pattern="^stats_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_leaderboard_callback, pattern="^leaderboard$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_tour_type_callback, pattern="^tour_type_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_tour_location_callback, pattern="^tour_location_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_payment_expired_callback, pattern="^payment_expired$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_invoice_cancelled_callback, pattern="^invoice_cancelled$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_pay_telegram_star_callback, pattern="^pay_telegram_star_"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_invoice_sent_callback, pattern="^invoice_sent$"))
    application.add_handler(CallbackQueryHandler(bot_instance.handle_approve_withdraw_callback, pattern="^approve_withdraw_"))
    
    # Обработчик ошибок
    application.add_error_handler(bot_instance.error_handler)
    
    print("🎣 Бот для рыбалки запущен!")
    
    # Запуск бота с обработкой ошибок
    # drop_pending_updates=True — при перезапуске все сообщения, отправленные
    # пока бот был выключен, будут проигнорированы (старые рыбалки не сработают).
    try:
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=[
                "message",
                "callback_query",
                "pre_checkout_query",
                "chosen_inline_result",
            ],
        )
        print("✅ Polling запущен успешно")
    except Exception as e:
        print(f"❌ Ошибка запуска бота: {e}")
        return

if __name__ == '__main__':
    main()
