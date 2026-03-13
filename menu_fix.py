# Fix for menu display with custom emojis
# This script shows what needs to be in show_fishing_menu function

menu_code = '''
        diamond_count = player.get('diamonds', 0)
        coin_emoji = '<tg-emoji emoji-id="5379600444098093058">⭐</tg-emoji>'
        diamond_emoji = '<tg-emoji emoji-id="5347855243556129844">💎</tg-emoji>'
        rod_emoji = '<tg-emoji emoji-id="5343609421316521960">🎣</tg-emoji>'
        location_emoji = '<tg-emoji emoji-id="5821128296217185461">📍</tg-emoji>'
        bait_emoji = '<tg-emoji emoji-id="5233206123036682153">🪱</tg-emoji>'
        
        menu_text = f"""
{rod_emoji} Меню рыбалки

{coin_emoji} Монеты: {html.escape(str(player['coins']))} {html.escape(COIN_NAME)}
{diamond_emoji} Бриллианты: {html.escape(str(diamond_count))}
{rod_emoji} Удочка: {html.escape(str(player['current_rod']))}
{location_emoji} Локация: {html.escape(str(player['current_location']))}
{bait_emoji} Наживка: {html.escape(str(player['current_bait']))}
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
'''
