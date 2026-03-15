# Справочник драгоценностей для выпадения при ловле мусора
# Формат: драгоценность -> {вероятность, цена, красивое_имя}

TREASURES = {
    "Ракушка": {
        "probability": 10.0,        # 10%
        "sell_price": 250,          # монеты при продаже
        "sell_xp": 5,               # опыт при продаже
        "display_name": "🐚 Ракушка"
    },
    "Жемчуг": {
        "probability": 0.5,         # 0.5%
        "sell_price": 3000,
        "sell_xp": 60,
        "display_name": "💎 Жемчуг"
    },
    "Кристалл": {
        "probability": 1.5,         # 1.5%
        "sell_price": 1800,
        "sell_xp": 35,
        "display_name": "✨ Кристалл"
    },
    "Трезубец Посейдона": {
        "probability": 0.2,         # 0.2%
        "sell_price": 25000,
        "sell_xp": 400,
        "display_name": "⚜️ Трезубец Посейдона"
    },
    "Золотой нож": {
        "probability": 0.8,         # 0.8%
        "sell_price": 7000,
        "sell_xp": 120,
        "display_name": "🔪 Золотой нож"
    },
    "Золотое кольцо": {
        "probability": 1.0,         # 1%
        "sell_price": 5500,
        "sell_xp": 95,
        "display_name": "💍 Золотое кольцо"
    },
    "Браслет": {
        "probability": 0.7,         # 0.7%
        "sell_price": 5000,
        "sell_xp": 85,
        "display_name": "✨ Браслет"
    },
    "Подвеска": {
        "probability": 1.2,         # 1.2%
        "sell_price": 4000,
        "sell_xp": 70,
        "display_name": "🔮 Подвеска"
    },
}

# Курсы обмена бриллиантов (хранится в поле diamonds в БД)
DIAMOND_BUY_PRICE = 500000      # 1 бриллиант = 500k монет (при покупке)
DIAMOND_SELL_PRICE = 250000     # 1 бриллиант = 250k монет (при продаже)

def get_treasure_name(treasure_key: str) -> str:
    """Получить красивое имя драгоценности"""
    if treasure_key in TREASURES:
        return TREASURES[treasure_key]["display_name"]
    return treasure_key

def get_treasure_probability(treasure_key: str) -> float:
    """Получить вероятность выпадения (в процентах)"""
    if treasure_key in TREASURES:
        return TREASURES[treasure_key]["probability"]
    return 0.0

def get_treasure_sell_price(treasure_key: str) -> int:
    """Получить цену продажи драгоценности в монетах"""
    if treasure_key in TREASURES:
        return TREASURES[treasure_key]["sell_price"]
    return 0

def get_treasure_sell_xp(treasure_key: str) -> int:
    """Получить опыт при продаже драгоценности"""
    if treasure_key in TREASURES:
        return TREASURES[treasure_key]["sell_xp"]
    return 0

def get_treasures_info() -> str:
    """Получить информацию о всех драгоценностях и их шансах выпадения"""
    info = "📊 <b>Шансы выпадения клада при поймке мусора</b>\n\n"
    
    # Сортируем по вероятности (по убыванию)
    sorted_treasures = sorted(
        TREASURES.items(),
        key=lambda x: x[1]["probability"],
        reverse=True
    )
    
    for treasure_key, treasure_data in sorted_treasures:
        prob = treasure_data["probability"]
        sell_price = treasure_data["sell_price"]
        sell_xp = treasure_data["sell_xp"]
        display_name = treasure_data["display_name"]
        
        # Форматируем вероятность красиво
        if prob >= 1:
            prob_str = f"{prob}%"
        else:
            prob_str = f"{prob}%"
        
        info += f"{display_name}\n"
        info += f"  • Шанс: {prob_str}\n"
        info += f"  • За продажу: {sell_price} 🪙 + {sell_xp} ✨ опыта\n\n"
    
    return info
