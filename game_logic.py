import sqlite3
from typing import Dict, Any, Tuple, List
from datetime import datetime, timedelta
import random
import logging
from pathlib import Path
from config import CATCH_CHANCE, NO_BITE_CHANCE, GUARANTEED_CATCH_COST, COOLDOWN_MINUTES, ROD_REPAIR_COST, CURRENT_SEASON, TRASH_CHANCE, get_current_season
from database import db, DB_PATH, BAMBOO_ROD, TEMP_ROD_RANGES
from weather import weather_system

logger = logging.getLogger(__name__)

class FishingGame:
    def __init__(self):
        self.current_season = self._get_current_season()
    
    def _normalize_fish_list(self, fish_list):
        """Ensure each fish entry is a dict with keys accessible by name.

        Some DB callers may return tuples or sqlite rows; normalize defensively.
        """
        if not fish_list:
            return fish_list
        normalized = []
        # Known fish columns order used by database.get_fish... queries
        keys = ['id','name','rarity','min_weight','max_weight','min_length','max_length','price','locations','seasons','suitable_baits','max_rod_weight','required_level','sticker_id']
        for f in fish_list:
            if isinstance(f, dict):
                normalized.append(f)
                continue
            if isinstance(f, (list, tuple)):
                normalized.append(dict(zip(keys, f)))
                continue
            try:
                normalized.append(dict(f))
            except Exception:
                normalized.append({})
        return normalized
    
    def _get_current_season(self) -> str:
        """Определить текущее время года"""
        return get_current_season()
    
    def get_durability_damage(self, catch_type: str, is_guaranteed: bool = False) -> int:
        """Получить урон прочности в зависимости от типа добычи и типа ловли
        
        Обычная ловля:
        - мусор: -1
        - обычная рыба: -5
        - редкая рыба: -10
        - легендарная рыба: -15
        
        Гарантированная ловля:
        - обычная рыба: -1
        - редкая рыба: -2
        - легендарная рыба: -3
        """
        if is_guaranteed:
            if catch_type == "Обычная":
                return 1
            elif catch_type == "Редкая":
                return 2
            elif catch_type == "Легендарная":
                return 3
            else:  # мусор или неловля при гарантированном
                return 0
        else:
            if catch_type == "trash":
                return 1
            elif catch_type == "Обычная":
                return 5
            elif catch_type == "Редкая":
                return 10
            elif catch_type == "Легендарная":
                return 15
            else:  # неловля при обычной ловле
                return 0

    def _consume_temp_rod_use(self, user_id: int, chat_id: int, rod_name: str) -> Dict[str, Any]:
        """Списать использование временной удочки и переключить на бамбук при поломке"""
        if rod_name not in TEMP_ROD_RANGES:
            return {"broken": False}

        result: Dict[str, Any] = db.consume_temp_rod_use(user_id, rod_name, chat_id)
        if result.get("broken"):
            db.update_player(user_id, chat_id, current_rod=BAMBOO_ROD)
        return result
    
    def can_fish(self, user_id: int, chat_id: int) -> Tuple[bool, str]:
        """Проверить, может ли игрок рыбачить"""
        player: Dict[str, Any] = db.get_player(user_id, chat_id)
        if not player:
            return False, "Сначала создайте профиль командой /start"

        if player.get('current_rod') == 'Гарпун':
            db.init_player_rod(user_id, BAMBOO_ROD, chat_id)
            db.update_player(user_id, chat_id, current_rod=BAMBOO_ROD)
            player = db.get_player(user_id, chat_id) or player
        
        # Проверка прочности удочки - если 0, нельзя ловить вообще
        player_rod = db.get_player_rod(user_id, player['current_rod'], chat_id)
        if player_rod:
            current_dur = player_rod.get('current_durability', 100)
            if current_dur <= 0:
                if player['current_rod'] in TEMP_ROD_RANGES:
                    return False, "Ваша удочка сломалась! Купите новую в магазине."
                return False, "Ваша удочка сломалась! Почините её командой /repair или подождите автовосстановления."
        
        # Проверка кулдауна
        last_fish = player.get('last_fish_time')
        logger.debug(f"can_fish: user={user_id} chat={chat_id} last_fish={last_fish} COOLDOWN_MINUTES={COOLDOWN_MINUTES}")
        if last_fish:
            last_time = datetime.fromisoformat(last_fish)
            time_passed = datetime.now() - last_time
            if time_passed < timedelta(minutes=COOLDOWN_MINUTES):
                remaining = timedelta(minutes=COOLDOWN_MINUTES) - time_passed
                minutes = int(remaining.total_seconds() // 60)
                seconds = int((remaining.total_seconds() % 60))
                logger.debug(f"can_fish: user={user_id} time_passed={time_passed}, remaining={remaining}")
                return False, f"Следующий заброс через {minutes}мин {seconds}сек"
        
        return True, ""
    
    def _get_time_until_repair(self, ban_until: str) -> str:
        """Получить время до починки удочки"""
        ban_time = datetime.fromisoformat(ban_until)
        remaining = ban_time - datetime.now()
        hours = int(remaining.total_seconds() // 3600)
        minutes = int((remaining.total_seconds() % 3600) // 60)
        return f"{hours}ч {minutes}мин"

    def fish_with_harpoon(self, user_id: int, chat_id: int, location: str) -> Dict[str, Any]:
        """Отдельная механика гарпуна (не удочка, отдельный инструмент)."""
        player = db.get_player(user_id, chat_id)
        if not player:
            return {
                "success": False,
                "message": "Профиль не найден. Используйте /start в этом чате.",
                "location": location,
            }

        player_level = player.get('level', 0) or 0
        self.current_season = self._get_current_season()

        fish_list = db.get_fish_by_location(location, self.current_season, min_level=player_level)
        fish_list = self._normalize_fish_list(fish_list)
        fish_list = [f for f in fish_list if float(f.get('min_weight', 0) or 0) >= 150]

        if not fish_list:
            return {
                "success": False,
                "message": "🐟 В этой локации нет рыбы для гарпуна (нужна рыба от 150 кг).",
                "location": location,
            }

        caught_fish = random.choice(fish_list)
        weight = round(random.uniform(float(caught_fish['min_weight']), float(caught_fish['max_weight'])), 2)
        length = round(random.uniform(float(caught_fish['min_length']), float(caught_fish['max_length'])), 1)

        if weight < 150:
            return {
                "success": False,
                "message": "Гарпун разорвал рыбу на две части 😢",
                "location": location,
            }

        db.add_caught_fish(user_id, chat_id, caught_fish['name'], weight, location, length)

        return {
            "success": True,
            "fish": caught_fish,
            "weight": weight,
            "length": length,
            "location": location,
            "harpoon": True,
        }
    
    def fish(self, user_id: int, chat_id: int, location: str = "Городской пруд", guaranteed: bool = False) -> Dict[str, Any]:
        """Основная функция ловли рыбы"""
        # Проверка на арест рыбнадзором
        player = db.get_player(user_id, chat_id)
        if player and player.get('is_banned'):
            ban_until = player.get('ban_until')
            if ban_until:
                now = datetime.now()
                ban_time = datetime.fromisoformat(ban_until)
                if now < ban_time:
                    remaining = ban_time - now
                    hours = int(remaining.total_seconds() // 3600)
                    minutes = int((remaining.total_seconds() % 3600) // 60)
                    return {
                        "success": False,
                        "message": f"⛔️ Вас арестовал рыбнадзор! До окончания ареста: {hours}ч {minutes}мин. Можно откупиться за 15 звезд командой /payfine"
                    }
                db.update_player(user_id, chat_id, is_banned=0, ban_until=None)

        # Проверка cooldown - не нужна для гарантированного улова (расплачено звездами)
        if not guaranteed:
            can_fish, message = self.can_fish(user_id, chat_id)
            if not can_fish:
                return {"success": False, "message": message}

        player = db.get_player(user_id, chat_id)
        if not player:
            return {
                "success": False,
                "message": "Профиль не найден. Используйте /start в этом чате.",
                "location": location
            }
        if player.get('current_rod') == 'Гарпун':
            db.init_player_rod(user_id, BAMBOO_ROD, chat_id)
            db.update_player(user_id, chat_id, current_rod=BAMBOO_ROD)
            player = db.get_player(user_id, chat_id) or player
        player_level = player.get('level', 0) or 0
        rod = db.get_rod(player['current_rod'])

        # Получаем бонус от наживки
        current_bait = db.get_player_baits(user_id) or []
        bait_bonus = 0
        for bait in current_bait:
            if bait['name'] == player['current_bait']:
                bait_bonus = bait['fish_bonus']
                break

        # Обновляем сезон
        self.current_season = self._get_current_season()
        feeder_bonus = db.get_active_feeder_bonus(user_id, chat_id)

        # Если гарантированный улов
        if guaranteed:
            return self._guaranteed_catch(user_id, location, player, chat_id, feeder_bonus)

        # Получаем погоду и применяем бонус
        weather = db.get_or_update_weather(location)
        weather_bonus = 0
        weather_condition = "Ясно"

        if weather:
            weather_condition = weather['condition']
            weather_bonus = weather_system.get_weather_bonus(weather_condition)
            logger.info(f"   🌍 Weather: {weather_condition} (bonus: {weather_bonus:+d}%)")

        # Единая механика для всех локаций: один бросок от 0 до 10000
        # 0-3000 = ничего не клюёт
        # 3001-6000 = мусор
        # 6001-8500 = обычная
        # 8501-9700 = редкая
        # 9701-9999 = легендарная
        # 10000 = NFT
        roll = random.randint(0, 10000)

        # Применяем погодный бонус/штраф и бонус кормушки
        adjusted_roll = roll + (weather_bonus * 50) + (feeder_bonus * 250)
        adjusted_roll = max(0, min(10000, adjusted_roll))  # Ограничиваем от 0 до 10000

        # --- ГАРПУН: спец.логика ---
        if rod and rod['name'] == 'Гарпун':
            # Ограничение: только рыба 150кг+ (и огромные)
            # Найдём подходящую рыбу (если выпадет меньше - fail)
            # Получаем список возможной рыбы для локации и сезона
            fish_list = db.get_fish_for_location(location, self.current_season, player_level)
            fish_list = [f for f in fish_list if f['min_weight'] >= 150]
            if not fish_list:
                return {"success": False, "message": "🐟 В этой локации нет рыбы для гарпуна!"}
            # Симулируем обычный roll, но если выпала рыба < 150кг, fail
            # (остальная логика ниже, но после выбора рыбы)
            # ...existing code...
            # После выбора рыбы:
            # if caught_fish['weight'] < 150:
            #     return {"success": False, "message": "Гарпун разорвал рыбу на две части 😢"}
            # (реализация ниже в коде catch)
        
        logger.info(f"🎣 User {user_id} started fishing at location: {location}")
        logger.info(
            f"   🎲 Random roll: {roll}/10000 (adjusted: {adjusted_roll}/10000 "
            f"with weather {weather_condition}, feeder {feeder_bonus:+d}%)"
        )
        logger.info("   📊 Ranges: 0-3000=NO_BITE, 3001-6000=TRASH, 6001-8500=COMMON, 8501-9700=RARE, 9701-9999=LEGENDARY, 10000=NFT")
        
        if roll == 10000:
            logger.info("   🏆 Result: NFT WIN (raw roll 10000)")
            db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
            return {
                "success": False,
                "nft_win": True,
                "location": location
            }

        force_legendary = adjusted_roll >= 9701
        if force_legendary:
            logger.info("   🎯 Forced LEGENDARY (adjusted roll >= 9701, NFT only on raw 10000)")

        if not force_legendary and adjusted_roll <= 3000:
            logger.info(f"   📊 Result: NO_BITE (adjusted roll {adjusted_roll} <= 3000)")
            no_bite_messages = [
                "Рыба сегодня не клюет...",
                "Поклевки нет, попробуйте позже",
                "Рыба спит на дне",
                "Сегодня плохой клев",
                "Рыба не интересуется приманкой",
                "Попробуйте другую локацию",
                "Вода слишком холодная для рыбы",
                "Рыба ушла на глубину"
            ]
            
            db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
            return {
                "success": False,
                "message": random.choice(no_bite_messages),
                "location": location,
                "no_bite": True
            }
        if not force_legendary and adjusted_roll <= 6000:  # 3001-6000
            logger.info("   📊 Result: TRASH (adjusted roll in range 3001-6000)")
            trash = db.get_random_trash(location)
            if trash:
                logger.info(f"   🗑️ Caught trash: {trash['name']}")
                
                # Применяем урон прочности удочки
                damage = self.get_durability_damage("trash", is_guaranteed=False)
                db.reduce_rod_durability(user_id, player['current_rod'], damage, chat_id)

                xp_earned = db.calculate_item_xp({
                    'rarity': 'Мусор',
                    'weight': trash.get('weight', 0),
                    'min_weight': 0,
                    'max_weight': 0,
                    'is_trash': True,
                })
                level_info = db.add_player_xp(user_id, chat_id, xp_earned)

                db.update_player(user_id, chat_id,
                                coins=player['coins'] + trash['price'],
                                last_fish_time=datetime.now().isoformat())

                temp_rod_result = self._consume_temp_rod_use(user_id, chat_id, player['current_rod'])
                
                trash_messages = [
                    f"😑 Ловля... Из воды выловлена {trash['name']}!",
                    f"🗑️ Ловля... Поймали {trash['name']}!",
                    f"😤 Ловля... Это был {trash['name']}, а не рыба!",
                ]
                
                return {
                    "success": False,
                    "is_trash": True,
                    "trash": trash,
                    "location": location,
                    "message": random.choice(trash_messages),
                    "earned": trash['price'],
                    "new_balance": player['coins'] + trash['price'],
                    "xp_earned": xp_earned,
                    "level_info": level_info,
                    "temp_rod_broken": temp_rod_result.get("broken", False)
                }
        
        # 6001-9999 = ловим рыбу с определением редкости
        logger.info("   📊 Result: CATCH (adjusted roll in range 6001-9999)")

        if force_legendary:
            target_rarity = "Легендарная"
        elif adjusted_roll <= 8500:
            target_rarity = "Обычная"
            logger.info("   🎯 Rarity: COMMON (adjusted roll in 6001-8500)")
        elif adjusted_roll <= 9700:
            target_rarity = "Редкая"
            logger.info("   🎯 Rarity: RARE (adjusted roll in 8501-9700)")
        else:
            target_rarity = "Легендарная"
            logger.info("   🎯 Rarity: LEGENDARY (adjusted roll in 9701-9999)")
        
        # Получаем список рыб для локации и сезона
        # Всегда учитывать сезон при выборе списка рыб (даже для легендарных)
        fish_list = db.get_fish_by_location(location, self.current_season, min_level=player_level)
        # Normalize rows to dicts in case some DB callers return tuples
        fish_list = self._normalize_fish_list(fish_list)
        if fish_list is None:
            fish_list = []
        if fish_list is None:
            fish_list = []
        if not fish_list:
            logger.info(f"   ⚠️ No fish available for location: {location}, season: {self.current_season}")
            db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
            return {
                "success": False,
                "message": "На этой локации нет рыбы в текущее время года.",
                "location": location
            }

        if force_legendary:
            legendary_fish = [f for f in fish_list if f['rarity'] == target_rarity]
            if not legendary_fish:
                logger.info(f"   ⚠️ No fish of rarity {target_rarity} available in season {self.current_season} for location {location} - SNAP")
                db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
                return {
                    "success": False,
                    "snap": True,
                    "message": f"В этой локации нет рыбы редкости {target_rarity} в текущий сезон — срыв.",
                    "location": location
                }
            caught_fish = random.choice(legendary_fish)
        else:
            # ============ МЕХАНИКА НАЖИВКИ: 90% на нужную наживку, 10% срыв ============
            bait_success_roll = random.randint(1, 100)
            logger.info(f"   🪱 Bait roll: {bait_success_roll}/100 (1-90=right bait, 91-100=wrong bait snap)")
            
            use_correct_bait = bait_success_roll <= 90
            
            # Ищем рыбу с НУЖНОЙ наживкой И НУЖНОЙ РЕДКОСТЬЮ
            correct_bait_fish = [
                f for f in fish_list 
                if db.check_bait_suitable_for_fish(player['current_bait'], f['name']) 
                and f['rarity'] == target_rarity
            ]
            
            # Если нет рыбы нужной редкости с нужной наживкой - оставляем пустой список
            # (в этом случае будет считаться, что рыба сорвалась)
            # no fallback to other rarities to avoid catching different rarity fish
            # if not correct_bait_fish: keep it empty and treat as snap below
            
            # Ищем рыбу с ЧУЖОЙ наживкой (для 10% случаев срыва)
            wrong_bait_fish = [f for f in fish_list if f['rarity'] == target_rarity]
            
            # Применяем выбор на основе броска наживки
            if use_correct_bait:
                # 90% - ловим рыбу на нужную наживку
                if correct_bait_fish:
                    logger.info(f"   ✅ Using correct bait - fishing for {player['current_bait']} suitable fish")
                    caught_fish = random.choice(correct_bait_fish)
                    logger.info(f"   🐟 Caught fish: {caught_fish['name']} (rarity: {caught_fish['rarity']}, bait: {player['current_bait']})")
                else:
                    # Нет рыбы на эту наживку - СРЫВ из-за неправильной наживки
                    logger.info(f"   ⚠️ No fish for bait '{player['current_bait']}' at {location} - treating as SNAP")
                    snap_messages = [
                        f"🪝 Рыба клюнула, но наживка {player['current_bait']} ей не подошла - рыба сорвалась!",
                        f"⚠️ Поклевка была, но рыба не клюет на {player['current_bait']} - срыв!",
                        f"😤 Почти поймал! Но рыба отказалась от {player['current_bait']}...",
                        f"🎣 Срыв! Попробуйте другую наживку для этой локации.",
                    ]
                    db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
                    return {
                        "success": False,
                        "snap": True,
                        "message": random.choice(snap_messages),
                        "location": location,
                        "wrong_bait": player['current_bait']
                    }
            else:
                # 10% - попытка поймать рыбу на чужую наживку = СРЫВ
                logger.info(f"   ❌ Wrong bait attempt - SNAP/BREAK!")
                snap_messages = [
                    "🪝 Рыба интенсивно тянула, но наживка оказалась чужой - рыба сорвалась!",
                    "⚠️ Рыба клюнула агрессивно на неправильную наживку, но вырвалась!",
                    "😤 Почти поймал! Но рыба не клюет на эту наживку...",
                    "🎣 Срыв! Попытался ловить рыбу не на ту наживку!",
                ]
                db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
                return {
                    "success": False,
                    "snap": True,
                    "message": random.choice(snap_messages),
                    "location": location,
                    "wrong_bait": player['current_bait']
                }
        
        # Наживка уже учтена при выборе рыбы
        
        # Расчет веса и размера рыбы
        weight = round(random.uniform(caught_fish['min_weight'], caught_fish['max_weight']), 2)
        length = round(random.uniform(caught_fish['min_length'], caught_fish['max_length']), 1)
        logger.info(f"   📏 Fish stats: weight={weight}kg, length={length}cm")

        # --- ГАРПУН: если пойманная рыба < 150кг, fail ---
        if rod and rod['name'] == 'Гарпун' and weight < 150:
            db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
            return {
                "success": False,
                "message": "Гарпун разорвал рыбу на две части 😢",
                "location": location
            }

        # Проверка на превышение веса - рыба срывается если слишком тяжелая
        max_rod_weight = rod.get('max_weight', 999)
        if weight > max_rod_weight:
            db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
            return {
                "success": False,
                "message": f"Рыба {caught_fish['name']} ({weight}кг) слишком тяжелая для вашей удочки и сорвалась!",
                "location": location
            }

        # Успешная ловля - рыба больше не продается автоматически

        # ===== МЕХАНИКА РЫБНАДЗОРА =====
        fish_inspector_chance = 0.01  # 1% шанс
        if random.random() < fish_inspector_chance:
            ban_hours = 1
            ban_until = (datetime.now() + timedelta(hours=ban_hours)).isoformat()
            db.update_player(user_id, chat_id, is_banned=1, ban_until=ban_until)
            return {
                "success": False,
                "fish_inspector": True,
                "message": f"🚨 Вас поймал рыбнадзор! Ваш улов конфискован, а вы арестованы на {ban_hours} час. Можно откупиться за 15 звезд командой /payfine"
            }
        
        # Применяем урон прочности удочки в зависимости от редкости рыбы
        damage = self.get_durability_damage(caught_fish['rarity'], is_guaranteed=False)
        db.reduce_rod_durability(user_id, player['current_rod'], damage, chat_id)
        
        # Проверяем прочность после урона
        player_rod = db.get_player_rod(user_id, player['current_rod'], chat_id)
        current_dur = player_rod.get('current_durability', 0) if player_rod else 0
        max_dur = player_rod.get('max_durability', 100) if player_rod else 100
        rod_broken = current_dur <= 0
        
        db.add_caught_fish(user_id, chat_id, caught_fish['name'], weight, location, length)
        
        # Расход наживки (только при успешной ловле и не за платный заброс!)
        if not guaranteed and player['current_bait'].lower() != 'черви':  # Черви бесконечные
            used = db.use_bait(user_id, player['current_bait'])
            # Если наживка закончилась, переключаем на черви
            if not used or db.get_bait_count(user_id, player['current_bait']) == 0:
                db.update_player_bait(user_id, chat_id, 'Черви')
            logger.info(f"   🪱 Used 1x {player['current_bait']}")

        temp_rod_result = self._consume_temp_rod_use(user_id, chat_id, player['current_rod'])
        
        db.update_player(user_id, chat_id,
                last_fish_time=datetime.now().isoformat())
        
        # Обновление популяции рыбы на локации
        self._update_fish_population(location, -1)
        
        # Determine earned price for the caught fish
        fish_price = caught_fish.get('price', 0)

        return {
            "success": True,
            "fish": caught_fish,
            "weight": weight,
            "length": length,
            "location": location,
            "earned": fish_price,
            "new_balance": player['coins'] + fish_price,
            # This was a normal (non-paid) catch
            "guaranteed": False,
            "stars_spent": 0,
            "rod_broken": rod_broken,
            "current_durability": current_dur,
            "max_durability": max_dur,
            "temp_rod_broken": temp_rod_result.get("broken", False)
        }
    
    def _guaranteed_catch(self, user_id: int, location: str, player: Dict[str, Any], chat_id: int, feeder_bonus: int = 0) -> Dict[str, Any]:
        """Гарантированный улов с фиксированными шансами."""
        roll = random.randint(0, 1000)
        adjusted_roll = max(0, min(1000, roll + (feeder_bonus * 25)))
        logger.info(f"   🎲 Guaranteed roll: {roll}/1000 (adjusted: {adjusted_roll}/1000, feeder {feeder_bonus:+d}%)")

        if adjusted_roll <= 400:
            # Trash branch for guaranteed cast
            logger.info("   📊 Result: TRASH (roll in range 0-400)")
            trash = db.get_random_trash(location)
            if trash:
                logger.info(f"   🗑️ Caught trash: {trash['name']}")
                damage = self.get_durability_damage("trash", is_guaranteed=True)
                db.reduce_rod_durability(user_id, player['current_rod'], damage, chat_id)

                xp_earned = db.calculate_item_xp({
                    'rarity': 'Мусор',
                    'weight': trash.get('weight', 0),
                    'min_weight': 0,
                    'max_weight': 0,
                    'is_trash': True,
                })
                level_info = db.add_player_xp(user_id, chat_id, xp_earned)

                db.update_player(user_id, chat_id,
                                coins=player['coins'] + trash['price'],
                                last_fish_time=datetime.now().isoformat())

                temp_rod_result = self._consume_temp_rod_use(user_id, chat_id, player['current_rod'])

                trash_messages = [
                    f"😑 Ловля... Из воды выловлена {trash['name']}!",
                    f"🗑️ Ловля... Поймали {trash['name']}!",
                    f"😤 Ловля... Это был {trash['name']}, а не рыба!",
                ]

                return {
                    "success": False,
                    "is_trash": True,
                    "trash": trash,
                    "location": location,
                    "message": random.choice(trash_messages),
                    "earned": trash['price'],
                    "new_balance": player['coins'] + trash['price'],
                    "xp_earned": xp_earned,
                    "level_info": level_info,
                    "temp_rod_broken": temp_rod_result.get("broken", False)
                }

        elif adjusted_roll <= 700:
            target_rarity = "Обычная"
        elif adjusted_roll <= 980:
            target_rarity = "Редкая"
        else:
            target_rarity = "Легендарная"

        logger.info(f"   🎯 Rarity: {target_rarity} (roll: {adjusted_roll})")

        # Гарантированный улов: учитывать только сезон, игнорировать наживку.
        fish_list = db.get_fish_by_location(location, self.current_season, min_level=player.get('level', 0))
        fish_list = self._normalize_fish_list(fish_list)
        if not fish_list:
            logger.info(f"   ⚠️ No fish available for location: {location}, season: {self.current_season}")
            db.update_player(user_id, chat_id, last_fish_time=datetime.now().isoformat())
            return {
                "success": False,
                "message": "На этой локации нет рыбы в текущее время года.",
                "location": location
            }

        # Ищем рыбу нужной редкости; если нет — гарантия выдаёт любую рыбу сезона (пользователь всегда получает что-то)
        target_fish = [f for f in fish_list if f['rarity'] == target_rarity]
        if not target_fish:
            logger.info(f"   ⚠️ No fish of rarity {target_rarity} available in season {self.current_season} for location {location} - falling back to any fish this season (guaranteed)")
            target_fish = fish_list

        caught_fish = random.choice(target_fish)

        weight = round(random.uniform(caught_fish['min_weight'], caught_fish['max_weight']), 2)
        length = round(random.uniform(caught_fish['min_length'], caught_fish['max_length']), 1)
        logger.info(f"   📏 Fish stats: weight={weight}kg, length={length}cm")

        # Применяем урон прочности для гарантированного улова
        damage = self.get_durability_damage(caught_fish['rarity'], is_guaranteed=True)
        db.reduce_rod_durability(user_id, player['current_rod'], damage, chat_id)

        # Проверяем прочность после урона
        player_rod = db.get_player_rod(user_id, player['current_rod'], chat_id)
        current_dur = player_rod.get('current_durability', 0) if player_rod else 0
        max_dur = player_rod.get('max_durability', 100) if player_rod else 100
        rod_broken = current_dur <= 0

        db.add_caught_fish(user_id, chat_id, caught_fish['name'], weight, location, length)

        xp_earned = db.calculate_item_xp({
            'rarity': caught_fish.get('rarity', 'Обычная'),
            'weight': weight,
            'min_weight': caught_fish.get('min_weight', 0),
            'max_weight': caught_fish.get('max_weight', 0),
            'is_trash': False,
        })
        level_info = db.add_player_xp(user_id, chat_id, xp_earned)

        fish_price = caught_fish.get('price', 0)
        db.update_player(user_id, chat_id,
                         coins=player['coins'] + fish_price,
                         last_fish_time=datetime.now().isoformat())

        temp_rod_result = self._consume_temp_rod_use(user_id, chat_id, player['current_rod'])

        return {
            "success": True,
            "fish": caught_fish,
            "weight": weight,
            "length": length,
            "location": location,
            "earned": fish_price,
            "new_balance": player['coins'] + fish_price,
            "guaranteed": True,
            "stars_spent": GUARANTEED_CATCH_COST,
            "rod_broken": rod_broken,
            "current_durability": current_dur,
            "max_durability": max_dur,
            "temp_rod_broken": temp_rod_result.get("broken", False)
        }
    
    def _update_fish_population(self, location: str, delta: int):
        """Обновить популяцию рыбы на локации"""
        db.update_location_players(location, delta)

# Глобальный экземпляр игры
game = FishingGame()
