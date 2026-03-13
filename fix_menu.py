#!/usr/bin/env python
# -*- coding: utf-8 -*-

with open('bot.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find menu_text line (around line 1951)
for i in range(len(lines)):
    if 'menu_text = f' in lines[i] and i > 1900:
        # Found it
        # Skip to end of f-string (ending with """)
        j = i + 1  
        while j < len(lines) and not ('"""' in lines[j] and lines[j].strip() == '"""'):
            j += 1
        
        # Replace lines i through j  
        new_lines = [
            '        menu_text = f"""\n',
            '{rod_emoji} Меню рыбалки\n',
            '\n',
            '{coin_emoji} Монеты: {html.escape(str(player["coins"]))} {html.escape(COIN_NAME)}\n',
            '{diamond_emoji} Бриллианты: {html.escape(str(diamond_count))}\n',
            '{rod_emoji} Удочка: {html.escape(str(player["current_rod"]))}\n',
            '{location_emoji} Локация: {html.escape(str(player["current_location"]))}\n',
            '{bait_emoji} Наживка: {html.escape(str(player["current_bait"]))}\n',
            '{durability_line}\n',
            '        """\n',
        ]
        
        lines[i:j+1] = new_lines
        print(f'Replaced lines {i} to {j}')
        break

with open('bot.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print('Menu template fixed with proper emoji usage and HTML escaping')
