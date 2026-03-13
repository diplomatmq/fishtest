#!/usr/bin/env python
# Quick syntax check
try:
    import ast
    with open('bot.py', 'r', encoding='utf-8') as f:
        code = f.read()
    ast.parse(code)
    print("✅ Syntax verification passed - bot.py is valid Python")
except SyntaxError as e:
    print(f"❌ Syntax error: {e}")
    exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)
