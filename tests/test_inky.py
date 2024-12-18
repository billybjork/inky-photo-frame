from inky.inky_uc8159 import Inky

try:
    inky_display = Inky()
    print("Display initialized successfully.")
except Exception as e:
    print(f"Failed to initialize the display: {e}")
