import RPi.GPIO as GPIO
import time

GPIO.setmode(GPIO.BCM)
GPIO.setup(5, GPIO.IN, pull_up_down=GPIO.PUD_UP)

try:
    print("Waiting for button press...")
    while True:
        if GPIO.input(5) == GPIO.LOW:
            print("Button pressed!")
        time.sleep(0.1)
except KeyboardInterrupt:
    print("Exiting")
finally:
    GPIO.cleanup()
