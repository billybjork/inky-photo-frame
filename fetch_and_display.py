import os
import time
import psycopg2
from dotenv import load_dotenv
from PIL import Image, ImageOps, ImageDraw, ImageFont
from inky.auto import auto
import RPi.GPIO as GPIO
import boto3
from io import BytesIO
import random
from datetime import datetime, timedelta

load_dotenv()

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

IMAGE_REPEAT_THRESHOLD = int(os.getenv("IMAGE_REPEAT_THRESHOLD", 10))  # in days
IMAGE_FALLBACK_SEARCH_DAYS = 30  # how many days back we look for fallback images
IMAGE_FALLBACK_LIMIT = 5  # how many images we pick for fallback scenario

# Initialize Inky Impression display
display = auto()
display.set_border(display.BLACK)
DISPLAY_RESOLUTION = (800, 480)  # Resolution of Inky Impression

def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(SUPABASE_DB_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def setup_button(pin=5):
    """
    Setup the GPIO pin for the "A" button input.
    """
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
    return pin

def query_images_by_month_day(month_day):
    """
    Query all images by the specified month_day in 'MM-DD' format.
    Returns a list of tuples (image_proxy_name, uuid, image_name, image_creation_date).
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor()
    try:
        query = """
        SELECT image_proxy_name, uuid, image_name, image_creation_date
        FROM assets
        WHERE TO_CHAR(image_creation_date, 'MM-DD') = %s
          AND image_proxy_name IS NOT NULL
        ORDER BY image_creation_date DESC;
        """
        cursor.execute(query, (month_day,))
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error querying images by date {month_day}: {e}")
        return []
    finally:
        conn.close()

def check_image_displayed_recently(uuid_val, threshold_date):
    """
    Check if the given image (by uuid) has been displayed on or after threshold_date.
    """
    conn = get_db_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM display_logs WHERE uuid = %s AND display_date >= %s",
                       (uuid_val, threshold_date))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        print(f"Error checking display logs: {e}")
        return False
    finally:
        conn.close()

def log_image_displayed(uuid_val, display_date):
    """
    Log that an image was displayed on display_date. Avoid duplicates.
    """
    conn = get_db_connection()
    if not conn:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM display_logs WHERE uuid = %s AND display_date = %s", (uuid_val, display_date))
        if cursor.fetchone()[0] == 0:
            cursor.execute("INSERT INTO display_logs (uuid, display_date) VALUES (%s, %s)",
                           (uuid_val, display_date))
            conn.commit()
    except Exception as e:
        print(f"Error logging display event: {e}")
    finally:
        conn.close()

def find_eligible_images_for_date(month_day, repeat_threshold_days=IMAGE_REPEAT_THRESHOLD, limit=IMAGE_FALLBACK_LIMIT):
    """
    For a given month_day (MM-DD), find up to 'limit' images that haven't been displayed recently.
    Checks images in descending order by image_creation_date.
    Returns a list of images (image_proxy_name, uuid, image_name, image_creation_date).
    """
    images = query_images_by_month_day(month_day)
    if not images:
        return []

    # Filter out images displayed recently
    threshold_date = (datetime.now() - timedelta(days=repeat_threshold_days)).date()
    eligible = []
    for img in images:
        _, uuid_val, _, _ = img
        if not check_image_displayed_recently(uuid_val, threshold_date):
            eligible.append(img)
        if len(eligible) >= limit:
            break
    return eligible

def find_images_for_today_and_fallback():
    """
    Attempt to find images for today's date (by month-day).
    If found, return all images for that date (no limit, since we cycle through them).
    If not found, fallback to previous days for up to 30 days, picking images not displayed recently.
    Return a list of images and a boolean indicating whether fallback was used.
    """
    today = datetime.now()
    today_month_day = today.strftime('%m-%d')

    # First try today's date - no limit since if we have today's images, we cycle through all of them.
    today_images = query_images_by_month_day(today_month_day)
    if today_images:
        # For today's images, we do NOT filter by repeat threshold, since you requested to cycle through all today's images.
        # If you do want to filter out repeats even for today's images, uncomment below lines:
        # threshold_date = (datetime.now() - timedelta(days=IMAGE_REPEAT_THRESHOLD)).date()
        # today_images = [img for img in today_images if not check_image_displayed_recently(img[1], threshold_date)]
        # If no images remain after filtering, then fallback logic would apply. 
        return today_images, False

    # No images for today, fallback scenario:
    # We'll look back up to IMAGE_FALLBACK_SEARCH_DAYS days and try to find up to IMAGE_FALLBACK_LIMIT eligible images.
    for i in range(1, IMAGE_FALLBACK_SEARCH_DAYS + 1):
        fallback_date = (today - timedelta(days=i))
        fallback_md = fallback_date.strftime('%m-%d')
        fallback_images = find_eligible_images_for_date(fallback_md, IMAGE_REPEAT_THRESHOLD, IMAGE_FALLBACK_LIMIT)
        if fallback_images:
            # Shuffle them so we don't always show the same images from that day first
            random.shuffle(fallback_images)
            return fallback_images, True

    return [], False

def fetch_image_from_s3(s3_key):
    """Download the image from S3 using the object key."""
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    try:
        print(f"Fetching image with key: {s3_key}")
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        image_data = response['Body'].read()
        return Image.open(BytesIO(image_data))
    except Exception as e:
        print(f"Error fetching image from S3: {e}")
        return None

def get_average_color(image):
    image = image.convert("RGB")
    pixels = list(image.getdata())
    r = sum(p[0] for p in pixels) / len(pixels)
    g = sum(p[1] for p in pixels) / len(pixels)
    b = sum(p[2] for p in pixels) / len(pixels)
    return (int(r), int(g), int(b))

def resize_image(image, target_resolution):
    canvas = Image.new("RGB", target_resolution, (0, 0, 0))
    image_copy = image.copy()
    image_copy.thumbnail(target_resolution, Image.LANCZOS)
    x_offset = (target_resolution[0] - image_copy.width) // 2
    y_offset = (target_resolution[1] - image_copy.height) // 2
    canvas.paste(image_copy, (x_offset, y_offset))

    need_top_bottom_box = image_copy.height < target_resolution[1]
    need_left_right_box = image_copy.width < target_resolution[0]

    if need_top_bottom_box:
        top_slice_height = min(10, image_copy.height)
        top_slice = image_copy.crop((0, 0, image_copy.width, top_slice_height))
        top_color = get_average_color(top_slice)
        bottom_slice_height = min(10, image_copy.height)
        bottom_slice = image_copy.crop((0, image_copy.height - bottom_slice_height, image_copy.width, image_copy.height))
        bottom_color = get_average_color(bottom_slice)

        if y_offset > 0:
            top_box = (0, 0, target_resolution[0], y_offset)
            ImageDraw.Draw(canvas).rectangle(top_box, fill=top_color)
        bottom_start = y_offset + image_copy.height
        if bottom_start < target_resolution[1]:
            bottom_box = (0, bottom_start, target_resolution[0], target_resolution[1])
            ImageDraw.Draw(canvas).rectangle(bottom_box, fill=bottom_color)

    if need_left_right_box:
        left_slice_width = min(10, image_copy.width)
        left_slice = image_copy.crop((0, 0, left_slice_width, image_copy.height))
        left_color = get_average_color(left_slice)

        right_slice_width = min(10, image_copy.width)
        right_slice = image_copy.crop((image_copy.width - right_slice_width, 0, image_copy.width, image_copy.height))
        right_color = get_average_color(right_slice)

        if x_offset > 0:
            left_box = (0, 0, x_offset, target_resolution[1])
            ImageDraw.Draw(canvas).rectangle(left_box, fill=left_color)
        right_start = x_offset + image_copy.width
        if right_start < target_resolution[0]:
            right_box = (right_start, 0, target_resolution[0], target_resolution[1])
            ImageDraw.Draw(canvas).rectangle(right_box, fill=right_color)

    return canvas, x_offset, y_offset, image_copy.width, image_copy.height

def format_date_ordinal(date_obj):
    day = date_obj.day
    if 11 <= day % 100 <= 13:
        suffix = 'th'
    else:
        suffix_map = {1: 'st', 2: 'nd', 3: 'rd'}
        suffix = suffix_map.get(day % 10, 'th')
    return f"{date_obj.strftime('%B')} {day}{suffix}, {date_obj.year}"

def choose_text_color_for_background(image, box):
    region = image.crop(box)
    gray = region.convert("L")
    hist = gray.histogram()
    total_pixels = sum(hist)
    brightness = sum(i * hist[i] for i in range(256)) / total_pixels
    return "black" if brightness > 128 else "white"

def overlay_date_text(image, date_obj, x_offset, y_offset, img_width, img_height, fallback_used=False):
    draw = ImageDraw.Draw(image)
    font_path = os.path.join(os.path.dirname(__file__), "EBGaramond12-Regular.otf")

    month_day_font_size = 60
    years_ago_font_size = 40

    month_day_font = ImageFont.truetype(font_path, month_day_font_size)
    years_ago_font = ImageFont.truetype(font_path, years_ago_font_size)

    if fallback_used:
        # Use today's date and prepend an asterisk
        today = datetime.now()
        formatted_date = format_date_ordinal(today)
        formatted_date = f"*{formatted_date}"  # Add asterisk to indicate fallback
        current_year = datetime.now().year
        years_ago = current_year - date_obj.year
        years_ago_text = f"{years_ago} years ago..." if years_ago > 1 else "Last year..."
    else:
        # Use the image's original creation date
        formatted_date = format_date_ordinal(date_obj)
        current_year = datetime.now().year
        years_ago = current_year - date_obj.year
        years_ago_text = f"{years_ago} years ago..." if years_ago > 1 else "Last year..."

    # Extract just the month/day portion (no year)
    month_day_text, _ = formatted_date.rsplit(", ", 1)

    image_width, image_height = image.size
    margin = 10

    month_day_bbox = month_day_font.getbbox(month_day_text)
    month_day_width = month_day_bbox[2] - month_day_bbox[0]
    month_day_height = month_day_bbox[3] - month_day_bbox[1]
    month_day_x_pos = x_offset + img_width - month_day_width - margin
    month_day_y_pos = y_offset + img_height - month_day_height - (margin + 10)

    years_ago_bbox = years_ago_font.getbbox(years_ago_text)
    years_ago_width = years_ago_bbox[2] - years_ago_bbox[0]
    years_ago_height = years_ago_bbox[3] - years_ago_bbox[1]
    years_ago_x_pos = x_offset + margin
    years_ago_y_pos = y_offset + margin

    month_day_box = (month_day_x_pos, month_day_y_pos, month_day_x_pos + month_day_width, month_day_y_pos + month_day_height)
    month_day_color = choose_text_color_for_background(image, month_day_box)

    years_ago_box = (years_ago_x_pos, years_ago_y_pos, years_ago_x_pos + years_ago_width, years_ago_y_pos + years_ago_height)
    years_ago_color = choose_text_color_for_background(image, years_ago_box)

    draw.text((month_day_x_pos, month_day_y_pos), month_day_text, fill=month_day_color, font=month_day_font)
    draw.text((years_ago_x_pos, years_ago_y_pos), years_ago_text, fill=years_ago_color, font=years_ago_font)

    return image

def display_image(image, image_date, uuid_val, fallback_used=False):
    """Resize the image, add date overlay, and display it on the Inky Impression, then log it."""
    try:
        resized_image, x_offset, y_offset, resized_w, resized_h = resize_image(image, DISPLAY_RESOLUTION)
        if image_date:
            resized_image = overlay_date_text(resized_image, image_date, x_offset, y_offset, resized_w, resized_h, fallback_used=fallback_used)
        display.set_image(resized_image)
        display.show()
        print("Image displayed successfully!")
        # Log the display event
        log_image_displayed(uuid_val, datetime.now().date())
    except Exception as e:
        print(f"Error displaying image: {e}")

if __name__ == "__main__":
    print("Starting image rotation process with no recent repeats in fallback mode...")

    # Setup the button for manual shuffle
    button_pin = setup_button()

    # Initial setup for the day and images
    current_date_str = datetime.now().strftime('%Y-%m-%d')
    images_to_cycle, fallback_used = find_images_for_today_and_fallback()
    index = 0

    try:
        while True:
            new_date_str = datetime.now().strftime('%Y-%m-%d')
            if new_date_str != current_date_str:
                print("Date has changed. Fetching new images for the new day...")
                images_to_cycle, fallback_used = find_images_for_today_and_fallback()
                current_date_str = new_date_str
                index = 0

            if not images_to_cycle:
                print("No images found (even after fallback). Retrying in 30 minutes...")
                # Wait 30 minutes, checking for button presses
                wait_seconds = 1800
                for i in range(wait_seconds):
                    if GPIO.input(button_pin) == GPIO.LOW:
                        print("Button pressed! Attempting to refetch images now...")
                        images_to_cycle, fallback_used = find_images_for_today_and_fallback()
                        index = 0
                        break
                    time.sleep(1)
                continue

            # Display the current image
            image_proxy_name, uuid_val, image_name, image_creation_date = images_to_cycle[index]
            s3_key = image_proxy_name
            image = fetch_image_from_s3(s3_key)
            if image:
                display_image(image, image_creation_date, uuid_val, fallback_used=fallback_used)
            else:
                print("Failed to fetch image. Will try the next one.")

            # Move to the next image
            index = (index + 1) % len(images_to_cycle)

            print("Waiting 30 minutes before the next image...")
            # Poll the button every second
            wait_seconds = 1800  # 30 minutes
            button_pressed = False
            for i in range(wait_seconds):
                if GPIO.input(button_pin) == GPIO.LOW:
                    print("Button pressed! Manually shuffling images...")
                    random.shuffle(images_to_cycle)
                    index = 0
                    button_pressed = True
                    break
                time.sleep(1)

            if button_pressed:
                continue

    except KeyboardInterrupt:
        print("Exiting")
    finally:
        GPIO.cleanup()