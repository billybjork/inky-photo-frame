import os
import time
import uuid
import psycopg2
from dotenv import load_dotenv
from PIL import Image, ImageOps, ImageDraw, ImageFont
from inky.auto import auto
import RPi.GPIO as GPIO
import boto3
from io import BytesIO
import random
from datetime import datetime, timedelta
import logging

# ------------------------------------------------------------------------------
# 1. Load environment variables and set up logging
# ------------------------------------------------------------------------------

load_dotenv()

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Set up logging to write to a file in the same directory
log_file = os.path.join(script_dir, "fetch_and_display.log")
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Script started and logging initialized.")

# ------------------------------------------------------------------------------
# 2. Environment variable checks
# ------------------------------------------------------------------------------

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Verify all required environment variables are present
required_env_vars = {
    "SUPABASE_DB_URL": SUPABASE_DB_URL,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": AWS_REGION,
    "S3_BUCKET_NAME": S3_BUCKET_NAME
}

missing_vars = [k for k, v in required_env_vars.items() if not v]
if missing_vars:
    msg = f"Missing required environment variables: {', '.join(missing_vars)}"
    logging.critical(msg)
    raise ValueError(msg)

# ------------------------------------------------------------------------------
# 3. Additional configuration
# ------------------------------------------------------------------------------

IMAGE_REPEAT_THRESHOLD = int(os.getenv("IMAGE_REPEAT_THRESHOLD", 10))  # in days
IMAGE_FALLBACK_SEARCH_DAYS = 30  # how many days back we look for fallback images
IMAGE_FALLBACK_LIMIT = 5  # how many images we pick for fallback scenario

# ------------------------------------------------------------------------------
# 4. Inky Impression display setup
# ------------------------------------------------------------------------------

try:
    display = auto()
    display.set_border(display.BLACK)
    DISPLAY_RESOLUTION = (800, 480)  # Resolution of Inky Impression
    logging.info("Inky Impression display initialized successfully.")
except Exception as e:
    logging.critical(f"Failed to initialize Inky display: {e}")
    raise

# ------------------------------------------------------------------------------
# 5. Frame ID logic
# ------------------------------------------------------------------------------

FRAME_ID_FILE = os.path.join(script_dir, "frame_id.txt")

def get_frame_id():
    """
    Retrieve or generate a unique frame ID. This is used to associate display logs
    with a specific Pi/Display device.
    """
    if not os.path.exists(FRAME_ID_FILE):
        frame_id = str(uuid.uuid4())
        with open(FRAME_ID_FILE, "w") as f:
            f.write(frame_id)
        logging.info(f"Generated new frame ID: {frame_id}")
    else:
        with open(FRAME_ID_FILE, "r") as f:
            frame_id = f.read().strip()
        logging.info(f"Loaded existing frame ID: {frame_id}")
    return frame_id

FRAME_ID = get_frame_id()

# ------------------------------------------------------------------------------
# 6. Database connection and query functions
# ------------------------------------------------------------------------------

def get_db_connection():
    """
    Establish a connection to the PostgreSQL database.
    We'll implement a simple retry mechanism to handle transient connection issues.
    """
    retries = 3
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(SUPABASE_DB_URL)
            logging.info("Database connection established successfully.")
            return conn
        except Exception as e:
            logging.error(f"Database connection attempt {attempt} failed: {e}")
            time.sleep(2 ** (attempt - 1))
    logging.critical("Failed to connect to the database after multiple attempts.")
    return None

def query_images_by_month_day(month_day):
    """
    Query all images by the specified month_day in 'MM-DD' format.
    Returns a list of tuples (image_proxy_name, uuid, image_name, image_creation_date).
    """
    conn = get_db_connection()
    if not conn:
        logging.error("No DB connection available. Returning empty list of images.")
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
        logging.info(f"Queried {len(results)} images for date {month_day}.")
        return results
    except Exception as e:
        logging.error(f"Error querying images by date {month_day}: {e}")
        return []
    finally:
        conn.close()

def check_image_displayed_recently(uuid_val, threshold_date):
    """
    Check if the given image (by uuid) has been displayed on or after threshold_date.
    """
    conn = get_db_connection()
    if not conn:
        logging.error("No DB connection available for checking recently displayed image.")
        return False

    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM display_logs 
            WHERE uuid = %s AND display_date >= %s AND frame_id = %s
        """, (uuid_val, threshold_date, FRAME_ID))
        count = cursor.fetchone()[0]
        return count > 0
    except Exception as e:
        logging.error(f"Error checking display logs for {uuid_val}: {e}")
        return False
    finally:
        conn.close()

def log_image_displayed(uuid_val, display_date):
    """
    Log that an image was displayed on display_date, avoiding duplicates for the same day.
    """
    conn = get_db_connection()
    if not conn:
        logging.error("No DB connection available for logging image display.")
        return

    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) 
            FROM display_logs 
            WHERE uuid = %s AND display_date = %s AND frame_id = %s
        """, (uuid_val, display_date, FRAME_ID))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                INSERT INTO display_logs (uuid, display_date, frame_id)
                VALUES (%s, %s, %s)
            """, (uuid_val, display_date, FRAME_ID))
            conn.commit()
            logging.info(f"Logged display of image {uuid_val} on {display_date}.")
        else:
            logging.info(f"Image {uuid_val} was already logged for {display_date}.")
    except Exception as e:
        logging.error(f"Error logging display event for {uuid_val}: {e}")
    finally:
        conn.close()

# ------------------------------------------------------------------------------
# 7. Fallback/eligible images logic
# ------------------------------------------------------------------------------

def find_eligible_images_for_date(
    month_day,
    repeat_threshold_days=IMAGE_REPEAT_THRESHOLD,
    limit=IMAGE_FALLBACK_LIMIT
):
    """
    For a given month_day (MM-DD), find up to 'limit' images that haven't been
    displayed recently. Checks images in descending order by image_creation_date.
    Returns a list of images (image_proxy_name, uuid, image_name, image_creation_date).
    """
    images = query_images_by_month_day(month_day)
    if not images:
        logging.info(f"No images found for {month_day}.")
        return []

    threshold_date = (datetime.now() - timedelta(days=repeat_threshold_days)).date()
    eligible = []
    for img in images:
        _, uuid_val, _, _ = img
        if not check_image_displayed_recently(uuid_val, threshold_date):
            eligible.append(img)
        if len(eligible) >= limit:
            break
    logging.info(f"Found {len(eligible)} eligible image(s) for {month_day}.")
    return eligible

def find_images_for_today_and_fallback():
    """
    Attempt to find images for today's date (by month-day).
      - If found, return all images for that date (we do NOT limit today's images).
      - If no images for today, fallback to previous days (up to IMAGE_FALLBACK_SEARCH_DAYS),
        picking images not displayed recently, up to IMAGE_FALLBACK_LIMIT.
    Returns (list_of_images, fallback_used_bool).
    """
    today = datetime.now()
    today_month_day = today.strftime('%m-%d')

    # First try today's date
    today_images = query_images_by_month_day(today_month_day)
    if today_images:
        logging.info(f"Found {len(today_images)} image(s) for today's date: {today_month_day}.")
        # If you want to filter out repeats for today's images, uncomment below lines:
        # threshold_date = (datetime.now() - timedelta(days=IMAGE_REPEAT_THRESHOLD)).date()
        # filtered_today_images = [img for img in today_images if not check_image_displayed_recently(img[1], threshold_date)]
        # if filtered_today_images:
        #     logging.info("Using filtered images for today.")
        #     return filtered_today_images, False
        # else:
        #     logging.info("No non-repeated images left for today, going into fallback mode.")
        return today_images, False

    logging.info(f"No images for today's date ({today_month_day}). Checking fallback dates...")
    # Fallback scenario: look back up to IMAGE_FALLBACK_SEARCH_DAYS
    for i in range(1, IMAGE_FALLBACK_SEARCH_DAYS + 1):
        fallback_date = (today - timedelta(days=i))
        fallback_md = fallback_date.strftime('%m-%d')
        fallback_images = find_eligible_images_for_date(
            fallback_md,
            IMAGE_REPEAT_THRESHOLD,
            IMAGE_FALLBACK_LIMIT
        )
        if fallback_images:
            # Shuffle so we don't always pick the same ones first
            random.shuffle(fallback_images)
            logging.info(f"Using fallback images from {fallback_md}.")
            return fallback_images, True

    logging.warning("No fallback images found within the specified date range.")
    return [], False

# ------------------------------------------------------------------------------
# 8. S3 interaction
# ------------------------------------------------------------------------------

def fetch_image_from_s3(s3_key):
    """
    Download the image from S3 using the provided object key, with retries
    in case of transient network or AWS issues.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    retries = 3
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Fetching image with key: {s3_key} (attempt {attempt})")
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            image_data = response['Body'].read()
            return Image.open(BytesIO(image_data))
        except Exception as e:
            logging.error(f"Error fetching image from S3 (attempt {attempt}): {e}")
            time.sleep(2 ** (attempt - 1))

    logging.error(f"Failed to fetch image {s3_key} after {retries} attempts.")
    return None

# ------------------------------------------------------------------------------
# 9. Image processing (resizing, overlay text, etc.)
# ------------------------------------------------------------------------------

def get_average_color(image):
    """
    Compute the average (R, G, B) color of the provided image.
    """
    image = image.convert("RGB")
    pixels = list(image.getdata())
    r = sum(p[0] for p in pixels) / len(pixels)
    g = sum(p[1] for p in pixels) / len(pixels)
    b = sum(p[2] for p in pixels) / len(pixels)
    return (int(r), int(g), int(b))

def resize_image(image, target_resolution):
    """
    Resize the image to fit within target_resolution, centering it on a canvas
    and using average color boxes for any borders needed.
    """
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
        bottom_slice = image_copy.crop((0, image_copy.height - bottom_slice_height,
                                        image_copy.width, image_copy.height))
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
        right_slice = image_copy.crop((image_copy.width - right_slice_width, 0,
                                       image_copy.width, image_copy.height))
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
    """
    Convert a datetime object to a string like "January 1st, 2023",
    handling 1st, 2nd, 3rd, etc. appropriately.
    """
    day = date_obj.day
    if 11 <= day % 100 <= 13:
        suffix = 'th'
    else:
        suffix_map = {1: 'st', 2: 'nd', 3: 'rd'}
        suffix = suffix_map.get(day % 10, 'th')
    return f"{date_obj.strftime('%B')} {day}{suffix}, {date_obj.year}"

def choose_text_color_for_background(image, box):
    """
    Analyze the brightness of the region in 'box' to decide whether
    text should be black or white.
    """
    region = image.crop(box)
    gray = region.convert("L")
    hist = gray.histogram()
    total_pixels = sum(hist)
    if total_pixels == 0:
        return "white"
    brightness = sum(i * hist[i] for i in range(256)) / total_pixels
    return "black" if brightness > 128 else "white"

def overlay_date_text(image, date_obj, x_offset, y_offset, img_width, img_height, fallback_used=False):
    """
    Draw the date text (and "X years ago...") on the provided image, accounting
    for fallback scenario vs. actual date scenario.
    """
    draw = ImageDraw.Draw(image)
    font_path = os.path.join(script_dir, "EBGaramond12-Regular.otf")

    if not os.path.exists(font_path):
        logging.warning(f"Font file not found at {font_path}. Text may not render properly.")

    month_day_font_size = 60
    years_ago_font_size = 40

    # Attempt to load fonts; fallback if missing
    try:
        month_day_font = ImageFont.truetype(font_path, month_day_font_size)
        years_ago_font = ImageFont.truetype(font_path, years_ago_font_size)
    except Exception as e:
        logging.error(f"Error loading font: {e}. Using default PIL font.")
        month_day_font = ImageFont.load_default()
        years_ago_font = ImageFont.load_default()

    if fallback_used:
        # Use today's date with an asterisk
        today = datetime.now()
        formatted_date = format_date_ordinal(today)
        formatted_date = f"*{formatted_date}"
        current_year = today.year
        years_ago = current_year - date_obj.year
        years_ago_text = f"{years_ago} years ago..." if years_ago > 1 else "Last year..."
    else:
        # Use image's original creation date
        formatted_date = format_date_ordinal(date_obj)
        current_year = datetime.now().year
        years_ago = current_year - date_obj.year
        years_ago_text = f"{years_ago} years ago..." if years_ago > 1 else "Last year..."

    # Extract just the month/day portion
    if ", " in formatted_date:
        month_day_text, _ = formatted_date.rsplit(", ", 1)
    else:
        month_day_text = formatted_date

    margin = 10

    # Calculate text bounding boxes
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

    # Determine appropriate text colors
    month_day_box = (
        month_day_x_pos, month_day_y_pos,
        month_day_x_pos + month_day_width,
        month_day_y_pos + month_day_height
    )
    years_ago_box = (
        years_ago_x_pos, years_ago_y_pos,
        years_ago_x_pos + years_ago_width,
        years_ago_y_pos + years_ago_height
    )
    month_day_color = choose_text_color_for_background(image, month_day_box)
    years_ago_color = choose_text_color_for_background(image, years_ago_box)

    # Draw text
    draw.text((month_day_x_pos, month_day_y_pos), month_day_text,
              fill=month_day_color, font=month_day_font)
    draw.text((years_ago_x_pos, years_ago_y_pos), years_ago_text,
              fill=years_ago_color, font=years_ago_font)

    return image

def display_image(image, image_date, uuid_val, fallback_used=False):
    """
    Resize the image, add date overlay, and display it on the Inky Impression.
    Then log that this image was displayed.
    """
    try:
        resized_image, x_offset, y_offset, resized_w, resized_h = resize_image(image, DISPLAY_RESOLUTION)
        if image_date:
            resized_image = overlay_date_text(
                resized_image, image_date,
                x_offset, y_offset,
                resized_w, resized_h,
                fallback_used=fallback_used
            )
        display.set_image(resized_image)
        display.show()
        logging.info(f"Displayed image UUID: {uuid_val}")
        log_image_displayed(uuid_val, datetime.now().date())
    except Exception as e:
        logging.error(f"Error displaying image {uuid_val}: {e}")

# ------------------------------------------------------------------------------
# 10. GPIO setup and main loop
# ------------------------------------------------------------------------------

def setup_button(pin=5):
    """
    Setup the GPIO pin for the "A" button input.
    (This is duplicated above for clarity; we keep this final version here.)
    """
    try:
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(pin, GPIO.IN, pull_up_down=GPIO.PUD_UP)
        logging.info(f"GPIO button configured on pin {pin}.")
        return pin
    except Exception as e:
        logging.critical(f"Failed to configure GPIO button: {e}")
        raise

if __name__ == "__main__":
    logging.info("Starting image rotation process with fallback logic.")
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
                logging.info("Date has changed. Fetching new images.")
                images_to_cycle, fallback_used = find_images_for_today_and_fallback()
                current_date_str = new_date_str
                index = 0

            if not images_to_cycle:
                print("No images found (even after fallback). Retrying in 30 minutes...")
                logging.warning("No images found. Will retry in 30 minutes.")
                # Wait 30 minutes, checking for button presses
                wait_seconds = 1800
                for i in range(wait_seconds):
                    if GPIO.input(button_pin) == GPIO.LOW:
                        print("Button pressed! Attempting to refetch images now...")
                        logging.info("Button pressed during wait. Refetching images.")
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
                logging.warning(f"Failed to fetch image with key {s3_key}. Skipping...")

            # Move to the next image
            index = (index + 1) % len(images_to_cycle)

            print("Waiting 30 minutes before the next image...")
            logging.info("Waiting 30 minutes before displaying the next image.")
            # Poll the button every second
            wait_seconds = 1800  # 30 minutes
            button_pressed = False
            for i in range(wait_seconds):
                if GPIO.input(button_pin) == GPIO.LOW:
                    print("Button pressed! Manually shuffling images...")
                    logging.info("Button pressed! Manually shuffling images.")
                    random.shuffle(images_to_cycle)
                    index = 0
                    button_pressed = True
                    break
                time.sleep(1)

            if button_pressed:
                continue

    except KeyboardInterrupt:
        print("Exiting")
        logging.info("Script interrupted by user (KeyboardInterrupt). Exiting.")
    finally:
        GPIO.cleanup()
        logging.info("GPIO cleaned up. Script terminated.")