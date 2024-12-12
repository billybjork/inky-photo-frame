import os
import time
import psycopg2
from dotenv import load_dotenv
from PIL import Image, ImageOps, ImageDraw, ImageFont
from inky.auto import auto
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

def query_images_by_month_day(month_day, limit=None):
    """
    Query images by the specified month_day in 'MM-DD' format.
    If limit is provided, limit the number of results returned.
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
        if limit is not None:
            results = results[:limit]
        return results
    except Exception as e:
        print(f"Error querying images by date {month_day}: {e}")
        return []
    finally:
        conn.close()

def find_images_for_today_and_fallback():
    """
    Attempt to find images for today's date (by month-day).
    If none found, fallback to previous days until images are found.
    If found images for today's date, return all of them in their given order.
    If fallback, return only the first 5 images found from the first day that has images.
    """
    today = datetime.now()
    today_month_day = today.strftime('%m-%d')

    # Try today's date first
    images = query_images_by_month_day(today_month_day)
    if images:
        return images, False  # Not fallback

    # No images for today's date, fallback to previous days
    for i in range(1, 31):
        fallback_date = today - timedelta(days=i)
        fallback_md = fallback_date.strftime('%m-%d')
        fallback_images = query_images_by_month_day(fallback_md)
        if fallback_images:
            # Take the first 5 images
            fallback_images = fallback_images[:5]
            # Shuffle them once
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

def resize_image(image, target_resolution):
    """
    Resize the image to fit the target resolution while maintaining aspect ratio.
    Returns the resized canvas, and also the x_offset, y_offset, and the resized image dimensions.
    """
    # Create a blank canvas with the target resolution and black background
    canvas = Image.new("RGB", target_resolution, (0, 0, 0))

    # Resize the image while maintaining aspect ratio
    image_copy = image.copy()
    image_copy.thumbnail(target_resolution, Image.LANCZOS)

    # Calculate position to center the image on the canvas
    x_offset = (target_resolution[0] - image_copy.width) // 2
    y_offset = (target_resolution[1] - image_copy.height) // 2

    # Paste the resized image onto the canvas
    canvas.paste(image_copy, (x_offset, y_offset))

    return canvas, x_offset, y_offset, image_copy.width, image_copy.height

def format_date_ordinal(date_obj):
    """Format the date with an ordinal indicator (e.g., 'August 5th, 1997')."""
    day = date_obj.day
    # Special cases
    if 11 <= day % 100 <= 13:
        suffix = 'th'
    else:
        suffix_map = {1: 'st', 2: 'nd', 3: 'rd'}
        suffix = suffix_map.get(day % 10, 'th')
    return f"{date_obj.strftime('%B')} {day}{suffix}, {date_obj.year}"

def choose_text_color_for_background(image, box):
    """
    Given an image and a box (tuple: (x1, y1, x2, y2)),
    determine average brightness in that region and return 'black' or 'white'.
    """
    # Crop the region where text will be placed
    region = image.crop(box)
    # Convert to grayscale to measure brightness
    gray = region.convert("L")
    hist = gray.histogram()
    # Compute weighted sum of pixel intensities
    # pixel value * count
    # Then divide by total pixels to get average brightness
    total_pixels = sum(hist)
    brightness = sum(i * hist[i] for i in range(256)) / total_pixels

    # If the region is bright (above a threshold), use black text; otherwise white text
    return "black" if brightness > 128 else "white"

def overlay_date_text(image, date_obj, x_offset, y_offset, img_width, img_height):
    """Overlay the formatted date text in the bottom-right corner and year in the top-left corner."""
    draw = ImageDraw.Draw(image)

    font_path = os.path.join(os.path.dirname(__file__), "DejaVuSerif.ttf")

    # Fonts for month/day and year
    month_day_font_size = 72  # Larger font for month/day
    year_font_size = 48  # Moderately larger font for year

    month_day_font = ImageFont.truetype(font_path, month_day_font_size)
    year_font = ImageFont.truetype(font_path, year_font_size)

    # Format the date text
    formatted_date = format_date_ordinal(date_obj)
    month_day_text, year_text = formatted_date.rsplit(", ", 1)

    # Get image dimensions
    image_width, image_height = image.size
    margin = 10

    # Position for month/day (bottom-right)
    month_day_bbox = month_day_font.getbbox(month_day_text)
    month_day_width = month_day_bbox[2] - month_day_bbox[0]
    month_day_height = month_day_bbox[3] - month_day_bbox[1]
    month_day_x_pos = x_offset + img_width - month_day_width - margin
    month_day_y_pos = y_offset + img_height - month_day_height - margin

    # Position for year (top-left)
    year_bbox = year_font.getbbox(year_text)
    year_width = year_bbox[2] - year_bbox[0]
    year_height = year_bbox[3] - year_bbox[1]
    year_x_pos = x_offset + margin
    year_y_pos = y_offset + margin

    # Determine text colors based on background brightness
    month_day_box = (month_day_x_pos, month_day_y_pos, month_day_x_pos + month_day_width, month_day_y_pos + month_day_height)
    month_day_color = choose_text_color_for_background(image, month_day_box)

    year_box = (year_x_pos, year_y_pos, year_x_pos + year_width, year_y_pos + year_height)
    year_color = choose_text_color_for_background(image, year_box)

    # Draw the texts on the image
    draw.text((month_day_x_pos, month_day_y_pos), month_day_text, fill=month_day_color, font=month_day_font)
    draw.text((year_x_pos, year_y_pos), year_text, fill=year_color, font=year_font)

    return image

def display_image(image, image_date):
    """Resize the image, add date overlay, and display it on the Inky Impression."""
    try:
        resized_image, x_offset, y_offset, resized_w, resized_h = resize_image(image, DISPLAY_RESOLUTION)
        if image_date:
            resized_image = overlay_date_text(resized_image, image_date, x_offset, y_offset, resized_w, resized_h)
        display.set_image(resized_image)
        display.show()
        print("Image displayed successfully!")
    except Exception as e:
        print(f"Error displaying image: {e}")

if __name__ == "__main__":
    print("Starting image rotation process...")

    current_date_str = datetime.now().strftime('%Y-%m-%d')
    images_to_cycle, fallback_used = find_images_for_today_and_fallback()

    index = 0

    while True:
        new_date_str = datetime.now().strftime('%Y-%m-%d')
        if new_date_str != current_date_str:
            print("Date has changed. Fetching new images for the new day...")
            images_to_cycle, fallback_used = find_images_for_today_and_fallback()
            current_date_str = new_date_str
            index = 0

        if not images_to_cycle:
            print("No images found (even after fallback). Retrying in 30 minutes...")
            time.sleep(1800)
            images_to_cycle, fallback_used = find_images_for_today_and_fallback()
            continue

        image_proxy_name, uuid_val, image_name, image_creation_date = images_to_cycle[index]
        s3_key = image_proxy_name
        image = fetch_image_from_s3(s3_key)
        if image:
            display_image(image, image_creation_date)
        else:
            print("Failed to fetch image. Will try the next one.")

        index = (index + 1) % len(images_to_cycle)
        print("Waiting 30 minutes before the next image...")
        time.sleep(1800)