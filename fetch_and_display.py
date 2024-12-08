import os
import time
import psycopg2
from dotenv import load_dotenv
from PIL import Image, ImageOps
from inky.auto import auto
import boto3
from io import BytesIO
import random
from datetime import datetime, timedelta

load_dotenv()

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
        # Found images for today's date; return all of them as-is
        # No shuffle, just cycle in the order returned
        return images, False  # False indicates not fallback

    # No images for today's date, fallback to previous days
    # We'll go back up to 30 days to find some images
    for i in range(1, 31):
        fallback_date = today - timedelta(days=i)
        fallback_md = fallback_date.strftime('%m-%d')
        fallback_images = query_images_by_month_day(fallback_md)
        if fallback_images:
            # Take the first 5 images
            fallback_images = fallback_images[:5]
            # Shuffle them once
            random.shuffle(fallback_images)
            return fallback_images, True  # True indicates fallback used

    # No images found even after fallback
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
    """Resize the image to fit the target resolution while maintaining aspect ratio."""
    # Create a blank canvas with the target resolution and white background
    canvas = Image.new("RGB", target_resolution, (255, 255, 255))

    # Resize the image while maintaining aspect ratio
    image.thumbnail(target_resolution, Image.LANCZOS)

    # Calculate position to center the image on the canvas
    x_offset = (target_resolution[0] - image.width) // 2
    y_offset = (target_resolution[1] - image.height) // 2

    # Paste the resized image onto the canvas
    canvas.paste(image, (x_offset, y_offset))
    return canvas

def display_image(image):
    """Resize the image and display it on the Inky Impression."""
    try:
        resized_image = resize_image(image, DISPLAY_RESOLUTION)
        display.set_image(resized_image)
        display.show()
        print("Image displayed successfully!")
    except Exception as e:
        print(f"Error displaying image: {e}")

if __name__ == "__main__":
    print("Starting image rotation process...")

    current_date_str = datetime.now().strftime('%Y-%m-%d')
    images_to_cycle, fallback_used = find_images_for_today_and_fallback()

    # images_to_cycle is a list of tuples: (image_proxy_name, uuid, image_name, image_creation_date)
    # We'll just cycle through them in order.
    index = 0

    while True:
        # Check if the date changed (e.g., after midnight)
        new_date_str = datetime.now().strftime('%Y-%m-%d')
        if new_date_str != current_date_str:
            # Date changed, re-fetch images
            print("Date has changed. Fetching new images for the new day...")
            images_to_cycle, fallback_used = find_images_for_today_and_fallback()
            current_date_str = new_date_str
            index = 0

        if not images_to_cycle:
            # No images found at all. Wait and try again.
            print("No images found (even after fallback). Retrying in 5 minutes...")
            time.sleep(300)
            images_to_cycle, fallback_used = find_images_for_today_and_fallback()
            continue

        # Get current image info
        s3_key = images_to_cycle[index][0]  # image_proxy_name is at index 0
        image = fetch_image_from_s3(s3_key)
        if image:
            display_image(image)
        else:
            print("Failed to fetch image. Will try the next one.")

        # Move to next image
        index = (index + 1) % len(images_to_cycle)

        # Wait 5 minutes before showing the next image
        print("Waiting 5 minutes before the next image...")
        time.sleep(300)