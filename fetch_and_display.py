import os
import time
import psycopg2
from dotenv import load_dotenv
from PIL import Image, ImageOps
from inky.auto import auto
import boto3
from io import BytesIO
import random

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

def fetch_random_image_uuid():
    """Fetch a random UUID of an image from the database."""
    conn = get_db_connection()
    if not conn:
        return None

    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT uuid FROM assets 
            WHERE image_proxy_name IS NOT NULL 
            ORDER BY RANDOM() LIMIT 1;
        """)
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching random image UUID: {e}")
        return None
    finally:
        conn.close()

def fetch_image_s3_key(uuid):
    """Fetch the S3 object key of an image from the database by UUID."""
    conn = get_db_connection()
    if not conn:
        return None

    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT image_proxy_name FROM assets 
            WHERE uuid = %s AND image_proxy_name IS NOT NULL 
            LIMIT 1;
        """, (uuid,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching image key: {e}")
        return None
    finally:
        conn.close()

def download_image_from_s3(s3_key):
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

    while True:
        # Step 1: Fetch a random image UUID from the database
        uuid = fetch_random_image_uuid()
        if not uuid:
            print("No image UUID found in the database. Retrying in 5 minutes.")
            time.sleep(300)  # Wait 5 minutes before retrying
            continue

        print(f"Random UUID fetched: {uuid}")

        # Step 2: Fetch the S3 object key from the database by UUID
        s3_key = fetch_image_s3_key(uuid)
        if not s3_key:
            print(f"No image key found in the database for UUID: {uuid}")
            time.sleep(300)  # Wait 5 minutes before retrying
            continue

        print(f"Image key fetched: {s3_key}")

        # Step 3: Download the image from S3
        image = download_image_from_s3(s3_key)
        if not image:
            print("Failed to download the image. Retrying in 5 minutes.")
            time.sleep(300)  # Wait 5 minutes before retrying
            continue

        # Step 4: Display the image on the e-paper display
        display_image(image)

        # Wait 5 minutes before showing the next image
        print("Waiting 5 minutes before the next image...")
        time.sleep(300)
