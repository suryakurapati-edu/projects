import os
from PIL import Image
from app.config import XRAWS_ROOT


class NotFoundError(Exception):
    """Raised when the requested X-ray image is missing."""


def load_image_by_id(xray_id: str):
    """Load X-ray image from images directory."""
    image_path = os.path.join(XRAWS_ROOT, f"{xray_id}.jpeg")
    if not os.path.exists(image_path):
        raise NotFoundError(f"X-ray image '{xray_id}' not found at {image_path}")
    return Image.open(image_path)
