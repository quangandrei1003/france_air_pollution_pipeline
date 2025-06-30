import re
import unicodedata


def slugify(text: str) -> str:
    """
    Convert string to slug format (lowercase, no spaces, no special chars)
    Example: "Saint-Étienne" -> "saint_etienne"
    """
    # Convert to lowercase and normalize unicode characters
    text = str(text).lower().strip()
    text = unicodedata.normalize('NFKD', text).encode(
        'ASCII', 'ignore').decode()

    # Replace any non-word character with underscore
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)

    return text
