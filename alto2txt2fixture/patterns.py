"""
Useful regular expressions, intially just ``PUBLICATION_CODE``.
"""

import re

PUBLICATION_CODE = re.compile(r"\d{7}")
