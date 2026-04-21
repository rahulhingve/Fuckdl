LANGUAGE_MUX_MAP = {
    # List of language tags that cannot be used by mkvmerge and need replacements.
    "none": "und",
    "nb": "nor",
}

TERRITORY_MAP = {
    "001": "",
    "150": "European",
    "419": "Latin American",
    "AU": "Australian",
    "BE": "Flemish",
    "BR": "Brazilian",
    "CA": "Canadian",
    "CZ": "Czech", 
    "DK": "Danish", 
    "EG": "Egyptian",
    "ES": "European",
    "FR": "European",
    "GB": "British",
    "GR": "Greek", 
    "HK": "Hong Kong",
    "IL": "Israeli", 
    "IN": "Indian", 
    "JP": "Japanese", 
    "KR": "Korean", 
    "MY": "Malaysian", 
    "NO": "Norwegian", 
    "PH": "Filipino", 
    "PS": "Palestinian",
    "PT": "European",
    "SE": "Swedish", 
    "SY": "Syrian",
    "US": "American",
}

# The max distance of languages to be considered "same", e.g. en, en-US, en-AU
LANGUAGE_MAX_DISTANCE = 5