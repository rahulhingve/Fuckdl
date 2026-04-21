# Fuckdl/utils/lang.py
"""
Language utilities for matching and comparing language codes.
"""

from typing import Union, List, Optional
from langcodes import Language


def is_close_match(
    target: Union[str, Language], 
    candidates: List[Union[str, Language]], 
    max_distance: int = 25
) -> bool:
    """
    Check if target language closely matches any candidate.
    
    Args:
        target: Target language to match
        candidates: List of candidate languages
        max_distance: Maximum distance for a close match (default: 25)
        
    Returns:
        True if a close match is found, False otherwise
    """
    from langcodes import closest_supported_match
    
    target_str = str(target)
    
    for candidate in candidates:
        candidate_str = str(candidate)
        match = closest_supported_match(target_str, [candidate_str], max_distance)
        if match:
            return True
    
    return False


def get_closest_match(
    target: Union[str, Language],
    candidates: List[Union[str, Language]],
    max_distance: int = 25
) -> Optional[str]:
    """
    Get the closest matching language from candidates.
    
    Args:
        target: Target language to match
        candidates: List of candidate languages
        max_distance: Maximum distance for a match (default: 25)
        
    Returns:
        The closest matching language code, or None if no match found
    """
    from langcodes import closest_supported_match
    
    target_str = str(target)
    candidate_strs = [str(c) for c in candidates]
    
    return closest_supported_match(target_str, candidate_strs, max_distance)


def normalize_language(lang: Union[str, Language]) -> str:
    """
    Normalize language code to a standard format.
    
    Args:
        lang: Language code or Language object
        
    Returns:
        Normalized language code string
    """
    if isinstance(lang, Language):
        return str(lang)
    return str(Language.get(lang))


def get_language_name(lang: Union[str, Language]) -> str:
    """
    Get human-readable language name.
    
    Args:
        lang: Language code or Language object
        
    Returns:
        Human-readable language name
    """
    if isinstance(lang, Language):
        return lang.display_name()
    return Language.get(lang).display_name()


def is_original_language(
    track_lang: Union[str, Language],
    title_lang: Union[str, Language]
) -> bool:
    """
    Check if track language matches title's original language.
    
    Args:
        track_lang: Track language
        title_lang: Title's original language
        
    Returns:
        True if languages match (ignoring script/territory)
    """
    track = Language.get(track_lang)
    title = Language.get(title_lang)
    
    # Compare base language only (ignore script and territory)
    return track.language == title.language


__all__ = (
    "is_close_match",
    "get_closest_match",
    "normalize_language",
    "get_language_name",
    "is_original_language"
)