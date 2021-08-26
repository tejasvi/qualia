from re import split


def normalized_prefixes(string: str) -> set[str]:
    return {word[:3].casefold() for word in split(r'(\W)', string) if word and not word.isspace()}
