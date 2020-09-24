# Define how to normalize some names like districts etc.


def district(name):
    return (
        name.replace(", Stadt", "")
        .replace(" der FernUniversität", "")
        .replace(", Klingenstadt", "")
    )
