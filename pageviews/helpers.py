# ==========================
# HBBT Wikipedia API Helpers
# ==========================
#
# Miscellaneous helper functions.
#

# Function consuming n items from the given generator
def consume(generator, n):
    acc = []

    for i in range(n):
        try:
            acc.append(next(generator))
        except:
            return acc

    return acc
