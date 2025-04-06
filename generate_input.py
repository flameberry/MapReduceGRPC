import random

# Common words to simulate real-world data
common_words = [
    "water",
    "climate",
    "change",
    "environment",
    "pollution",
    "resource",
    "management",
    "sustainability",
    "global",
    "warming",
    "technology",
    "smart",
    "city",
    "green",
    "future",
    "energy",
    "earth",
    "natural",
    "disaster",
    "flood",
    "rainfall",
    "data",
    "analytics",
    "community",
    "action",
]

output_file = "input_large.txt"
num_words = 10000

with open(output_file, "w") as f:
    for _ in range(num_words):
        word = random.choice(common_words)
        f.write(word + " ")

print(f"Generated {output_file} with {num_words} words.")
