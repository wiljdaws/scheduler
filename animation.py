from builtins import print
import os
import time

# Define the frames
frames = [
    "╔════╤╤╤╤════╗\n" +
    "║    │││ \\   ║\n" +
    "║    │││  O  ║\n" +
    "║    OOO     ║",

    "╔════╤╤╤╤════╗\n" +
    "║    ││││    ║\n" +
    "║    ││││    ║\n" +
    "║    OOOO    ║",

    "╔════╤╤╤╤════╗\n" +
    "║   / │││    ║\n" +
    "║  O  │││    ║\n" +
    "║     OOO    ║",

    "╔════╤╤╤╤════╗\n" +
    "║    ││││    ║\n" +
    "║    ││││    ║\n" +
    "║    OOOO    ║"
]

# Function to clear the console
def clear_console():
    os.system('cls' if os.name == 'nt' else 'clear')

# Function to display the frames as animation
def display_animation(frames = frames, repeat=True, frame_delay=0.17):
    while True:
        for frame in frames:
            clear_console()
            print(frame)
            time.sleep(frame_delay)
        if not repeat:
            break


