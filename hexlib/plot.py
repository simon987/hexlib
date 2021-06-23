import numpy as np
import matplotlib.pyplot as plt

DATA = [
    *["apple"] * 5,
    *["banana"] * 12,
    *["strawberry"] * 8,
    *["pineapple"] * 2,
]


def plot_freq_bar(items, ylabel="frequency", title=""):
    item_set, item_counts = np.unique(items, return_counts=True)

    plt.bar(item_set, item_counts)
    plt.xticks(rotation=35)
    plt.ylabel(ylabel)
    plt.title(title)

    for i, cnt in enumerate(item_counts):
        plt.text(x=i, y=cnt / 2, s=cnt, ha="center", color="white")

    plt.tight_layout()


if __name__ == '__main__':
    plot_freq_bar(DATA, title="My title")
    plt.show()


