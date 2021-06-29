import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay

DATA = [
    *["apple"] * 5,
    *["banana"] * 12,
    *["strawberry"] * 8,
    *["pineapple"] * 2,
]


class Cmap:
    Accent = "Accent"
    Accent_r = "Accent_r"
    Blues = "Blues"
    Blues_r = "Blues_r"
    BrBG = "BrBG"
    BrBG_r = "BrBG_r"
    BuGn = "BuGn"
    BuGn_r = "BuGn_r"
    BuPu = "BuPu"
    BuPu_r = "BuPu_r"
    CMRmap = "CMRmap"
    CMRmap_r = "CMRmap_r"
    Dark2 = "Dark2"
    Dark2_r = "Dark2_r"
    GnBu = "GnBu"
    GnBu_r = "GnBu_r"
    Greens = "Greens"
    Greens_r = "Greens_r"
    Greys = "Greys"
    Greys_r = "Greys_r"
    OrRd = "OrRd"
    OrRd_r = "OrRd_r"
    Oranges = "Oranges"
    Oranges_r = "Oranges_r"
    PRGn = "PRGn"
    PRGn_r = "PRGn_r"
    Paired = "Paired"
    Paired_r = "Paired_r"
    Pastel1 = "Pastel1"
    Pastel1_r = "Pastel1_r"
    Pastel2 = "Pastel2"
    Pastel2_r = "Pastel2_r"
    PiYG = "PiYG"
    PiYG_r = "PiYG_r"
    PuBu = "PuBu"
    PuBuGn = "PuBuGn"
    PuBuGn_r = "PuBuGn_r"
    PuBu_r = "PuBu_r"
    PuOr = "PuOr"
    PuOr_r = "PuOr_r"
    PuRd = "PuRd"
    PuRd_r = "PuRd_r"
    Purples = "Purples"
    Purples_r = "Purples_r"
    RdBu = "RdBu"
    RdBu_r = "RdBu_r"
    RdGy = "RdGy"
    RdGy_r = "RdGy_r"
    RdPu = "RdPu"
    RdPu_r = "RdPu_r"
    RdYlBu = "RdYlBu"
    RdYlBu_r = "RdYlBu_r"
    RdYlGn = "RdYlGn"
    RdYlGn_r = "RdYlGn_r"
    Reds = "Reds"
    Reds_r = "Reds_r"
    Set1 = "Set1"
    Set1_r = "Set1_r"
    Set2 = "Set2"
    Set2_r = "Set2_r"
    Set3 = "Set3"
    Set3_r = "Set3_r"
    Spectral = "Spectral"
    Spectral_r = "Spectral_r"
    Wistia = "Wistia"
    Wistia_r = "Wistia_r"
    YlGn = "YlGn"
    YlGnBu = "YlGnBu"
    YlGnBu_r = "YlGnBu_r"
    YlGn_r = "YlGn_r"
    YlOrBr = "YlOrBr"
    YlOrBr_r = "YlOrBr_r"
    YlOrRd = "YlOrRd"
    YlOrRd_r = "YlOrRd_r"
    afmhot = "afmhot"
    afmhot_r = "afmhot_r"
    autumn = "autumn"
    autumn_r = "autumn_r"
    binary = "binary"
    binary_r = "binary_r"
    bone = "bone"
    bone_r = "bone_r"
    brg = "brg"
    brg_r = "brg_r"
    bwr = "bwr"
    bwr_r = "bwr_r"
    cividis = "cividis"
    cividis_r = "cividis_r"
    cool = "cool"
    cool_r = "cool_r"
    coolwarm = "coolwarm"
    coolwarm_r = "coolwarm_r"
    copper = "copper"
    copper_r = "copper_r"
    cubehelix = "cubehelix"
    cubehelix_r = "cubehelix_r"
    flag = "flag"
    flag_r = "flag_r"
    gist_earth = "gist_earth"
    gist_earth_r = "gist_earth_r"
    gist_gray = "gist_gray"
    gist_gray_r = "gist_gray_r"
    gist_heat = "gist_heat"
    gist_heat_r = "gist_heat_r"
    gist_ncar = "gist_ncar"
    gist_ncar_r = "gist_ncar_r"
    gist_rainbow = "gist_rainbow"
    gist_rainbow_r = "gist_rainbow_r"
    gist_stern = "gist_stern"
    gist_stern_r = "gist_stern_r"
    gist_yarg = "gist_yarg"
    gist_yarg_r = "gist_yarg_r"
    gnuplot = "gnuplot"
    gnuplot2 = "gnuplot2"
    gnuplot2_r = "gnuplot2_r"
    gnuplot_r = "gnuplot_r"
    gray = "gray"
    gray_r = "gray_r"
    hot = "hot"
    hot_r = "hot_r"
    hsv = "hsv"
    hsv_r = "hsv_r"
    inferno = "inferno"
    inferno_r = "inferno_r"
    jet = "jet"
    jet_r = "jet_r"
    magma = "magma"
    magma_r = "magma_r"
    nipy_spectral = "nipy_spectral"
    nipy_spectral_r = "nipy_spectral_r"
    ocean = "ocean"
    ocean_r = "ocean_r"
    pink = "pink"
    pink_r = "pink_r"
    plasma = "plasma"
    plasma_r = "plasma_r"
    prism = "prism"
    prism_r = "prism_r"
    rainbow = "rainbow"
    rainbow_r = "rainbow_r"
    seismic = "seismic"
    seismic_r = "seismic_r"
    spring = "spring"
    spring_r = "spring_r"
    summer = "summer"
    summer_r = "summer_r"
    tab10 = "tab10"
    tab10_r = "tab10_r"
    tab20 = "tab20"
    tab20_r = "tab20_r"
    tab20b = "tab20b"
    tab20b_r = "tab20b_r"
    tab20c = "tab20c"
    tab20c_r = "tab20c_r"
    terrain = "terrain"
    terrain_r = "terrain_r"
    turbo = "turbo"
    turbo_r = "turbo_r"
    twilight = "twilight"
    twilight_r = "twilight_r"
    twilight_shifted = "twilight_shifted"
    twilight_shifted_r = "twilight_shifted_r"
    viridis = "viridis"
    viridis_r = "viridis_r"
    winter = "winter"
    winter_r = "winter_r"


def plot_freq_bar(items, ylabel="frequency", title=""):
    item_set, item_counts = np.unique(items, return_counts=True)

    plt.bar(item_set, item_counts)
    plt.xticks(rotation=35)
    plt.ylabel(ylabel)
    plt.title(title)

    for i, cnt in enumerate(item_counts):
        plt.text(x=i, y=cnt / 2, s=cnt, ha="center", color="white")

    plt.tight_layout()


def plot_confusion_matrix(y_true=None, y_pred=None, cm=None, labels=None, title=None, cmap=None):
    if not cm:
        cm = confusion_matrix(y_true, y_pred, labels=labels)

    if type(cm) == list:
        cm = np.array(cm)

    cm_display = ConfusionMatrixDisplay(cm, display_labels=labels)
    cm_display.plot(cmap=cmap)

    if title:
        plt.title(title)

    if labels:
        plt.xticks(rotation=30)

    plt.tight_layout()


if __name__ == '__main__':
    plot_freq_bar(DATA, title="My title")
    plt.show()

    plot_confusion_matrix(
        cm=[[12, 1, 0],
            [3, 14, 1],
            [5, 6, 7]],
        title="My title",
        labels=["apple", "orange", "grape"],
        cmap=Cmap.viridis
    )
    plt.show()
