"""
Useful code snippets for making commonly used plots in Raphtory.
"""

from pathlib import Path
import numpy as np


motif_im_dir = Path(__file__).parents[1].absolute().as_posix() + "/motif-images/"


def global_motif_heatplot(motifs, cmap="YlGnBu", **kwargs):
    """
    Out-of-the-box plotting of global motif counts corresponding to the layout in Motifs in Temporal Networks (Paranjape et al)

    Args:
        motifs(list | np.ndarray): 1 dimensional length-40 array of motifs, which should be the list of motifs returned from the `global_temporal_three_node_motifs` function in Raphtory.
        **kwargs: arguments to

    Returns:
        matplotlib.axes.Axes: ax item containing the heatmap with motif labels on the axes.
    """
    # import is here as it's a pretty niche function and not worth having a seaborn dependency for the whole project
    import seaborn as sns
    import matplotlib.pyplot as plt
    from matplotlib.offsetbox import OffsetImage, AnnotationBbox

    def _get_motif(xory: str, y: int):
        path = motif_im_dir + xory + str(y) + ".png"
        return plt.imread(path)

    def _get_motif_labels(motif_map):
        return np.vectorize(human_format)(motif_map)

    def _offset_image(xory, coord, name, ax):
        img = _get_motif(xory, name)
        im = OffsetImage(img, zoom=0.04)
        im.image.axes = ax

        if xory == "x":
            ab = AnnotationBbox(
                im,
                (coord + 0.5, 5.5),
                xybox=(0.0, -40.0),
                frameon=False,
                xycoords="data",
                boxcoords="offset points",
                box_alignment=(0.5, 0.5),
                pad=0,
            )

        else:
            ab = AnnotationBbox(
                im,
                (0, coord),
                xybox=(0.0, -40.0),
                frameon=False,
                xycoords="data",
                boxcoords="offset points",
                box_alignment=(1.0, 0.0),
                pad=0,
            )

        ax.add_artist(ab)

    def _add_motifs_to_ax(ax):
        for i in range(6):
            _offset_image("x", i, i, ax)
            _offset_image("y", i, i, ax)

    motif_matrix = to_motif_matrix(motifs)
    labels = _get_motif_labels(motif_matrix)

    ax = sns.heatmap(
        motif_matrix,
        square=True,
        cbar=True,
        cmap=cmap,
        annot=labels,
        annot_kws={"size": 13},
        fmt="",
        cbar_kws={"shrink": 1.0},
        **kwargs,
    )
    _add_motifs_to_ax(ax)
    ax.tick_params(axis="x", which="major", pad=50)
    ax.tick_params(axis="y", which="major", pad=50)
    plt.setp(ax.get_xticklabels(), visible=False)
    plt.setp(ax.get_yticklabels(), visible=False)
    plt.tight_layout()
    return ax


def to_motif_matrix(motifs, data_type=int):
    """
    Converts a 40d vector of global motifs to a 2d grid of motifs corresponding to the layout in Motifs in Temporal Networks (Paranjape et al)

    Args:
        motifs(list | np.ndarray): 1 dimensional length-40 array of motifs.

    Returns:
        np.ndarray: 6x6 array of motifs whose ijth element is M_ij in Motifs in Temporal Networks (Paranjape et al).
    """
    mapper = {
        0: (5, 5),
        1: (5, 4),
        2: (4, 5),
        3: (4, 4),
        4: (4, 3),
        5: (4, 2),
        6: (5, 3),
        7: (5, 2),
        8: (0, 0),
        9: (0, 1),
        10: (1, 0),
        11: (1, 1),
        12: (2, 1),
        13: (2, 0),
        14: (3, 1),
        15: (3, 0),
        16: (0, 5),
        17: (0, 4),
        18: (1, 5),
        19: (1, 4),
        20: (2, 3),
        21: (2, 2),
        22: (3, 3),
        23: (3, 2),
        24: (5, 0),
        25: (5, 1),
        26: (4, 0),
        27: (4, 1),
        28: (4, 1),
        29: (4, 0),
        30: (5, 1),
        31: (5, 0),
        32: (0, 2),
        33: (2, 4),
        34: (1, 2),
        35: (3, 4),
        36: (0, 3),
        37: (2, 5),
        38: (1, 3),
        39: (3, 5),
    }

    motif_2d = np.zeros((6, 6), dtype=data_type)
    for i in range(40):
        motif_2d[mapper[i]] = motifs[i]
    return motif_2d


def human_format(num):
    """
    Converts a number over 1000 to a string with 1 d.p and the corresponding letter. e.g. with input 24134, 24.1k as a string would be returned. This is used in the motif plots to make annotated heatmap cells more concise.

    Args:
        num(int): number to be abbreviated

    Returns:
        str: number in abbreviated string format.
    """
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return "%.1f%s" % (num, ["", "K", "M", "B", "T", "P"][magnitude])


# Relating to distributions


def cdf(observations, normalised=True):
    """
    Returns x coordinates and y coordinates for a cdf (cumulative density function) from a list of observations.

    Args:
        observations(list): list of observations, should be numeric
        normalised(bool): if true, y coordinates normalised such that y is the probability of finding a value less than or equal to x, if false y is the number of observations less than or equal to x. Defaults to True.

    Returns:
        Tuple[np.ndarray, np.ndarray]: the x and y coordinates for the cdf
    """
    data = np.array(observations, dtype=object)
    N = len(observations)

    x = np.sort(data)
    if normalised:
        y = np.arange(N) / float(N - 1)
    else:
        y = np.arange(N)
    return x, y


def ccdf(observations, normalised=True):
    """
    Returns x coordinates and y coordinates for a ccdf (complementary cumulative density function) from a list of observations.

    Args:
        observations(list): list of observations, should be numeric
        normalised(bool,optional): Defaults to True. If true, y coordinates normalised such that y is the probability of finding a value greater than than or equal to x, if false y is the number of observations greater than or equal to x.

    Returns:
        Tuple[np.ndarray, np.ndarray]: x and y coordinates for the cdf
    """
    x, y = cdf(observations, normalised)
    if normalised:
        return x, 1.0 - y
    else:
        return x, len(observations) - y


def lorenz(observations):
    """
    Returns x coordinates and y coordinates for a Lorenz Curve from a list of observations.

    Args:
        observations(list): list of observations, should be numeric

    Returns:
        Tuple[np.ndarray, np.ndarray]: x and y coordinates for the cdf
    """
    tmp_arr = np.array(sorted(observations))
    # print(tmp_arr[0])
    x = np.arange(len(observations)) / (len(observations) - 1)
    y = tmp_arr.cumsum() / tmp_arr.sum()
    return x, y


def ordinal_number(number):
    """
    Returns ordinal number of integer input.

    Args:
        number(int): input number

    Returns:
        str: ordinal for that number as string
    """
    if 10 <= number % 100 <= 20:
        suffix = "th"
    else:
        suffixes = {1: "st", 2: "nd", 3: "rd"}
        suffix = suffixes.get(number % 10, "th")

    return str(number) + suffix
